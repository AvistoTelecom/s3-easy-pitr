package test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal"
	s3client "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
)

// DestroyOptions holds configuration for the destroy operation
type DestroyOptions struct {
	Client       *s3.Client
	Bucket       string
	DeleteBucket bool
	Logger       *zap.SugaredLogger
}

// DestroyBucketWithProgress deletes all objects, versions, and delete markers from a bucket with progress tracking
func DestroyBucketWithProgress(ctx context.Context, opts DestroyOptions) error {
	// First, count total items to delete
	opts.Logger.Info("Analyzing bucket versions (may take a while for large buckets)")
	stats, err := s3client.GetDestroyStats(ctx, opts.Client, opts.Bucket, "")
	if err != nil {
		return fmt.Errorf("failed to count bucket items: %w", err)
	}

	if stats.TotalVersionsAndMarkers == 0 {
		opts.Logger.Info("Bucket is already empty")
		// If delete bucket is requested, still delete the bucket
		if opts.DeleteBucket {
			return deleteBucket(ctx, opts.Client, opts.Bucket, opts.Logger)
		}
		return nil
	}

	// Log detailed information before deletion
	opts.Logger.Infow("Bucket statistics",
		"total_items", stats.TotalVersionsAndMarkers,
		"individual_files", stats.UniqueKeys,
		"delete_markers", stats.DeleteMarkers,
	)

	// Create progress bar
	p := mpb.New(
		mpb.WithWidth(60),
		mpb.WithOutput(os.Stdout),
		mpb.WithAutoRefresh(),
	)

	bar := p.AddBar(int64(stats.TotalVersionsAndMarkers),
		mpb.PrependDecorators(
			decor.Name("Deleting: "),
			decor.CountersNoUnit("%d/%d"),
		),
		mpb.AppendDecorators(
			decor.Percentage(),
		),
	)

	// Create a logger that writes through mpb to avoid cropping
	logger, err := internal.CreateLoggerWithOutput(p, zap.L().Level())
	if err != nil {
		return fmt.Errorf("create progress-aware logger: %w", err)
	}
	sugar := logger.Sugar()

	var deletedCount atomic.Int64

	// Progress callback
	progressCallback := func() {
		deletedCount.Add(1)
		bar.Increment()
	}

	// Delete all versions and delete markers in a single pass
	if err := deleteAllObjectsWithProgress(ctx, opts.Client, opts.Bucket, sugar, progressCallback); err != nil {
		p.Wait()
		return fmt.Errorf("failed to delete objects: %w", err)
	}

	// Wait for progress bar to finish rendering
	p.Wait()

	deleted := deletedCount.Load()
	opts.Logger.Infow("Bucket cleanup complete", "deleted", deleted, "total", stats.TotalVersionsAndMarkers)

	// Delete the bucket if requested
	if opts.DeleteBucket {
		return deleteBucket(ctx, opts.Client, opts.Bucket, opts.Logger)
	}

	return nil
}

// deleteAllObjectsWithProgress deletes all versions and delete markers in a single pass
func deleteAllObjectsWithProgress(ctx context.Context, client *s3.Client, bucket string, logger *zap.SugaredLogger, progressCallback func()) error {
	var continuationToken *string

	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: continuationToken,
			MaxKeys:   aws.Int32(1000),
		})
		if err != nil {
			return fmt.Errorf("list object versions: %w", err)
		}

		// Delete all versions
		for _, version := range output.Versions {
			logger.Debugw("Deleting version", "key", *version.Key, "version_id", *version.VersionId)
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       version.Key,
				VersionId: version.VersionId,
			})
			if err != nil {
				logger.Warnw("Failed to delete version", "key", *version.Key, "error", err)
			}
			progressCallback()
		}

		// Delete all delete markers
		for _, marker := range output.DeleteMarkers {
			logger.Debugw("Deleting delete marker", "key", *marker.Key, "version_id", *marker.VersionId)
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
			if err != nil {
				logger.Warnw("Failed to delete marker", "key", *marker.Key, "error", err)
			}
			progressCallback()
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextKeyMarker
	}

	return nil
}

// deleteBucket deletes the bucket itself
func deleteBucket(ctx context.Context, client *s3.Client, bucket string, logger *zap.SugaredLogger) error {
	logger.Infow("Deleting bucket", "bucket", bucket)
	_, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("failed to delete bucket: %w", err)
	}
	logger.Infow("Bucket successfully destroyed", "bucket", bucket)
	return nil
}
