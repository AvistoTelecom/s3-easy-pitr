package s3

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.uber.org/zap"
)

// DeleteAllVersions deletes all object versions from a bucket
func DeleteAllVersions(ctx context.Context, client *s3.Client, bucket string, logger *zap.SugaredLogger) error {
	var continuationToken *string
	deletedCount := 0

	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: continuationToken,
			MaxKeys:   aws.Int32(1000),
		})
		if err != nil {
			return fmt.Errorf("list object versions: %w", err)
		}

		// Delete versions
		for _, version := range output.Versions {
			logger.Debugw("Deleting version", "key", *version.Key, "version_id", *version.VersionId)
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       version.Key,
				VersionId: version.VersionId,
			})
			if err != nil {
				logger.Warnw("Failed to delete version", "key", *version.Key, "error", err)
			} else {
				deletedCount++
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextKeyMarker
	}

	logger.Infow("Deleted object versions", "count", deletedCount)
	return nil
}

// DeleteAllDeleteMarkers deletes all delete markers from a bucket
func DeleteAllDeleteMarkers(ctx context.Context, client *s3.Client, bucket string, logger *zap.SugaredLogger) error {
	var continuationToken *string
	deletedCount := 0

	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			KeyMarker: continuationToken,
			MaxKeys:   aws.Int32(1000),
		})
		if err != nil {
			return fmt.Errorf("list object versions: %w", err)
		}

		// Delete delete markers
		for _, marker := range output.DeleteMarkers {
			logger.Debugw("Deleting delete marker", "key", *marker.Key, "version_id", *marker.VersionId)
			_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
			if err != nil {
				logger.Warnw("Failed to delete marker", "key", *marker.Key, "error", err)
			} else {
				deletedCount++
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextKeyMarker
	}

	logger.Infow("Deleted delete markers", "count", deletedCount)
	return nil
}

// FormatBytes formats bytes into human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// ParseBytes parses a human-readable byte string (e.g., "1GB", "500MB") into bytes
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}

	// If it's just a number, return it
	if val, err := strconv.ParseInt(s, 10, 64); err == nil {
		return val, nil
	}

	sUpper := strings.ToUpper(s)
	var multiplier int64 = 1
	var numStr string = s

	suffixes := []struct {
		suffix string
		mult   int64
	}{
		{"TIB", 1024 * 1024 * 1024 * 1024},
		{"TB", 1024 * 1024 * 1024 * 1024},
		{"T", 1024 * 1024 * 1024 * 1024},
		{"GIB", 1024 * 1024 * 1024},
		{"GB", 1024 * 1024 * 1024},
		{"G", 1024 * 1024 * 1024},
		{"MIB", 1024 * 1024},
		{"MB", 1024 * 1024},
		{"M", 1024 * 1024},
		{"KIB", 1024},
		{"KB", 1024},
		{"K", 1024},
		{"B", 1},
	}

	for _, suff := range suffixes {
		if strings.HasSuffix(sUpper, suff.suffix) {
			multiplier = suff.mult
			numStr = strings.TrimSuffix(sUpper, suff.suffix)
			break
		}
	}

	val, err := strconv.ParseFloat(strings.TrimSpace(numStr), 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format: %s", s)
	}

	return int64(val * float64(multiplier)), nil
}
