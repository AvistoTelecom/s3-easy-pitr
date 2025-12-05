package test

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal"
	s3utils "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
)

// UploadOptions holds configuration for the upload operation
type UploadOptions struct {
	Client      *s3.Client
	Bucket      string
	FileSizes   []int64
	MaxParallel int
	Logger      *zap.SugaredLogger
}

// GenerateFileSizes creates a slice of file sizes based on the total size requirement
func GenerateFileSizes(numFiles int, totalSize int64) []int64 {
	sizes := make([]int64, numFiles)

	if totalSize <= 0 {
		// Random sizes between 1KB and 1MB
		for i := range numFiles {
			sizes[i] = int64(mrand.Intn(1047552) + 1024) // 1KB to 1MB
		}
		return sizes
	}

	// Calculate sizes to meet total_size requirement
	var accumulated int64

	for i := range numFiles {
		if i == numFiles-1 {
			// Last file gets the remainder
			sizes[i] = totalSize - accumulated
		} else {
			remainingFiles := int64(numFiles - i)
			remainingSize := totalSize - accumulated
			avgSize := remainingSize / remainingFiles

			// Adapt variation based on average size
			variation := avgSize / 2
			if variation < 1 {
				variation = 1
			}

			randomAdjust := mrand.Int63n(variation*2+1) - variation
			size := avgSize + randomAdjust

			// Ensure minimum size of 1KB
			if size < 1024 {
				size = 1024
			}

			// Ensure we leave space for remaining files
			maxSize := remainingSize - (remainingFiles-1)*1024
			if size > maxSize {
				size = maxSize
			}

			sizes[i] = size
			accumulated += size
		}
	}

	return sizes
}

// UploadFilesWithProgress uploads files in parallel with a progress bar
func UploadFilesWithProgress(ctx context.Context, opts UploadOptions) error {
	total := int64(len(opts.FileSizes))

	// Create progress bar
	p := mpb.New(
		mpb.WithWidth(60),
		mpb.WithOutput(os.Stdout),
		mpb.WithAutoRefresh(),
	)

	bar := p.AddBar(total,
		mpb.PrependDecorators(
			decor.Name("Uploading: "),
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

	var (
		wg            sync.WaitGroup
		uploadedCount atomic.Int64
		failedCount   atomic.Int64
		uploadedBytes atomic.Int64
	)

	semaphore := make(chan struct{}, opts.MaxParallel)
	errChan := make(chan error, len(opts.FileSizes))

	for i, size := range opts.FileSizes {
		wg.Add(1)
		semaphore <- struct{}{} // Acquire

		go func(index int, fileSize int64) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release

			filename := fmt.Sprintf("file_%04d.bin", index+1)

			// Create a random data reader
			reader := io.LimitReader(rand.Reader, fileSize)

			_, err := opts.Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:        aws.String(opts.Bucket),
				Key:           aws.String(filename),
				Body:          reader,
				ContentLength: aws.Int64(fileSize),
			})

			if err != nil {
				sugar.Errorw("Failed to upload file", "filename", filename, "error", err)
				failedCount.Add(1)
				errChan <- fmt.Errorf("upload %s: %w", filename, err)
			} else {
				uploadedCount.Add(1)
				uploadedBytes.Add(fileSize)
				sugar.Debugw("✓ Uploaded", "filename", filename, "size", fileSize)
			}

			// Update progress bar
			bar.Increment()
		}(i, size)
	}

	wg.Wait()
	close(errChan)

	// Wait for progress bar to finish rendering FIRST
	p.Wait()

	uploaded := uploadedCount.Load()
	failed := failedCount.Load()
	bytes := uploadedBytes.Load()

	// Now log the summary with a regular logger (not mpb-integrated) after progress bar is done
	opts.Logger.Infow("Upload complete",
		"uploaded", uploaded,
		"failed", failed,
		"total", len(opts.FileSizes),
		"bytes", s3utils.FormatBytes(bytes),
	)

	return nil
}
