package pitr

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal"
	internalS3 "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
	awsS3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awsS3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Options for the PITR run
type Options struct {
	Client    *internalS3.Client
	Bucket    string
	Prefix    string
	Target    time.Time // Target time for point-in-time recovery
	Parallel  int
	Logger    *zap.SugaredLogger
	PrintFreq time.Duration
	// CopyRetries controls how many times a failing CopyObject will be retried
	// when restoring a version. Set to 0 to disable retries. Default is 3.
	CopyRetries int
	// CopyTimeout is the per-CopyObject request timeout. Use a reasonably
	// long timeout for S3-compatible endpoints (e.g. 2m).
	CopyTimeout time.Duration
	// CopyPartSize is the part size to use for multipart copies (in bytes)
	CopyPartSize int64
	// MultipartThreshold defines the size (in bytes) above which multipart
	// copy will be used instead of a single CopyObject
	MultipartThreshold int64
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

// Run will perform the point-in-time recovery
func Run(ctx context.Context, opts Options) error {
	startTime := time.Now()

	// Minimal checks
	if opts.Client == nil {
		return errors.New("s3 client is nil")
	}

	// Collect and log bucket statistics - this also discovers all keys
	opts.Logger.Info("Analyzing bucket (may take a while for large buckets)")
	stats, err := internalS3.GetBucketStats(ctx, opts.Client.S3, opts.Bucket, opts.Prefix, &opts.Target)
	if err != nil {
		return fmt.Errorf("get bucket statistics: %w", err)
	}

	if stats.UniqueKeys == 0 {
		opts.Logger.Infow("no objects to process")
		return nil
	}

	opts.Logger.Infow("Bucket statistics",
		"individual_files", stats.UniqueKeys,
		"total_versions_and_markers", stats.TotalVersionsAndMarkers,
		"recoverable_files", stats.RecoverableFiles,
	)

	// Only process recoverable files
	if len(stats.RecoverableKeys) == 0 {
		opts.Logger.Infow("no recoverable files found at target time")
		return nil
	}

	total := int64(len(stats.RecoverableKeys))

	// Create progress tracker
	prog := newProgress(total)

	// Create a new logger that writes through mpb to avoid cropping
	// This ensures logs appear above the progress bar
	// Use the global logger's level
	logger, err := internal.CreateLoggerWithOutput(prog.Output(), zap.L().Level())
	if err != nil {
		return fmt.Errorf("create progress-aware logger: %w", err)
	}
	progressLogger := logger.Sugar()

	// Track success/failure counts
	var recoveredCount, failedCount int64

	// Use larger buffer for better throughput
	jobs := make(chan string, opts.Parallel*10)
	var wg sync.WaitGroup

	// start workers
	for i := 0; i < opts.Parallel; i++ {
		wg.Go(func() {
			for key := range jobs {
				if err := processKey(ctx, &opts, key); err != nil {
					// Ensure error prints on its own line
					progressLogger.Errorw("process object failed", "object", key, "err", err)
					atomic.AddInt64(&failedCount, 1)
				} else {
					progressLogger.Debugw("processed", "object", key)
					atomic.AddInt64(&recoveredCount, 1)
				}
				// Mutex-protected progress update
				prog.increment()
			}
		})
	}

	// feed jobs - only recoverable keys
	go func() {
		for _, k := range stats.RecoverableKeys {
			jobs <- k
		}
		close(jobs)
	}()

	wg.Wait()
	prog.Wait()

	// Calculate duration and log final statistics
	duration := time.Since(startTime)
	recovered := atomic.LoadInt64(&recoveredCount)
	failed := atomic.LoadInt64(&failedCount)

	opts.Logger.Infow("pitr completed",
		"recovered", recovered,
		"failed", failed,
		"total", len(stats.RecoverableKeys),
		"duration", duration.String(),
		"completed_at", time.Now().Format(time.RFC3339),
	)

	return nil
}

// listAllKeys lists all object keys using ListObjectsV2
func listAllKeys(ctx context.Context, client *awsS3.Client, bucket, prefix string, workers int) ([]string, error) {
	// Discover top-level common prefixes to split the listing work
	delimiter := "/"
	in := &awsS3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix, Delimiter: &delimiter}
	out, err := client.ListObjectsV2(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("list common prefixes: %w", err)
	}

	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		if cp.Prefix != nil {
			prefixes = append(prefixes, *cp.Prefix)
		}
	}
	// If no common prefixes, just use the provided prefix
	if len(prefixes) == 0 {
		prefixes = append(prefixes, prefix)
	}

	if workers <= 0 {
		workers = 1
	}

	prefixCh := make(chan string, len(prefixes))
	keysCh := make(chan string, 1024)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// start workers
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			for p := range prefixCh {
				paginator := awsS3.NewListObjectsV2Paginator(client, &awsS3.ListObjectsV2Input{Bucket: &bucket, Prefix: &p})
				for paginator.HasMorePages() {
					select {
					case <-ctx.Done():
						return
					default:
					}
					page, err := paginator.NextPage(ctx)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("list objects for prefix %s: %w", p, err):
						default:
						}
						return
					}
					for _, obj := range page.Contents {
						if obj.Key != nil {
							select {
							case keysCh <- *obj.Key:
							case <-ctx.Done():
								return
							}
						}
					}
				}
			}
		})
	}

	// feed prefixes
	go func() {
		for _, p := range prefixes {
			prefixCh <- p
		}
		close(prefixCh)
	}()

	// collector
	keys := make([]string, 0, 10000) // Pre-allocate for better performance
	done := make(chan struct{})
	go func() {
		for k := range keysCh {
			keys = append(keys, k)
		}
		close(done)
	}()

	// wait for workers
	go func() {
		wg.Wait()
		close(keysCh)
	}()

	select {
	case e := <-errCh:
		return nil, e
	case <-done:
		return keys, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// processKey finds the version as of target time and restores it, with
// retries and per-request timeout to reduce transient failures.
func processKey(ctx context.Context, opts *Options, key string) error {
	client := opts.Client.S3
	bucket := opts.Bucket
	targetTime := opts.Target

	// List versions to find the one active at target time
	// Use Prefix=key to get only versions of this specific key
	paginator := awsS3.NewListObjectVersionsPaginator(client, &awsS3.ListObjectVersionsInput{
		Bucket: &bucket,
		Prefix: &key,
	})

	var targetVersion *string
	var targetSize int64
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list versions: %w", err)
		}

		for _, version := range page.Versions {
			if version.LastModified == nil || version.Key == nil || *version.Key != key {
				continue
			}
			// Find latest version that existed at or before target time
			if version.LastModified.Before(targetTime) || version.LastModified.Equal(targetTime) {
				if targetVersion == nil {
					targetVersion = version.VersionId
					if version.Size != nil {
						targetSize = *version.Size
					}
				} else if version.LastModified.After(*page.Versions[0].LastModified) {
					targetVersion = version.VersionId
					if version.Size != nil {
						targetSize = *version.Size
					}
				}
			}
		}
	}

	if targetVersion == nil {
		opts.Logger.Debugw("no version found at or before target time", "object", key)
		return nil
	}

	// Prepare retry parameters
	maxAttempts := opts.CopyRetries
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	perReqTimeout := opts.CopyTimeout
	if perReqTimeout <= 0 {
		perReqTimeout = 2 * time.Minute
	}

	copySource := bucket + "/" + key + "?versionId=" + *targetVersion

	// Choose multipart or single copy based on size from version listing
	if targetSize > opts.MultipartThreshold && opts.CopyPartSize > 0 {
		return multipartCopyWithRetries(ctx, client, bucket, key, copySource, targetSize, maxAttempts, perReqTimeout, opts)
	}

	return copyWithRetries(ctx, client, bucket, key, copySource, maxAttempts, perReqTimeout, opts)
}

// copyWithRetries performs a single CopyObject with retries and timeout
func copyWithRetries(ctx context.Context, client *awsS3.Client, bucket, key, copySource string, maxAttempts int, perReqTimeout time.Duration, opts *Options) error {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, perReqTimeout)
		_, err := client.CopyObject(attemptCtx, &awsS3.CopyObjectInput{
			Bucket:     &bucket,
			Key:        &key,
			CopySource: &copySource,
		})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == maxAttempts {
			break
		}
		base := 500 * time.Millisecond
		backoff := base * time.Duration(1<<(attempt-1))
		jitter := time.Duration(rng.Int63n(int64(backoff/2) + 1))
		sleep := backoff + jitter
		opts.Logger.Debugw("copy attempt failed, will retry", "object", key, "attempt", attempt, "sleep", sleep, "err", err)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return fmt.Errorf("context canceled while backing off: %w", ctx.Err())
		}
	}
	return fmt.Errorf("single-copy failed after %d attempts: %w", maxAttempts, lastErr)
}

// multipartCopyWithRetries performs a multipart copy using UploadPartCopy for each part.
func multipartCopyWithRetries(ctx context.Context, client *awsS3.Client, bucket, key, copySource string, size int64, maxAttempts int, perReqTimeout time.Duration, opts *Options) error {
	partSize := opts.CopyPartSize
	if partSize < 5*1024*1024 {
		partSize = 5 * 1024 * 1024
	}
	numParts := int((size + partSize - 1) / partSize)

	// Initiate multipart upload
	createOut, err := client.CreateMultipartUpload(ctx, &awsS3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("initiate multipart upload: %w", err)
	}
	uploadID := createOut.UploadId
	completedParts := make([]awsS3types.CompletedPart, numParts)

	// Upload parts in parallel (use up to 5 concurrent part uploads)
	partWorkers := 5
	if numParts < partWorkers {
		partWorkers = numParts
	}

	type partJob struct {
		partNum int
		start   int64
		end     int64
	}

	partJobs := make(chan partJob, numParts)
	partErrors := make(chan error, numParts)
	var partWg sync.WaitGroup

	// Start part upload workers
	for w := 0; w < partWorkers; w++ {
		partWg.Add(1)
		go func() {
			defer partWg.Done()
			for job := range partJobs {
				rangeHeader := fmt.Sprintf("bytes=%d-%d", job.start, job.end)
				var partOut *awsS3.UploadPartCopyOutput
				var lastErr error

				for attempt := 1; attempt <= maxAttempts; attempt++ {
					attemptCtx, cancel := context.WithTimeout(ctx, perReqTimeout)
					pn := int32(job.partNum)
					partOut, err = client.UploadPartCopy(attemptCtx, &awsS3.UploadPartCopyInput{
						Bucket:          &bucket,
						Key:             &key,
						UploadId:        uploadID,
						PartNumber:      &pn,
						CopySource:      &copySource,
						CopySourceRange: &rangeHeader,
					})
					cancel()
					if err == nil {
						break
					}
					lastErr = err
					if attempt == maxAttempts {
						break
					}
					base := 500 * time.Millisecond
					backoff := base * time.Duration(1<<(attempt-1))
					jitter := time.Duration(rng.Int63n(int64(backoff/2) + 1))
					sleep := backoff + jitter
					opts.Logger.Debugw("upload-part-copy attempt failed, will retry", "key", key, "part", job.partNum, "attempt", attempt, "err", err, "sleep", sleep)
					select {
					case <-time.After(sleep):
					case <-ctx.Done():
						partErrors <- fmt.Errorf("context canceled while backing off: %w", ctx.Err())
						return
					}
				}

				if partOut == nil {
					partErrors <- fmt.Errorf("upload part %d failed: %w", job.partNum, lastErr)
					return
				}

				// Store completed part (parts are 1-indexed)
				pn := int32(job.partNum)
				completedParts[job.partNum-1] = awsS3types.CompletedPart{ETag: partOut.CopyPartResult.ETag, PartNumber: &pn}
			}
		}()
	}

	// Feed part jobs
	for partNum := 1; partNum <= numParts; partNum++ {
		start := int64(partSize) * int64(partNum-1)
		end := start + int64(partSize) - 1
		if end >= size {
			end = size - 1
		}
		partJobs <- partJob{partNum: partNum, start: start, end: end}
	}
	close(partJobs)

	// Wait for all parts to complete
	partWg.Wait()
	close(partErrors)

	// Check for any errors
	if len(partErrors) > 0 {
		err := <-partErrors
		client.AbortMultipartUpload(context.Background(), &awsS3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: uploadID})
		return err
	}

	// complete
	_, err = client.CompleteMultipartUpload(ctx, &awsS3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        uploadID,
		MultipartUpload: &awsS3types.CompletedMultipartUpload{Parts: completedParts},
	})
	if err != nil {
		client.AbortMultipartUpload(context.Background(), &awsS3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: uploadID})
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}
