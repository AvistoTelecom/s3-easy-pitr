package pitr

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

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
	// Minimal checks
	if opts.Client == nil {
		return errors.New("s3 client is nil")
	}

	// list objects first (this can be slow on large buckets)
	opts.Logger.Infow("listing objects (may take a while for large buckets)", "bucket", opts.Bucket, "prefix", opts.Prefix)
	keys, err := listAllKeys(ctx, opts.Client.S3, opts.Bucket, opts.Prefix, opts.Parallel)
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}
	if len(keys) == 0 {
		opts.Logger.Infow("no objects to process")
		return nil
	}

	total := int64(len(keys))

	// Create progress tracker
	prog := newProgress(total)

	// Create a new logger that writes through mpb to avoid cropping
	// This ensures logs appear above the progress bar
	// Use the global logger's level
	logger, err := createLoggerWithOutput(prog.Output(), zap.L().Level())
	if err != nil {
		return fmt.Errorf("create progress-aware logger: %w", err)
	}
	opts.Logger = logger.Sugar()

	jobs := make(chan string, opts.Parallel*2)
	var wg sync.WaitGroup

	// start workers
	for i := 0; i < opts.Parallel; i++ {
		wg.Go(func() {
			for key := range jobs {
				if err := processKey(ctx, &opts, key); err != nil {
					// Ensure error prints on its own line
					opts.Logger.Errorw("process key failed", "key", key, "err", err)
				} else {
					opts.Logger.Debugw("processed", "key", key)
				}
				// Mutex-protected progress update
				prog.increment()
			}
		})
	}

	// feed jobs
	go func() {
		for _, k := range keys {
			jobs <- k
		}
		close(jobs)
	}()

	wg.Wait()
	prog.Wait()
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
	var keys []string
	done := make(chan struct{})
	go func() {
		for k := range keysCh {
			keys = append(keys, k)
			// overwrite same line with current count
			fmt.Printf("\rListing: %d\033[K", len(keys))
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
		// finish the line
		fmt.Println()
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
	paginator := awsS3.NewListObjectVersionsPaginator(client, &awsS3.ListObjectVersionsInput{
		Bucket: &bucket,
		Prefix: &key,
	})

	var targetVersion *string
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
				} else if version.LastModified.After(*page.Versions[0].LastModified) {
					targetVersion = version.VersionId
				}
			}
		}
	}

	if targetVersion == nil {
		return fmt.Errorf("no version found at or before target time")
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

	// Determine object size using HeadObject (with version)
	headIn := &awsS3.HeadObjectInput{Bucket: &bucket, Key: &key, VersionId: targetVersion}
	headOut, err := client.HeadObject(ctx, headIn)
	if err != nil {
		// Fall back to a single copy if we can't determine size
		opts.Logger.Warnw("head object failed, falling back to single copy", "key", key, "err", err)
		return copyWithRetries(ctx, client, bucket, key, copySource, maxAttempts, perReqTimeout, opts)
	}
	size := int64(0)
	if headOut.ContentLength != nil {
		size = *headOut.ContentLength
	}

	// Choose multipart or single copy
	if size > opts.MultipartThreshold && opts.CopyPartSize > 0 {
		return multipartCopyWithRetries(ctx, client, bucket, key, copySource, size, maxAttempts, perReqTimeout, opts)
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
		opts.Logger.Debugw("copy attempt failed, will retry", "key", key, "attempt", attempt, "sleep", sleep, "err", err)
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
	completedParts := make([]awsS3types.CompletedPart, 0, numParts)

	// copy each part
	for partNum := 1; partNum <= numParts; partNum++ {
		start := int64(partSize) * int64(partNum-1)
		end := start + int64(partSize) - 1
		if end >= size {
			end = size - 1
		}
		rangeHeader := fmt.Sprintf("bytes=%d-%d", start, end)

		var partOut *awsS3.UploadPartCopyOutput
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			attemptCtx, cancel := context.WithTimeout(ctx, perReqTimeout)
			pn := int32(partNum)
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
			opts.Logger.Infow("upload-part-copy attempt failed, will retry", "key", key, "part", partNum, "attempt", attempt, "err", err, "sleep", sleep)
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				// abort
				client.AbortMultipartUpload(context.Background(), &awsS3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: uploadID})
				return fmt.Errorf("context canceled while backing off: %w", ctx.Err())
			}
		}
		if partOut == nil {
			// abort upload
			client.AbortMultipartUpload(context.Background(), &awsS3.AbortMultipartUploadInput{Bucket: &bucket, Key: &key, UploadId: uploadID})
			return fmt.Errorf("upload part %d failed: %w", partNum, lastErr)
		}

		// append completed part
		pn := int32(partNum)
		completedParts = append(completedParts, awsS3types.CompletedPart{ETag: partOut.CopyPartResult.ETag, PartNumber: &pn})
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

// createLoggerWithOutput creates a zap logger that writes to the given writer
func createLoggerWithOutput(output io.Writer, level zapcore.Level) (*zap.Logger, error) {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeDuration = zapcore.StringDurationEncoder
	encoderConfig.StacktraceKey = ""

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(output),
		level,
	)

	return zap.New(core), nil
}
