package s3

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// BucketStats holds statistics about a bucket
type BucketStats struct {
	UniqueKeys              int
	TotalVersionsAndMarkers int
	DeleteMarkers           int
	RecoverableFiles        int
	RecoverableKeys         []string // Keys that have recoverable versions
}

// GetBucketStats collects statistics about the bucket
// If targetTime is nil, it only counts files without checking recoverability
func GetBucketStats(ctx context.Context, client *s3.Client, bucket, prefix string, keys []string, targetTime *time.Time) (BucketStats, error) {
	stats := BucketStats{
		UniqueKeys:      len(keys),
		RecoverableKeys: make([]string, 0, len(keys)),
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	// Use a worker pool to count versions in parallel
	workers := 10
	if len(keys) < workers {
		workers = len(keys)
	}

	keysCh := make(chan string, len(keys))
	for _, k := range keys {
		keysCh <- k
	}
	close(keysCh)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range keysCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// List versions for this key
				paginator := s3.NewListObjectVersionsPaginator(client, &s3.ListObjectVersionsInput{
					Bucket: &bucket,
					Prefix: &key,
				})

				versionsCount := 0
				deleteMarkersCount := 0
				hasRecoverableVersion := false

				for paginator.HasMorePages() {
					page, err := paginator.NextPage(ctx)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("list versions for key %s: %w", key, err):
						default:
						}
						return
					}

					for _, version := range page.Versions {
						if version.Key != nil && *version.Key == key {
							versionsCount++
							// Check if this version is recoverable (at or before target time)
							if targetTime != nil && version.LastModified != nil &&
								(version.LastModified.Before(*targetTime) || version.LastModified.Equal(*targetTime)) {
								hasRecoverableVersion = true
							}
						}
					}

					// Count delete markers
					for _, marker := range page.DeleteMarkers {
						if marker.Key != nil && *marker.Key == key {
							deleteMarkersCount++
						}
					}
				}

				mu.Lock()
				stats.TotalVersionsAndMarkers += versionsCount + deleteMarkersCount
				stats.DeleteMarkers += deleteMarkersCount
				if targetTime == nil || hasRecoverableVersion {
					stats.RecoverableFiles++
					stats.RecoverableKeys = append(stats.RecoverableKeys, key)
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return stats, err
	}

	return stats, nil
}

// GetDestroyStats collects statistics for the destroy command
// This is a faster version that doesn't need to check individual keys
func GetDestroyStats(ctx context.Context, client *s3.Client, bucket, prefix string) (BucketStats, error) {
	stats := BucketStats{}
	uniqueKeys := make(map[string]struct{})

	var continuationToken *string
	totalVersions := 0

	for {
		output, err := client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:    aws.String(bucket),
			Prefix:    aws.String(prefix),
			KeyMarker: continuationToken,
			MaxKeys:   aws.Int32(1000),
		})
		if err != nil {
			return stats, fmt.Errorf("list object versions: %w", err)
		}

		// Count versions and track unique keys
		for _, version := range output.Versions {
			if version.Key != nil {
				uniqueKeys[*version.Key] = struct{}{}
			}
		}

		// Track keys from delete markers as well
		for _, marker := range output.DeleteMarkers {
			if marker.Key != nil {
				uniqueKeys[*marker.Key] = struct{}{}
			}
		}

		totalVersions += len(output.Versions)
		stats.DeleteMarkers += len(output.DeleteMarkers)

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextKeyMarker
	}

	stats.UniqueKeys = len(uniqueKeys)
	stats.TotalVersionsAndMarkers = totalVersions + stats.DeleteMarkers

	return stats, nil
}
