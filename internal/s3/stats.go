package s3

import (
	"context"
	"fmt"
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
func GetBucketStats(ctx context.Context, client *s3.Client, bucket, prefix string, targetTime *time.Time) (BucketStats, error) {
	stats := BucketStats{
		RecoverableKeys: make([]string, 0),
	}

	// Map to track unique keys and their version info
	type keyInfo struct {
		versionsCount      int
		deleteMarkersCount int
		hasRecoverable     bool
	}
	keyMap := make(map[string]*keyInfo)

	var keyMarker *string
	var versionIdMarker *string

	// Single pass through all versions
	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1000),
		}

		// Set continuation markers if present
		if keyMarker != nil {
			input.KeyMarker = keyMarker
		}
		if versionIdMarker != nil {
			input.VersionIdMarker = versionIdMarker
		}

		output, err := client.ListObjectVersions(ctx, input)
		if err != nil {
			return stats, fmt.Errorf("list object versions: %w", err)
		}

		// Process versions
		for _, version := range output.Versions {
			if version.Key == nil {
				continue
			}
			key := *version.Key

			if keyMap[key] == nil {
				keyMap[key] = &keyInfo{}
			}
			keyMap[key].versionsCount++

			// Check if this version is recoverable
			if targetTime != nil && version.LastModified != nil &&
				(version.LastModified.Before(*targetTime) || version.LastModified.Equal(*targetTime)) {
				keyMap[key].hasRecoverable = true
			}
		}

		// Process delete markers
		for _, marker := range output.DeleteMarkers {
			if marker.Key == nil {
				continue
			}
			key := *marker.Key

			if keyMap[key] == nil {
				keyMap[key] = &keyInfo{}
			}
			keyMap[key].deleteMarkersCount++
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}

		// Update both markers for next iteration
		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker
	}

	// Aggregate statistics
	for key, info := range keyMap {
		stats.TotalVersionsAndMarkers += info.versionsCount + info.deleteMarkersCount
		stats.DeleteMarkers += info.deleteMarkersCount

		if targetTime == nil || info.hasRecoverable {
			stats.RecoverableFiles++
			stats.RecoverableKeys = append(stats.RecoverableKeys, key)
		}
	}

	stats.UniqueKeys = len(keyMap)

	return stats, nil
}

// GetDestroyStats collects statistics for the destroy command
// This is a faster version that doesn't need to check individual keys
func GetDestroyStats(ctx context.Context, client *s3.Client, bucket, prefix string) (BucketStats, error) {
	stats := BucketStats{}
	uniqueKeys := make(map[string]struct{})

	var keyMarker *string
	var versionIdMarker *string
	totalVersions := 0

	for {
		input := &s3.ListObjectVersionsInput{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(prefix),
			MaxKeys: aws.Int32(1000),
		}

		// Set continuation markers if present
		if keyMarker != nil {
			input.KeyMarker = keyMarker
		}
		if versionIdMarker != nil {
			input.VersionIdMarker = versionIdMarker
		}

		output, err := client.ListObjectVersions(ctx, input)
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

		// Update both markers for next iteration
		keyMarker = output.NextKeyMarker
		versionIdMarker = output.NextVersionIdMarker
	}

	stats.UniqueKeys = len(uniqueKeys)
	stats.TotalVersionsAndMarkers = totalVersions + stats.DeleteMarkers

	return stats, nil
}
