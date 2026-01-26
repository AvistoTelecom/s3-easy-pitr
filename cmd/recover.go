package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal/pitr"
	s3client "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
)

var recover = &cobra.Command{
	Use:     "recover",
	Short:   "Recover at a point-in-time",
	Example: rootCmd.Example,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Early validation with friendly messages
		bucket := viper.GetString("bucket")
		targetTimeStr := viper.GetString("target-time")

		if bucket == "" {
			return fmt.Errorf("missing required option: --bucket or environment variable S3_PITR_BUCKET")
		}

		if targetTimeStr == "" {
			return fmt.Errorf("missing required option: --target-time (RFC3339) or environment variable S3_PITR_TARGET_TIME")
		}

		targetTime, err := time.Parse(time.RFC3339, targetTimeStr)
		if err != nil {
			return fmt.Errorf("invalid --target-time: must be RFC3339 (example: 2025-11-05T10:00:00Z): %w", err)
		}

		if targetTime.After(time.Now()) {
			return fmt.Errorf("--target-time must be in the past, not in the future")
		}

		// validate parallel workers count
		parallel := viper.GetInt("parallel")
		if parallel <= 0 {
			return fmt.Errorf("invalid --parallel: must be > 0")
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		// use root-level logger (initialized in initConfig)
		sugar := zap.S()

		endpoint := viper.GetString("endpoint")
		bucket := viper.GetString("bucket")
		prefix := viper.GetString("prefix")
		parallel := viper.GetInt("parallel")
		remove := viper.GetBool("remove")

		// We don't check for errors because it is already checked
		// on PreRunE function
		targetTimeStr := viper.GetString("target-time")
		targetTime, _ := time.Parse(time.RFC3339, targetTimeStr)

		cfg := s3client.Config{
			Endpoint: endpoint,
		}

		client, err := s3client.NewClient(context.Background(), cfg)
		if err != nil {
			sugar.Fatalf("create s3 client: %v", err)
		}

		// Check if bucket versioning is enabled
		if err := client.CheckVersioningEnabled(context.Background(), bucket); err != nil {
			sugar.Fatalf("bucket versioning check failed: %v", err)
		}

		// parse copy/multipart tuning values from viper
		copyRetries := viper.GetInt("copy-retries")
		copyTimeoutStr := viper.GetString("copy-timeout")
		copyTimeout := 2 * time.Minute
		if copyTimeoutStr != "" {
			if d, err := time.ParseDuration(copyTimeoutStr); err == nil {
				copyTimeout = d
			} else {
				sugar.Fatalf("invalid copy-timeout: %v", err)
			}
		}

		partSizeStr := viper.GetString("copy-part-size")
		partSize, err := s3client.ParseBytes(partSizeStr)
		if err != nil || partSize <= 0 {
			// default 200MB
			partSize = 200 * 1024 * 1024
		}
		multipartThresholdStr := viper.GetString("multipart-threshold")
		multipartThreshold, err := s3client.ParseBytes(multipartThresholdStr)
		if err != nil || multipartThreshold <= 0 {
			multipartThreshold = 1 * 1024 * 1024 * 1024
		}

		opts := pitr.Options{
			Client: client,
			Bucket: bucket,
			Prefix: prefix,
			Target: targetTime,
			Remove: remove,

			Parallel:           parallel,
			Logger:             sugar,
			PrintFreq:          500 * time.Millisecond,
			CopyRetries:        copyRetries,
			CopyTimeout:        copyTimeout,
			CopyPartSize:       partSize,
			MultipartThreshold: multipartThreshold,
		}
		sugar.Infow("Starting PITR", "endpoint", endpoint, "bucket", bucket, "prefix", prefix, "parallel", parallel)

		if err := pitr.Run(context.Background(), opts); err != nil {
			sugar.Fatalw("Recovery failed, your bucket might be in an inconsistent state, please either run previous command again or change target time if you want to rollback", "error", err)
		}

		sugar.Infow("PITR finished", "endpoint", endpoint, "bucket", bucket, "prefix", prefix)
	},
}

func init() {
	// Add --remove flag to the pitr command
	recover.Flags().Bool("remove", false, "Delete files that didn't exist at target time (env: S3_PITR_REMOVE)")
	viper.BindPFlag("remove", recover.Flags().Lookup("remove"))
	viper.BindEnv("remove", "S3_PITR_REMOVE")

	// Add example: allow reading env var S3_PITR_PARALLEL as integer
	if v := os.Getenv("S3_PITR_PARALLEL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			viper.Set("parallel", n)
		}
	}
}
