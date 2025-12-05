package cmd

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/AvistoTelecom/s3-easy-pitr/internal/pitr"
	s3client "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
)

var pitrCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a point-in-time recovery",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		// Early validation with friendly messages
		endpoint := viper.GetString("endpoint")
		bucket := viper.GetString("bucket")
		access := viper.GetString("access-key")
		secret := viper.GetString("secret-key")
		targetTimeStr := viper.GetString("target-time")

		if endpoint == "" {
			return fmt.Errorf("missing required option: --endpoint or environment variable S3_PITR_ENDPOINT")
		}
		if bucket == "" {
			return fmt.Errorf("missing required option: --bucket or environment variable S3_PITR_BUCKET")
		}
		if access == "" {
			return fmt.Errorf("missing required option: --access-key or environment variable S3_PITR_ACCESS_KEY")
		}
		if secret == "" {
			return fmt.Errorf("missing required option: --secret-key or environment variable S3_PITR_SECRET_KEY")
		}
		if targetTimeStr == "" {
			return fmt.Errorf("missing required option: --target-time (RFC3339) or environment variable S3_PITR_TARGET_TIME")
		}
		if _, err := time.Parse(time.RFC3339, targetTimeStr); err != nil {
			return fmt.Errorf("invalid --target-time: must be RFC3339 (example: 2025-11-05T10:00:00Z): %w", err)
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// use root-level logger (initialized in initConfig)
		sugar := zap.S()

		endpoint := viper.GetString("endpoint")
		access := viper.GetString("access-key")
		secret := viper.GetString("secret-key")
		region := viper.GetString("region")
		insecure := viper.GetBool("insecure")
		bucket := viper.GetString("bucket")
		prefix := viper.GetString("prefix")

		targetTimeStr := viper.GetString("target-time")
		if targetTimeStr == "" {
			return fmt.Errorf("target-time is required (format: RFC3339)")
		}
		targetTime, err := time.Parse(time.RFC3339, targetTimeStr)
		if err != nil {
			return fmt.Errorf("invalid target time format (should be RFC3339, e.g. 2025-11-05T10:00:00Z): %w", err)
		}

		parallel := viper.GetInt("parallel")

		if endpoint == "" {
			return fmt.Errorf("endpoint is required (flag or S3_PITR_ENDPOINT)")
		}
		if bucket == "" {
			return fmt.Errorf("bucket is required (flag or S3_PITR_BUCKET)")
		}
		if access == "" {
			return fmt.Errorf("access key is required (flag or S3_PITR_ACCESS_KEY)")
		}
		if secret == "" {
			return fmt.Errorf("secret key is required (flag or S3_PITR_SECRET_KEY)")
		}

		if parallel <= 0 {
			parallel = 4
		}

		sugar.Infow("starting pitr", "endpoint", endpoint, "bucket", bucket, "prefix", prefix, "parallel", parallel)

		cfg := s3client.Config{
			Endpoint:  endpoint,
			AccessKey: access,
			SecretKey: secret,
			Region:    region,
			Insecure:  insecure,
		}

		client, err := s3client.NewClient(context.Background(), cfg)
		if err != nil {
			return fmt.Errorf("create s3 client: %w", err)
		}

		// parse copy/multipart tuning values from viper
		copyRetries := viper.GetInt("copy-retries")
		copyTimeoutStr := viper.GetString("copy-timeout")
		copyTimeout := 2 * time.Minute
		if copyTimeoutStr != "" {
			if d, err := time.ParseDuration(copyTimeoutStr); err == nil {
				copyTimeout = d
			} else {
				return fmt.Errorf("invalid copy-timeout: %w", err)
			}
		}

		// parse sizes like "100MB" into bytes
		parseSize := func(s string) (int64, error) {
			s = strings.TrimSpace(s)
			if s == "" {
				return 0, nil
			}
			// normalize
			u := strings.ToUpper(s)
			mul := int64(1)
			if strings.HasSuffix(u, "KB") {
				mul = 1024
				u = strings.TrimSuffix(u, "KB")
			} else if strings.HasSuffix(u, "MB") {
				mul = 1024 * 1024
				u = strings.TrimSuffix(u, "MB")
			} else if strings.HasSuffix(u, "GB") {
				mul = 1024 * 1024 * 1024
				u = strings.TrimSuffix(u, "GB")
			} else if strings.HasSuffix(u, "B") {
				mul = 1
				u = strings.TrimSuffix(u, "B")
			}
			u = strings.TrimSpace(u)
			v, err := strconv.ParseFloat(u, 64)
			if err != nil {
				return 0, err
			}
			return int64(v * float64(mul)), nil
		}

		partSizeStr := viper.GetString("copy-part-size")
		partSize, err := parseSize(partSizeStr)
		if err != nil || partSize <= 0 {
			// default 200MB
			partSize = 200 * 1024 * 1024
		}
		multipartThresholdStr := viper.GetString("multipart-threshold")
		multipartThreshold, err := parseSize(multipartThresholdStr)
		if err != nil || multipartThreshold <= 0 {
			multipartThreshold = 1 * 1024 * 1024 * 1024
		}

		opts := pitr.Options{
			Client: client,
			Bucket: bucket,
			Prefix: prefix,
			Target: targetTime,

			Parallel:           parallel,
			Logger:             sugar,
			PrintFreq:          500 * time.Millisecond,
			CopyRetries:        copyRetries,
			CopyTimeout:        copyTimeout,
			CopyPartSize:       partSize,
			MultipartThreshold: multipartThreshold,
		}

		if err := pitr.Run(context.Background(), opts); err != nil {
			sugar.Errorw("pitr failed, your bucket might be in an inconsistent state, please either run previous command again or change target time if you want to rollback", "error", err)
			return err
		}

		return nil
	},
}

func init() {
	// Add example: allow reading env var S3_PITR_PARALLEL as integer
	if v := os.Getenv("S3_PITR_PARALLEL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			viper.Set("parallel", n)
		}
	}
}
