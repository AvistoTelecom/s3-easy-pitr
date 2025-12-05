package cmd

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	s3client "github.com/AvistoTelecom/s3-easy-pitr/internal/s3"
	"github.com/AvistoTelecom/s3-easy-pitr/internal/test"
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Test utilities for S3 bucket operations",
	Long:  "Commands to generate test data and manage S3 buckets for testing purposes",
}

var generateCmd = &cobra.Command{
	Use:           "generate",
	Short:         "Generate and upload random files to S3",
	Long:          "Generate random files and upload them to an S3 bucket with configurable parallelism",
	SilenceErrors: true,
	Example: `  # Generate 500 files with random sizes (1KB-1MB each)
  s3-easy-pitr test generate --num-files 500

  # Generate 100 files totaling exactly 100MB
  s3-easy-pitr test generate --num-files 100 --total-size 100MB

  # Generate 1000 files with 20 parallel uploads
  s3-easy-pitr test generate --num-files 1000 --parallel 20`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		endpoint := viper.GetString("endpoint")
		bucket := viper.GetString("bucket")
		access := viper.GetString("access-key")
		secret := viper.GetString("secret-key")

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
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runGenerate()
	},
}

var destroyCmd = &cobra.Command{
	Use:           "destroy",
	Short:         "Destroy an S3 bucket",
	Long:          "Delete all objects, versions, and delete markers from a bucket, then delete the bucket itself",
	SilenceErrors: true,
	Example: `  # Destroy a bucket (will prompt for confirmation)
  s3-easy-pitr test destroy

  # Destroy a specific bucket
  s3-easy-pitr test destroy --bucket my-old-bucket`,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		endpoint := viper.GetString("endpoint")
		bucket := viper.GetString("bucket")
		access := viper.GetString("access-key")
		secret := viper.GetString("secret-key")

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
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return runDestroy()
	},
}

func init() {
	// Add flags for generate command
	generateCmd.Flags().Int64("num-files", 1000, "Number of files to generate")
	generateCmd.Flags().String("total-size", "0", "Total size (e.g. 100MB, 1GB). If 0, random sizes 1KB-1MB per file")
	// Note: parallel flag is inherited from global persistent flags

	viper.BindPFlag("test.num-files", generateCmd.Flags().Lookup("num-files"))
	viper.BindPFlag("test.total-size", generateCmd.Flags().Lookup("total-size"))

	// Add flags for destroy command
	destroyCmd.Flags().Bool("delete-bucket", false, "Delete the bucket after emptying it")
	viper.BindPFlag("test.delete-bucket", destroyCmd.Flags().Lookup("delete-bucket"))

	// Add subcommands to test command
	testCmd.AddCommand(generateCmd)
	testCmd.AddCommand(destroyCmd)

	// Add test command to root
	rootCmd.AddCommand(testCmd)
}

func runGenerate() error {
	sugar := zap.S()

	endpoint := viper.GetString("endpoint")
	access := viper.GetString("access-key")
	secret := viper.GetString("secret-key")
	region := viper.GetString("region")
	insecure := viper.GetBool("insecure")
	bucket := viper.GetString("bucket")

	numFiles := viper.GetInt("test.num-files")
	totalSizeStr := viper.GetString("test.total-size")
	maxParallel := viper.GetInt("parallel")

	totalSize, err := s3client.ParseBytes(totalSizeStr)
	if err != nil {
		return fmt.Errorf("invalid total-size: %w", err)
	}

	if numFiles <= 0 {
		return fmt.Errorf("num-files must be greater than 0")
	}
	if maxParallel <= 0 {
		maxParallel = 10
	}

	sugar.Infow("Starting S3 files generation",
		"endpoint", endpoint,
		"region", region,
		"bucket", bucket,
		"num_files", numFiles,
		"parallel", maxParallel,
	)

	if totalSize > 0 {
		sugar.Infow("Total size target", "bytes", totalSize, "human", s3client.FormatBytes(totalSize))
	} else {
		sugar.Infow("File sizes", "mode", "random (1KB - 1MB per file)")
	}

	// Create S3 client
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

	// Check if bucket versioning is enabled
	client.CheckVersioningEnabled(context.Background(), bucket)

	// Generate file sizes
	fileSizes := test.GenerateFileSizes(numFiles, totalSize)

	sugar.Infow("Calculated file sizes",
		"num_files", len(fileSizes),
		"actual_total_bytes", sumSizes(fileSizes),
		"actual_total_human", s3client.FormatBytes(sumSizes(fileSizes)),
	)

	// Upload files in parallel with progress bar
	return test.UploadFilesWithProgress(context.Background(), test.UploadOptions{
		Client:      client.S3,
		Bucket:      bucket,
		FileSizes:   fileSizes,
		MaxParallel: maxParallel,
		Logger:      sugar,
	})
}

func runDestroy() error {
	sugar := zap.S()

	endpoint := viper.GetString("endpoint")
	access := viper.GetString("access-key")
	secret := viper.GetString("secret-key")
	region := viper.GetString("region")
	insecure := viper.GetBool("insecure")
	bucket := viper.GetString("bucket")
	deleteBucket := viper.GetBool("test.delete-bucket")

	// Use fmt for interactive warnings since this needs user attention
	fmt.Printf("\n⚠️  WARNING: You are about to empty the bucket: %s\n", bucket)
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Printf("Region: %s\n", region)
	fmt.Println("\nThis will:")
	fmt.Println("  1. Delete all objects and versions in the bucket")
	if deleteBucket {
		fmt.Println("  2. Delete the bucket itself")
	} else {
		fmt.Println("  2. Keep the empty bucket (use --delete-bucket to delete it)")
	}
	fmt.Println("\nThis action CANNOT be undone!")
	fmt.Print("\nType the bucket name to confirm: ")

	reader := bufio.NewReader(os.Stdin)
	confirmation, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read confirmation: %w", err)
	}

	confirmation = strings.TrimSpace(confirmation)
	if confirmation != bucket {
		sugar.Infow("Bucket destruction cancelled - name did not match")
		return nil
	}

	// Create S3 client
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

	// Check if bucket versioning is enabled
	client.CheckVersioningEnabled(context.Background(), bucket)

	// Destroy bucket with progress bar
	return test.DestroyBucketWithProgress(context.Background(), test.DestroyOptions{
		Client:       client.S3,
		Bucket:       bucket,
		DeleteBucket: deleteBucket,
		Logger:       sugar,
	})
}

// sumSizes calculates the total of all file sizes
func sumSizes(sizes []int64) int64 {
	var total int64
	for _, size := range sizes {
		total += size
	}
	return total
}
