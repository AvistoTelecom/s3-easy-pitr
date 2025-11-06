package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/AvistoTelecom/s3-easy-pitr/internal"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	rootCmd = &cobra.Command{
		Use:     "s3-easy-pitr",
		Short:   "Simple PITR (Point-In-Time Recovery) for S3-compatible storage",
		Long:    "s3-easy-pitr is a small utility to recover object versions from S3-compatible storage as of a given time.",
		Example: "# Run example using environment variables:\nS3_PITR_ENDPOINT=https://s3.example.com S3_PITR_ACCESS_KEY=AK... S3_PITR_SECRET_KEY=SK... S3_PITR_BUCKET=my-bucket S3_PITR_TARGET_TIME=2025-11-05T10:00:00Z s3-easy-pitr run --parallel 8",
	}
)

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringP("endpoint", "e", "", "S3 endpoint (required, env: S3_PITR_ENDPOINT)")
	rootCmd.PersistentFlags().StringP("access-key", "a", "", "S3 access key (env: S3_PITR_ACCESS_KEY)")
	rootCmd.PersistentFlags().StringP("secret-key", "s", "", "S3 secret key (env: S3_PITR_SECRET_KEY)")
	rootCmd.PersistentFlags().StringP("region", "r", "", "S3 region (optional, env: S3_PITR_REGION)")
	rootCmd.PersistentFlags().BoolP("insecure", "k", false, "Disable TLS verification / use http (env: S3_PITR_INSECURE)")
	rootCmd.PersistentFlags().StringP("bucket", "b", "", "S3 bucket to target (required, env: S3_PITR_BUCKET)")
	rootCmd.PersistentFlags().StringP("prefix", "p", "", "Prefix to filter objects (env: S3_PITR_PREFIX)")

	rootCmd.PersistentFlags().IntP("parallel", "n", 8, "Number of parallel workers (env: S3_PITR_PARALLEL)")
	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error (env: S3_PITR_LOG_LEVEL)")
	rootCmd.PersistentFlags().StringP("target-time", "t", "", "Target time for point-in-time recovery (RFC3339, e.g. 2025-11-05T10:00:00Z) (required, env: S3_PITR_TARGET_TIME)")

	// Copy/multipart tuning
	rootCmd.PersistentFlags().Int("copy-retries", 5, "Number of retries for copy operations (env: S3_PITR_COPY_RETRIES). Increase for flaky networks")
	rootCmd.PersistentFlags().String("copy-timeout", "2m", "Per-copy timeout duration (env: S3_PITR_COPY_TIMEOUT), e.g. 30s, 2m. Increase for slow endpoints or very large parts")
	rootCmd.PersistentFlags().String("copy-part-size", "200MB", "Part size for multipart copy (env: S3_PITR_COPY_PART_SIZE), e.g. 50MB, 200MB. Minimum 5MB")
	rootCmd.PersistentFlags().String("multipart-threshold", "1GB", "Objects larger than this use multipart copy (env: S3_PITR_MULTIPART_THRESHOLD). Default 1GB")

	viper.BindPFlag("endpoint", rootCmd.PersistentFlags().Lookup("endpoint"))
	viper.BindPFlag("access-key", rootCmd.PersistentFlags().Lookup("access-key"))
	viper.BindPFlag("secret-key", rootCmd.PersistentFlags().Lookup("secret-key"))
	viper.BindPFlag("region", rootCmd.PersistentFlags().Lookup("region"))
	viper.BindPFlag("insecure", rootCmd.PersistentFlags().Lookup("insecure"))
	viper.BindPFlag("bucket", rootCmd.PersistentFlags().Lookup("bucket"))
	viper.BindPFlag("prefix", rootCmd.PersistentFlags().Lookup("prefix"))

	viper.BindPFlag("parallel", rootCmd.PersistentFlags().Lookup("parallel"))
	viper.BindPFlag("log-level", rootCmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("target-time", rootCmd.PersistentFlags().Lookup("target-time"))
	viper.BindPFlag("copy-retries", rootCmd.PersistentFlags().Lookup("copy-retries"))
	viper.BindPFlag("copy-timeout", rootCmd.PersistentFlags().Lookup("copy-timeout"))
	viper.BindPFlag("copy-part-size", rootCmd.PersistentFlags().Lookup("copy-part-size"))
	viper.BindPFlag("multipart-threshold", rootCmd.PersistentFlags().Lookup("multipart-threshold"))

	// Environment variables setup
	viper.SetEnvPrefix("S3_PITR")
	viper.AutomaticEnv()

	// Explicitly bind config keys to env vars
	viper.BindEnv("endpoint")
	viper.BindEnv("access-key", "S3_PITR_ACCESS_KEY") // Handle hyphenated keys
	viper.BindEnv("secret-key", "S3_PITR_SECRET_KEY")
	viper.BindEnv("region")
	viper.BindEnv("insecure")
	viper.BindEnv("bucket")
	viper.BindEnv("prefix")
	viper.BindEnv("parallel")
	viper.BindEnv("log-level", "S3_PITR_LOG_LEVEL")
	viper.BindEnv("target-time", "S3_PITR_TARGET_TIME")
	viper.BindEnv("copy-retries", "S3_PITR_COPY_RETRIES")
	viper.BindEnv("copy-timeout", "S3_PITR_COPY_TIMEOUT")
	viper.BindEnv("copy-part-size", "S3_PITR_COPY_PART_SIZE")
	viper.BindEnv("multipart-threshold", "S3_PITR_MULTIPART_THRESHOLD")

	rootCmd.AddCommand(pitrCmd)
}

type contextKey string

const loggerContextKey contextKey = "logger"

func initConfig() {
	// initialize logger once using viper-configured log-level
	logLevel := strings.ToLower(viper.GetString("log-level")) // e.g. "info"

	var lvl zapcore.Level
	if err := lvl.Set(logLevel); err != nil {
		// fallback or handle invalid level
		fmt.Printf("Invalid log level %q, defaulting to info\n", logLevel)
		lvl = zapcore.InfoLevel
	}

	logger, err := internal.CreateLoggerWithOutput(os.Stdout, lvl)
	if err != nil {
		// if logger cannot be built, print and continue with default
		fmt.Fprintln(os.Stderr, "failed to initialize logger:", err)
		return
	}
	// replace global logger so code can use zap.L()/zap.S()
	zap.ReplaceGlobals(logger)
	// attach sugared logger into root context for convenience
	rootCmd.SetContext(context.WithValue(rootCmd.Context(), loggerContextKey, logger.Sugar()))
}

// Execute runs the root command
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
