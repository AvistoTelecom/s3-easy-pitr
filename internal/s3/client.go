package s3

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds the connection configuration for an S3-compatible service
type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
	Insecure  bool
}

// Client wraps AWS S3 client
type Client struct {
	S3 *s3.Client
}

// NewClient creates an S3 client using the aws-sdk-go-v2 that can target S3-compatible endpoints
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	// Setup AWS config options
	var opts []func(*config.LoadOptions) error

	// Create a custom credentials provider chain that only uses static credentials
	credProvider := aws.NewCredentialsCache(
		credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
	)

	opts = append(opts,
		// Use our custom credentials provider
		config.WithCredentialsProvider(credProvider),
		// Disable EC2 IMDS completely
		config.WithEC2IMDSClientEnableState(imds.ClientDisabled),
	)
	// Configure HTTP client with longer timeout and optional insecure setting
	httpClient := &http.Client{
		Timeout: 30 * time.Second, // Increased timeout for slower S3-compatible endpoints
	}
	if cfg.Insecure {
		httpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}
	opts = append(opts, config.WithHTTPClient(httpClient))

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Configure S3-specific options
	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			// For S3-compatible endpoints path-style addressing is required
			o.UsePathStyle = true
		},
	}

	// Add custom endpoint if provided
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	// Create S3 client with endpoint options
	s3client := s3.NewFromConfig(awsCfg, s3Opts...)
	return &Client{S3: s3client}, nil
}
