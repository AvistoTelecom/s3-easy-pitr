package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Config holds the connection configuration for an S3-compatible service
type Config struct {
	Endpoint string
}

// Client wraps AWS S3 client
type Client struct {
	S3 *s3.Client
}

// NewClient creates an S3 client using the aws-sdk-go-v2 that can target S3-compatible endpoints
func NewClient(ctx context.Context, c Config) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load default config for aws s3: %w", err)
	}

	var s3Options []func(*s3.Options)
	if c.Endpoint != "" {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(c.Endpoint)
		})
	}

	client := s3.NewFromConfig(cfg, s3Options...)

	return &Client{S3: client}, nil
}

// CheckVersioningEnabled checks if versioning is enabled on the bucket
func (c *Client) CheckVersioningEnabled(ctx context.Context, bucket string) error {
	input := &s3.GetBucketVersioningInput{
		Bucket: aws.String(bucket),
	}

	output, err := c.S3.GetBucketVersioning(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to get bucket versioning status: %w", err)
	}

	// Check if versioning is enabled
	if output.Status != "Enabled" {
		status := string(output.Status)
		if status == "" {
			status = "Disabled"
		}
		return fmt.Errorf("bucket versioning is not enabled (status: %s) - please enable versioning to use PITR", status)
	}
	return nil
}
