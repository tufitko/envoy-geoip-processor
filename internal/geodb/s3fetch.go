package geodb

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of the S3 client used by S3Fetcher.
type S3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Fetcher downloads a database from S3, skipping unchanged ETags.
type S3Fetcher struct {
	Client S3API
	Bucket string
	Key    string
}

func parseS3URL(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if u.Scheme != "s3" || bucket == "" || key == "" {
		return "", "", fmt.Errorf("source must look like s3://bucket/key, got %q", raw)
	}
	return bucket, key, nil
}

// NewS3Fetcher builds a fetcher for an s3://bucket/key URL using the
// default AWS credential chain (env, IRSA, shared config).
func NewS3Fetcher(ctx context.Context, rawURL string) (*S3Fetcher, error) {
	bucket, key, err := parseS3URL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &S3Fetcher{Client: s3.NewFromConfig(cfg), Bucket: bucket, Key: key}, nil
}

func (f *S3Fetcher) Fetch(ctx context.Context, dst string, prev Meta) (bool, Meta, error) {
	head, err := f.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &f.Bucket, Key: &f.Key})
	if err != nil {
		return false, prev, fmt.Errorf("head s3://%s/%s: %w", f.Bucket, f.Key, err)
	}
	if etag := aws.ToString(head.ETag); etag != "" && etag == prev.ETag {
		return false, prev, nil
	}
	obj, err := f.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &f.Bucket, Key: &f.Key})
	if err != nil {
		return false, prev, fmt.Errorf("get s3://%s/%s: %w", f.Bucket, f.Key, err)
	}
	defer obj.Body.Close()
	if err := writeMMDB(obj.Body, dst); err != nil {
		return false, prev, err
	}
	return true, Meta{ETag: aws.ToString(obj.ETag)}, nil
}
