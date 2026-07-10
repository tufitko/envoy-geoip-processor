package geodb

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeS3 struct {
	etag string
	body []byte
	gets int
}

func (f *fakeS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ETag: aws.String(f.etag)}, nil
}

func (f *fakeS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets++
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.body)), ETag: aws.String(f.etag)}, nil
}

func TestS3Fetcher(t *testing.T) {
	db := mmdbBytes(t)
	fake := &fakeS3{etag: `"abc"`, body: db}
	f := &S3Fetcher{Client: fake, Bucket: "b", Key: "city.mmdb"}
	dst := filepath.Join(t.TempDir(), "city.mmdb")

	changed, meta, err := f.Fetch(context.Background(), dst, Meta{})
	if err != nil || !changed || meta.ETag != `"abc"` {
		t.Fatalf("first: changed=%v meta=%+v err=%v", changed, meta, err)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, db) {
		t.Error("downloaded file differs")
	}

	changed, _, err = f.Fetch(context.Background(), dst, meta)
	if err != nil || changed || fake.gets != 1 {
		t.Fatalf("second: changed=%v gets=%d err=%v, want unchanged with no extra GET", changed, fake.gets, err)
	}
}

func TestParseS3URL(t *testing.T) {
	bucket, key, err := parseS3URL("s3://my-bucket/path/to/asn.mmdb")
	if err != nil || bucket != "my-bucket" || key != "path/to/asn.mmdb" {
		t.Errorf("got %q %q %v", bucket, key, err)
	}
	if _, _, err := parseS3URL("s3:///nokey"); err == nil {
		t.Error("expected error for missing bucket")
	}
}
