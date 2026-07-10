package geodb

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
)

// Meta identifies the remote version of a database file; it is persisted
// next to the cached .mmdb and fed back into Fetch for conditional requests.
type Meta struct {
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
}

// Fetcher downloads a database when the remote differs from prev.
// changed=false means dst was not touched.
type Fetcher interface {
	Fetch(ctx context.Context, dst string, prev Meta) (changed bool, next Meta, err error)
}

// writeMMDB writes the payload to dst. A gzip stream is treated as tar.gz
// and the first *.mmdb member is extracted; anything else is copied as-is.
func writeMMDB(r io.Reader, dst string) error {
	br := bufio.NewReader(r)
	magic, err := br.Peek(2)
	if err == nil && magic[0] == 0x1f && magic[1] == 0x8b {
		gz, err := gzip.NewReader(br)
		if err != nil {
			return err
		}
		defer gz.Close()
		tr := tar.NewReader(gz)
		for {
			hdr, err := tr.Next()
			if err == io.EOF {
				return fmt.Errorf("no .mmdb file inside archive")
			}
			if err != nil {
				return err
			}
			if hdr.Typeflag == tar.TypeReg && strings.HasSuffix(hdr.Name, ".mmdb") {
				return copyToFile(tr, dst)
			}
		}
	}
	return copyToFile(br, dst)
}

func copyToFile(r io.Reader, dst string) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
