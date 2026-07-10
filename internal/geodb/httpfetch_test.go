package geodb

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func mmdbBytes(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../testdata/GeoIP2-City-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func tarGz(t *testing.T, name string, body []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	return buf.Bytes()
}

func TestHTTPFetcher(t *testing.T) {
	db := mmdbBytes(t)
	var gotAuth, gotINM string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotINM = r.Header.Get("If-None-Match")
		if gotINM == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Write(db)
	}))
	defer srv.Close()

	t.Setenv("TEST_LICENSE", "user:secret")
	f := &HTTPFetcher{URL: srv.URL, BasicEnv: "TEST_LICENSE"}
	dst := filepath.Join(t.TempDir(), "city.mmdb")

	changed, meta, err := f.Fetch(context.Background(), dst, Meta{})
	if err != nil || !changed || meta.ETag != `"v1"` {
		t.Fatalf("first fetch: changed=%v meta=%+v err=%v", changed, meta, err)
	}
	if gotAuth == "" {
		t.Error("basic auth header not sent")
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, db) {
		t.Error("downloaded file differs")
	}

	changed, _, err = f.Fetch(context.Background(), dst, meta)
	if err != nil || changed {
		t.Fatalf("second fetch: changed=%v err=%v, want unchanged", changed, err)
	}
	if gotINM != `"v1"` {
		t.Errorf("If-None-Match not sent, got %q", gotINM)
	}
}

func TestHTTPFetcherTarGz(t *testing.T) {
	db := mmdbBytes(t)
	archive := tarGz(t, "GeoIP2-City_20260101/GeoIP2-City.mmdb", db)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(archive)
	}))
	defer srv.Close()

	dst := filepath.Join(t.TempDir(), "city.mmdb")
	changed, _, err := (&HTTPFetcher{URL: srv.URL}).Fetch(context.Background(), dst, Meta{})
	if err != nil || !changed {
		t.Fatalf("changed=%v err=%v", changed, err)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, db) {
		t.Error("extracted mmdb differs from original")
	}
}

func TestHTTPFetcherErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer srv.Close()
	if _, _, err := (&HTTPFetcher{URL: srv.URL}).Fetch(context.Background(), filepath.Join(t.TempDir(), "x"), Meta{}); err == nil {
		t.Error("expected error on 403")
	}
	// tar.gz without .mmdb inside
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(tarGz(t, "README.txt", []byte("hi")))
	}))
	defer srv2.Close()
	if _, _, err := (&HTTPFetcher{URL: srv2.URL}).Fetch(context.Background(), filepath.Join(t.TempDir(), "x"), Meta{}); err == nil {
		t.Error("expected error when archive has no .mmdb")
	}
}
