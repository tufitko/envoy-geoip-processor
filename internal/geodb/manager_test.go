package geodb

import (
	"context"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"envoy-geoip-processor/internal/config"
)

// scriptedFetcher plays back a sequence of fetch outcomes.
type scriptedFetcher struct {
	mu    sync.Mutex
	steps []func(dst string) (bool, Meta, error)
	calls int
}

func (s *scriptedFetcher) Fetch(_ context.Context, dst string, prev Meta) (bool, Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.calls >= len(s.steps) {
		return false, prev, nil
	}
	step := s.steps[s.calls]
	s.calls++
	return step(dst)
}

func copyTestDB(t *testing.T, dst string) {
	t.Helper()
	b, err := os.ReadFile("../../testdata/GeoIP2-City-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func testConfig(dir string) *config.Config {
	return &config.Config{
		CacheDir: dir,
		Databases: map[string]config.Database{
			"city": {Source: "https://example.com/x", CheckInterval: config.Duration(time.Hour), Required: true},
		},
	}
}

func newTestManager(t *testing.T, dir string, f Fetcher) *Manager {
	t.Helper()
	m, err := NewManager(testConfig(dir), map[string]Fetcher{"city": f},
		slog.New(slog.NewTextHandler(os.Stderr, nil)), prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestManagerUpdateAndLookup(t *testing.T) {
	dir := t.TempDir()
	f := &scriptedFetcher{steps: []func(string) (bool, Meta, error){
		func(dst string) (bool, Meta, error) { copyTestDB(t, dst); return true, Meta{ETag: "v1"}, nil },
	}}
	m := newTestManager(t, dir, f)
	if m.Ready() {
		t.Fatal("must not be ready before load")
	}
	m.CheckNow(context.Background())
	if !m.Ready() {
		t.Fatal("must be ready after successful fetch")
	}
	path, _ := ParsePath("country.iso_code")
	got, found, err := m.Lookup("city", netip.MustParseAddr("2.125.160.216"), path)
	if err != nil || !found || got != "GB" {
		t.Fatalf("lookup: (%q, %v, %v)", got, found, err)
	}
	// meta persisted for restarts
	if _, err := os.Stat(filepath.Join(dir, "city.meta.json")); err != nil {
		t.Errorf("meta file not written: %v", err)
	}
}

func TestManagerKeepsOldOnBadDownload(t *testing.T) {
	dir := t.TempDir()
	f := &scriptedFetcher{steps: []func(string) (bool, Meta, error){
		func(dst string) (bool, Meta, error) { copyTestDB(t, dst); return true, Meta{ETag: "v1"}, nil },
		func(dst string) (bool, Meta, error) {
			os.WriteFile(dst, []byte("garbage"), 0o644)
			return true, Meta{ETag: "v2"}, nil
		},
	}}
	m := newTestManager(t, dir, f)
	m.CheckNow(context.Background())
	m.CheckNow(context.Background()) // корявый файл — reader должен остаться старым
	path, _ := ParsePath("country.iso_code")
	got, found, err := m.Lookup("city", netip.MustParseAddr("2.125.160.216"), path)
	if err != nil || !found || got != "GB" {
		t.Fatalf("lookup after bad update: (%q, %v, %v)", got, found, err)
	}
}

func TestManagerLoadCache(t *testing.T) {
	dir := t.TempDir()
	copyTestDB(t, filepath.Join(dir, "city.mmdb"))
	m := newTestManager(t, dir, &scriptedFetcher{})
	m.LoadCache()
	if !m.Ready() {
		t.Fatal("must be ready from disk cache without network")
	}
}

func TestManagerConcurrentLookupDuringSwap(t *testing.T) {
	dir := t.TempDir()
	swap := func(dst string) (bool, Meta, error) { copyTestDB(t, dst); return true, Meta{}, nil }
	f := &scriptedFetcher{steps: []func(string) (bool, Meta, error){swap, swap, swap, swap, swap}}
	m := newTestManager(t, dir, f)
	m.CheckNow(context.Background())
	path, _ := ParsePath("country.iso_code")
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				m.Lookup("city", netip.MustParseAddr("2.125.160.216"), path)
			}
		}()
	}
	for range 4 {
		m.CheckNow(context.Background())
	}
	wg.Wait()
}

func TestManagerUnknownDB(t *testing.T) {
	m := newTestManager(t, t.TempDir(), &scriptedFetcher{})
	if _, _, err := m.Lookup("nope", netip.MustParseAddr("1.1.1.1"), []any{"x"}); err == nil {
		t.Error("expected error for unknown db")
	}
}
