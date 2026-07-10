package admin

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type fakeReady bool

func (f fakeReady) Ready() bool { return bool(f) }

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestEndpoints(t *testing.T) {
	reg := prometheus.NewRegistry()
	ready := Handler(fakeReady(true), reg)
	notReady := Handler(fakeReady(false), reg)

	if got := get(t, ready, "/healthz").Code; got != http.StatusOK {
		t.Errorf("healthz: %d", got)
	}
	if got := get(t, ready, "/readyz").Code; got != http.StatusOK {
		t.Errorf("readyz ready: %d", got)
	}
	if got := get(t, notReady, "/readyz").Code; got != http.StatusServiceUnavailable {
		t.Errorf("readyz not ready: %d", got)
	}
	if got := get(t, ready, "/metrics").Code; got != http.StatusOK {
		t.Errorf("metrics: %d", got)
	}
}
