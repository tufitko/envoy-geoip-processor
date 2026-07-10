// Package admin exposes health, readiness and metrics endpoints.
package admin

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Readier interface {
	Ready() bool
}

func Handler(r Readier, reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if r.Ready() {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "databases not loaded", http.StatusServiceUnavailable)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}
