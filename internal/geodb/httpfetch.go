package geodb

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// HTTPFetcher downloads a database over HTTP(S) using conditional requests.
type HTTPFetcher struct {
	URL      string
	BasicEnv string // env var with "user:password"; empty = no auth
	Client   *http.Client
}

func (h *HTTPFetcher) Fetch(ctx context.Context, dst string, prev Meta) (bool, Meta, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return false, prev, err
	}
	if prev.ETag != "" {
		req.Header.Set("If-None-Match", prev.ETag)
	}
	if prev.LastModified != "" {
		req.Header.Set("If-Modified-Since", prev.LastModified)
	}
	if h.BasicEnv != "" {
		user, pass, ok := strings.Cut(os.Getenv(h.BasicEnv), ":")
		if !ok {
			return false, prev, fmt.Errorf("env %s must contain user:password", h.BasicEnv)
		}
		req.SetBasicAuth(user, pass)
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return false, prev, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusNotModified:
		return false, prev, nil
	case resp.StatusCode != http.StatusOK:
		return false, prev, fmt.Errorf("GET %s: unexpected status %s", h.URL, resp.Status)
	}
	if err := writeMMDB(resp.Body, dst); err != nil {
		return false, prev, err
	}
	return true, Meta{ETag: resp.Header.Get("ETag"), LastModified: resp.Header.Get("Last-Modified")}, nil
}
