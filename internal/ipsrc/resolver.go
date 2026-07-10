// Package ipsrc picks the client IP from an ordered chain of sources.
package ipsrc

import (
	"net/netip"
	"strings"

	"envoy-geoip-processor/internal/config"
)

type Resolver struct {
	sources []config.IPSource
}

func New(sources []config.IPSource) *Resolver { return &Resolver{sources: sources} }

// Resolve walks the chain and returns the first valid IP.
// headers must be keyed by lowercase name. A comma-separated value is
// treated as a proxy list and its first (leftmost) element is used.
func (r *Resolver) Resolve(headers map[string]string, envoySourceAddr string) (netip.Addr, bool) {
	for _, s := range r.sources {
		var candidate string
		switch {
		case s.Header != "":
			candidate = headers[s.Header]
			if i := strings.IndexByte(candidate, ','); i >= 0 {
				candidate = candidate[:i]
			}
		case s.Envoy == "source_address":
			candidate = envoySourceAddr
		}
		if ip, ok := parseAddr(candidate); ok {
			return ip, true
		}
	}
	return netip.Addr{}, false
}

func parseAddr(s string) (netip.Addr, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return netip.Addr{}, false
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap(), true
	}
	if ip, err := netip.ParseAddr(s); err == nil {
		return ip.Unmap(), true
	}
	return netip.Addr{}, false
}
