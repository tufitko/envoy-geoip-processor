package ipsrc

import (
	"net/netip"
	"testing"

	"envoy-geoip-processor/internal/config"
)

func chain() *Resolver {
	return New([]config.IPSource{
		{Header: "x-real-ip"},
		{Header: "x-forwarded-for"},
		{Envoy: "source_address"},
	})
}

func TestResolve(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
		envoy   string
		want    string
		ok      bool
	}{
		{"real ip wins", map[string]string{"x-real-ip": "198.51.100.7", "x-forwarded-for": "203.0.113.5"}, "", "198.51.100.7", true},
		{"garbage falls through", map[string]string{"x-real-ip": "not-an-ip", "x-forwarded-for": "203.0.113.5, 10.0.0.1"}, "", "203.0.113.5", true},
		{"xff first element", map[string]string{"x-forwarded-for": "203.0.113.5, 10.0.0.1"}, "", "203.0.113.5", true},
		{"envoy fallback with port", map[string]string{}, "203.0.113.9:41234", "203.0.113.9", true},
		{"ipv6 with port", map[string]string{"x-real-ip": "[2001:db8::1]:443"}, "", "2001:db8::1", true},
		{"v4-mapped unmapped", map[string]string{"x-real-ip": "::ffff:192.0.2.1"}, "", "192.0.2.1", true},
		{"nothing valid", map[string]string{"x-real-ip": "zz"}, "junk", "", false},
	}
	for _, c := range cases {
		got, ok := chain().Resolve(c.headers, c.envoy)
		if ok != c.ok {
			t.Errorf("%s: ok=%v want %v", c.name, ok, c.ok)
			continue
		}
		if ok && got != netip.MustParseAddr(c.want) {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
