// Package geodb manages MaxMind databases: download, refresh, lookup.
package geodb

import (
	"fmt"
	"net/netip"
	"strconv"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2"
)

// ParsePath converts "subdivisions.0.iso_code" into DecodePath elements;
// numeric segments become array indices.
func ParsePath(s string) ([]any, error) {
	if s == "" {
		return nil, fmt.Errorf("empty path")
	}
	parts := strings.Split(s, ".")
	out := make([]any, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			return nil, fmt.Errorf("empty segment in path %q", s)
		}
		if n, err := strconv.Atoi(p); err == nil {
			out = append(out, n)
		} else {
			out = append(out, p)
		}
	}
	return out, nil
}

// FormatValue renders a decoded scalar as a header value.
func FormatValue(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case bool:
		return strconv.FormatBool(t), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case map[string]any, []any:
		return "", fmt.Errorf("path resolves to a composite value (%T); point it at a scalar", v)
	default:
		// int/uint of any width the decoder produces
		return fmt.Sprint(t), nil
	}
}

// LookupPath looks ip up in r and decodes the value at path.
// found=false when the IP is absent or the record has nothing at path.
func LookupPath(r *maxminddb.Reader, ip netip.Addr, path []any) (string, bool, error) {
	res := r.Lookup(ip)
	if err := res.Err(); err != nil {
		return "", false, err
	}
	if !res.Found() {
		return "", false, nil
	}
	var v any
	if err := res.DecodePath(&v, path...); err != nil {
		return "", false, err
	}
	if v == nil {
		return "", false, nil
	}
	s, err := FormatValue(v)
	if err != nil {
		return "", false, err
	}
	return s, true, nil
}
