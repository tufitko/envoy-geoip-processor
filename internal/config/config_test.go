package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

const valid = `
cache_dir: /tmp/geoip
ip_sources:
  - header: X-Real-IP
  - header: x-forwarded-for
  - envoy: source_address
databases:
  city:
    source: https://example.com/city.tar.gz
    auth: {basic_env: MAXMIND_LICENSE}
    check_interval: 1h
    required: true
  asn:
    source: s3://bucket/asn.mmdb
headers:
  X-GeoIP-Country-Code: {db: city, path: country.iso_code}
  x-geoip-asn: {db: asn, path: autonomous_system_number, default: "0"}
`

func TestLoadValid(t *testing.T) {
	cfg, err := Load(write(t, valid))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen.GRPC != ":9000" || cfg.Listen.Admin != ":8080" {
		t.Errorf("defaults not applied: %+v", cfg.Listen)
	}
	if !cfg.OverwriteEnabled() {
		t.Error("overwrite must default to true")
	}
	if cfg.IPSources[0].Header != "x-real-ip" {
		t.Errorf("ip source header not lowercased: %q", cfg.IPSources[0].Header)
	}
	city := cfg.Databases["city"]
	if time.Duration(city.CheckInterval) != time.Hour || !city.Required {
		t.Errorf("city db parsed wrong: %+v", city)
	}
	if time.Duration(cfg.Databases["asn"].CheckInterval) != 6*time.Hour {
		t.Error("check_interval default 6h not applied")
	}
	h, ok := cfg.Headers["x-geoip-country-code"]
	if !ok || h.DB != "city" || h.Path != "country.iso_code" {
		t.Errorf("header key not normalized/parsed: %+v", cfg.Headers)
	}
	if d := cfg.Headers["x-geoip-asn"].Default; d == nil || *d != "0" {
		t.Error("default not parsed")
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string]string{
		"unknown field":   valid + "\nbogus: 1\n",
		"missing db ref":  "ip_sources: [{header: x}]\ndatabases: {city: {source: \"https://e/x\"}}\nheaders: {h: {db: nope, path: p}}",
		"both ip fields":  "ip_sources: [{header: x, envoy: source_address}]\ndatabases: {c: {source: \"https://e/x\"}}\nheaders: {h: {db: c, path: p}}",
		"bad envoy source": "ip_sources: [{envoy: wat}]\ndatabases: {c: {source: \"https://e/x\"}}\nheaders: {h: {db: c, path: p}}",
		"bad scheme":      "ip_sources: [{header: x}]\ndatabases: {c: {source: \"ftp://e/x\"}}\nheaders: {h: {db: c, path: p}}",
		"empty path":      "ip_sources: [{header: x}]\ndatabases: {c: {source: \"https://e/x\"}}\nheaders: {h: {db: c, path: \"\"}}",
		"no ip sources":   "databases: {c: {source: \"https://e/x\"}}\nheaders: {h: {db: c, path: p}}",
		"bad duration":    "ip_sources: [{header: x}]\ndatabases: {c: {source: \"https://e/x\", check_interval: soon}}\nheaders: {h: {db: c, path: p}}",
	}
	for name, body := range cases {
		if _, err := Load(write(t, body)); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}
