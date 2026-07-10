# envoy-geoip-processor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Go ext_proc gRPC-сервис для envoy-gateway: определяет геоданные клиента по MaxMind-базам и ставит настраиваемые хедеры; базы автоскачиваются (HTTP/S3) и периодически обновляются.

**Architecture:** Один бинарь, четыре слоя: `internal/config` (YAML), `internal/geodb` (fetch HTTP/S3, менеджер баз с atomic swap, lookup по пути), `internal/ipsrc` (выбор IP из цепочки источников), `internal/extproc` + `internal/admin` (gRPC ext_proc и healthz/readyz/metrics). Спека: `docs/superpowers/specs/2026-07-10-geoip-ext-proc-design.md`.

**Tech Stack:** Go 1.26, `github.com/oschwald/maxminddb-golang/v2` (v2.4+), `github.com/envoyproxy/go-control-plane/envoy`, `google.golang.org/grpc`, `github.com/aws/aws-sdk-go-v2`, `github.com/prometheus/client_golang`, `gopkg.in/yaml.v3`.

## Global Constraints

- Module path: `envoy-geoip-processor` (уже в go.mod), Go `1.26`.
- Все тесты гоняются `go test -race ./...` — каждый Task заканчивается зелёным прогоном.
- Хедеры и имена баз в конфиге нормализуются в lowercase; порядок выставления хедеров детерминированный (сортировка по имени).
- Fail-open в рантайме: ошибка lookup никогда не роняет запрос. Ready — только когда все `required` базы загружены.
- Тестовые mmdb: официальные из `github.com/maxmind/MaxMind-DB` (закоммичены в `testdata/`). Опорные факты: в `GeoIP2-City-Test.mmdb` IP `2.125.160.216` → GB / ENG / Boxford / postal OX1 / lat 51.75 / lon -1.25 / Europe/London; в `GeoLite2-ASN-Test.mmdb` IP `1.128.0.0` → ASN 1221 / "Telstra Pty Ltd".
- Коммит после каждого таска (сообщения указаны в шагах).

## File Structure

```
cmd/geoip-processor/main.go        — wiring, graceful shutdown
internal/config/config.go          — типы конфига, Load, валидация, дефолты
internal/geodb/lookup.go           — ParsePath, FormatValue, LookupPath
internal/geodb/fetch.go            — Fetcher interface, Meta, writeMMDB (gzip/tar)
internal/geodb/httpfetch.go        — HTTP conditional fetch
internal/geodb/s3fetch.go          — S3 fetch (HeadObject ETag)
internal/geodb/manager.go          — Manager: cache, update loop, atomic swap, Ready
internal/ipsrc/resolver.go         — выбор IP из цепочки
internal/extproc/server.go         — ext_proc Processor
internal/admin/admin.go            — healthz/readyz/metrics
testdata/*.mmdb                    — тестовые базы MaxMind
examples/config.yaml               — пример конфига (прод)
deploy/compose/{docker-compose.yaml,envoy.yaml,config.yaml}
Dockerfile, .gitignore, README.md
charts/geoip-processor/...         — Helm chart + EnvoyExtensionPolicy
```

---

### Task 1: Config package

**Files:**
- Create: `internal/config/config.go`
- Test: `internal/config/config_test.go`
- Modify: `go.mod` (deps)

**Interfaces:**
- Produces: `config.Load(path string) (*Config, error)`; типы `Config{Listen, CacheDir string, IPSources []IPSource, Overwrite *bool, Databases map[string]Database, Headers map[string]HeaderRule}`, `Listen{GRPC, Admin string}`, `IPSource{Header, Envoy string}`, `Database{Source string, Auth Auth{BasicEnv string}, CheckInterval Duration, Required bool}`, `HeaderRule{DB, Path string, Default *string}`, `Duration` (обёртка `time.Duration` с `UnmarshalYAML`), метод `(*Config).OverwriteEnabled() bool`.

- [ ] **Step 1: deps**

```bash
go get gopkg.in/yaml.v3@latest
```

- [ ] **Step 2: Write the failing test**

`internal/config/config_test.go`:

```go
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
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/config/`
Expected: FAIL (compile error: `Load` undefined)

- [ ] **Step 4: Implement**

`internal/config/config.go`:

```go
// Package config loads and validates the processor YAML configuration.
package config

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration is a time.Duration that unmarshals from YAML strings like "6h".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

type Listen struct {
	GRPC  string `yaml:"grpc"`
	Admin string `yaml:"admin"`
}

// IPSource is one element of the client-IP resolution chain.
// Exactly one of Header or Envoy is set. Envoy currently supports
// only "source_address" (downstream address from ext_proc attributes).
type IPSource struct {
	Header string `yaml:"header"`
	Envoy  string `yaml:"envoy"`
}

type Auth struct {
	// BasicEnv names an env var holding "user:password" for HTTP basic auth.
	BasicEnv string `yaml:"basic_env"`
}

type Database struct {
	Source        string   `yaml:"source"` // https://... | http://... | s3://bucket/key
	Auth          Auth     `yaml:"auth"`
	CheckInterval Duration `yaml:"check_interval"`
	Required      bool     `yaml:"required"` // gates /readyz
}

type HeaderRule struct {
	DB      string  `yaml:"db"`
	Path    string  `yaml:"path"` // dot-separated, ints are array indices
	Default *string `yaml:"default"`
}

type Config struct {
	Listen    Listen                `yaml:"listen"`
	CacheDir  string                `yaml:"cache_dir"`
	IPSources []IPSource            `yaml:"ip_sources"`
	Overwrite *bool                 `yaml:"overwrite"`
	Databases map[string]Database   `yaml:"databases"`
	Headers   map[string]HeaderRule `yaml:"headers"`
}

// OverwriteEnabled reports whether geoip headers replace client-sent ones
// (default true; false keeps a client header if present).
func (c *Config) OverwriteEnabled() bool { return c.Overwrite == nil || *c.Overwrite }

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Listen.GRPC == "" {
		c.Listen.GRPC = ":9000"
	}
	if c.Listen.Admin == "" {
		c.Listen.Admin = ":8080"
	}
	if c.CacheDir == "" {
		c.CacheDir = "/var/cache/geoip"
	}
	for i, s := range c.IPSources {
		c.IPSources[i].Header = strings.ToLower(s.Header)
	}
	for name, db := range c.Databases {
		if db.CheckInterval == 0 {
			db.CheckInterval = Duration(6 * time.Hour)
			c.Databases[name] = db
		}
	}
	normalized := make(map[string]HeaderRule, len(c.Headers))
	for name, rule := range c.Headers {
		normalized[strings.ToLower(name)] = rule
	}
	c.Headers = normalized
}

func (c *Config) validate() error {
	if len(c.IPSources) == 0 {
		return fmt.Errorf("ip_sources must not be empty")
	}
	for i, s := range c.IPSources {
		switch {
		case s.Header != "" && s.Envoy != "":
			return fmt.Errorf("ip_sources[%d]: set either header or envoy, not both", i)
		case s.Header == "" && s.Envoy == "":
			return fmt.Errorf("ip_sources[%d]: set header or envoy", i)
		case s.Envoy != "" && s.Envoy != "source_address":
			return fmt.Errorf("ip_sources[%d]: unsupported envoy source %q (only source_address)", i, s.Envoy)
		}
	}
	if len(c.Databases) == 0 {
		return fmt.Errorf("databases must not be empty")
	}
	for name, db := range c.Databases {
		u, err := url.Parse(db.Source)
		if err != nil {
			return fmt.Errorf("database %s: bad source: %w", name, err)
		}
		switch u.Scheme {
		case "http", "https", "s3":
		default:
			return fmt.Errorf("database %s: unsupported source scheme %q", name, u.Scheme)
		}
		if db.CheckInterval <= 0 {
			return fmt.Errorf("database %s: check_interval must be > 0", name)
		}
	}
	if len(c.Headers) == 0 {
		return fmt.Errorf("headers must not be empty")
	}
	for name, rule := range c.Headers {
		if _, ok := c.Databases[rule.DB]; !ok {
			return fmt.Errorf("header %s: unknown db %q", name, rule.DB)
		}
		if rule.Path == "" {
			return fmt.Errorf("header %s: path must not be empty", name)
		}
	}
	return nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/config/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum internal/config
git commit -m "feat: config package with validation and defaults"
```

---

### Task 2: Test databases + path lookup

**Files:**
- Create: `testdata/GeoIP2-City-Test.mmdb`, `testdata/GeoLite2-ASN-Test.mmdb` (скачать)
- Create: `internal/geodb/lookup.go`
- Test: `internal/geodb/lookup_test.go`

**Interfaces:**
- Produces: `geodb.ParsePath(s string) ([]any, error)`; `geodb.FormatValue(v any) (string, error)`; `geodb.LookupPath(r *maxminddb.Reader, ip netip.Addr, path []any) (val string, found bool, err error)`.

- [ ] **Step 1: deps + testdata**

```bash
go get github.com/oschwald/maxminddb-golang/v2@latest
mkdir -p testdata
curl -fsSL -o testdata/GeoIP2-City-Test.mmdb  https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoIP2-City-Test.mmdb
curl -fsSL -o testdata/GeoLite2-ASN-Test.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-ASN-Test.mmdb
```

- [ ] **Step 2: Write the failing test**

`internal/geodb/lookup_test.go`:

```go
package geodb

import (
	"net/netip"
	"reflect"
	"testing"

	"github.com/oschwald/maxminddb-golang/v2"
)

func TestParsePath(t *testing.T) {
	got, err := ParsePath("subdivisions.0.iso_code")
	if err != nil {
		t.Fatal(err)
	}
	want := []any{"subdivisions", 0, "iso_code"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
	for _, bad := range []string{"", "a..b"} {
		if _, err := ParsePath(bad); err == nil {
			t.Errorf("ParsePath(%q): expected error", bad)
		}
	}
}

func openCity(t *testing.T) *maxminddb.Reader {
	t.Helper()
	r, err := maxminddb.Open("../../testdata/GeoIP2-City-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestLookupPath(t *testing.T) {
	city := openCity(t)
	ip := netip.MustParseAddr("2.125.160.216")
	cases := []struct{ path, want string }{
		{"country.iso_code", "GB"},
		{"subdivisions.0.iso_code", "ENG"},
		{"city.names.en", "Boxford"},
		{"postal.code", "OX1"},
		{"location.time_zone", "Europe/London"},
		{"location.latitude", "51.75"},
		{"location.longitude", "-1.25"},
	}
	for _, c := range cases {
		path, err := ParsePath(c.path)
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := LookupPath(city, ip, path)
		if err != nil || !found || got != c.want {
			t.Errorf("%s: got (%q, %v, %v), want (%q, true, nil)", c.path, got, found, err, c.want)
		}
	}
}

func TestLookupPathASN(t *testing.T) {
	r, err := maxminddb.Open("../../testdata/GeoLite2-ASN-Test.mmdb")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ip := netip.MustParseAddr("1.128.0.0")
	path, _ := ParsePath("autonomous_system_number")
	got, found, err := LookupPath(r, ip, path)
	if err != nil || !found || got != "1221" {
		t.Errorf("asn: got (%q, %v, %v)", got, found, err)
	}
	path, _ = ParsePath("autonomous_system_organization")
	got, found, _ = LookupPath(r, ip, path)
	if !found || got != "Telstra Pty Ltd" {
		t.Errorf("org: got (%q, %v)", got, found)
	}
}

func TestLookupPathMisses(t *testing.T) {
	city := openCity(t)
	// IP not in the test database at all.
	path, _ := ParsePath("country.iso_code")
	if _, found, err := LookupPath(city, netip.MustParseAddr("203.0.113.5"), path); found || err != nil {
		t.Errorf("unknown ip: found=%v err=%v, want false, nil", found, err)
	}
	// Existing IP, path absent in the record.
	path, _ = ParsePath("traits.nope")
	if _, found, _ := LookupPath(city, netip.MustParseAddr("2.125.160.216"), path); found {
		t.Error("absent path: want found=false")
	}
	// Path pointing at a composite value must error, not emit garbage.
	path, _ = ParsePath("location")
	if _, _, err := LookupPath(city, netip.MustParseAddr("2.125.160.216"), path); err == nil {
		t.Error("composite value: want error")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/geodb/`
Expected: FAIL (compile error: `ParsePath` undefined)

- [ ] **Step 4: Implement**

`internal/geodb/lookup.go`:

```go
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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/geodb/`
Expected: PASS. Контингенси: если `TestLookupPathMisses` падает на кейсе «absent path» потому, что `DecodePath` возвращает ошибку (а не оставляет значение nil) — ослабь этот кейс теста: проверяй только `found == false`, без проверки `err`. Реализацию `LookupPath` не меняй; допиши в тесте комментарий, какое поведение библиотеки зафиксировано.

- [ ] **Step 6: Commit**

```bash
git add testdata internal/geodb go.mod go.sum
git commit -m "feat: mmdb path lookup with value formatting"
```

---

### Task 3: HTTP fetcher (conditional download, tar.gz)

**Files:**
- Create: `internal/geodb/fetch.go`, `internal/geodb/httpfetch.go`
- Test: `internal/geodb/httpfetch_test.go`

**Interfaces:**
- Produces: `geodb.Meta{ETag, LastModified string}`; `geodb.Fetcher interface { Fetch(ctx context.Context, dst string, prev Meta) (changed bool, next Meta, err error) }`; `geodb.HTTPFetcher{URL, BasicEnv string, Client *http.Client}` implementing `Fetcher`; internal helper `writeMMDB(r io.Reader, dst string) error` (распаковка gzip+tar → первый `*.mmdb`, иначе сырая копия).

- [ ] **Step 1: Write the failing test**

`internal/geodb/httpfetch_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/geodb/`
Expected: FAIL (compile error: `HTTPFetcher` undefined)

- [ ] **Step 3: Implement**

`internal/geodb/fetch.go`:

```go
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
```

`internal/geodb/httpfetch.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/geodb/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/geodb
git commit -m "feat: HTTP fetcher with conditional download and tar.gz extraction"
```

---

### Task 4: S3 fetcher

**Files:**
- Create: `internal/geodb/s3fetch.go`
- Test: `internal/geodb/s3fetch_test.go`
- Modify: `go.mod` (aws-sdk-go-v2)

**Interfaces:**
- Consumes: `Meta`, `Fetcher`, `writeMMDB` из Task 3.
- Produces: `geodb.S3Fetcher{Client S3API, Bucket, Key string}` implementing `Fetcher`; `geodb.S3API interface { HeadObject(...); GetObject(...) }` (сигнатуры aws-sdk-go-v2 s3.Client); `geodb.NewS3Fetcher(ctx context.Context, rawURL string) (*S3Fetcher, error)` — парсит `s3://bucket/key`, креды из стандартной AWS-цепочки.

- [ ] **Step 1: deps**

```bash
go get github.com/aws/aws-sdk-go-v2/config@latest github.com/aws/aws-sdk-go-v2/service/s3@latest
```

- [ ] **Step 2: Write the failing test**

`internal/geodb/s3fetch_test.go`:

```go
package geodb

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type fakeS3 struct {
	etag string
	body []byte
	gets int
}

func (f *fakeS3) HeadObject(ctx context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return &s3.HeadObjectOutput{ETag: aws.String(f.etag)}, nil
}

func (f *fakeS3) GetObject(ctx context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.gets++
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.body)), ETag: aws.String(f.etag)}, nil
}

func TestS3Fetcher(t *testing.T) {
	db := mmdbBytes(t)
	fake := &fakeS3{etag: `"abc"`, body: db}
	f := &S3Fetcher{Client: fake, Bucket: "b", Key: "city.mmdb"}
	dst := filepath.Join(t.TempDir(), "city.mmdb")

	changed, meta, err := f.Fetch(context.Background(), dst, Meta{})
	if err != nil || !changed || meta.ETag != `"abc"` {
		t.Fatalf("first: changed=%v meta=%+v err=%v", changed, meta, err)
	}
	if got, _ := os.ReadFile(dst); !bytes.Equal(got, db) {
		t.Error("downloaded file differs")
	}

	changed, _, err = f.Fetch(context.Background(), dst, meta)
	if err != nil || changed || fake.gets != 1 {
		t.Fatalf("second: changed=%v gets=%d err=%v, want unchanged with no extra GET", changed, fake.gets, err)
	}
}

func TestParseS3URL(t *testing.T) {
	bucket, key, err := parseS3URL("s3://my-bucket/path/to/asn.mmdb")
	if err != nil || bucket != "my-bucket" || key != "path/to/asn.mmdb" {
		t.Errorf("got %q %q %v", bucket, key, err)
	}
	if _, _, err := parseS3URL("s3:///nokey"); err == nil {
		t.Error("expected error for missing bucket")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/geodb/`
Expected: FAIL (compile error: `S3Fetcher` undefined)

- [ ] **Step 4: Implement**

`internal/geodb/s3fetch.go`:

```go
package geodb

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3API is the subset of the S3 client used by S3Fetcher.
type S3API interface {
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3Fetcher downloads a database from S3, skipping unchanged ETags.
type S3Fetcher struct {
	Client S3API
	Bucket string
	Key    string
}

func parseS3URL(raw string) (bucket, key string, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", err
	}
	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")
	if u.Scheme != "s3" || bucket == "" || key == "" {
		return "", "", fmt.Errorf("source must look like s3://bucket/key, got %q", raw)
	}
	return bucket, key, nil
}

// NewS3Fetcher builds a fetcher for an s3://bucket/key URL using the
// default AWS credential chain (env, IRSA, shared config).
func NewS3Fetcher(ctx context.Context, rawURL string) (*S3Fetcher, error) {
	bucket, key, err := parseS3URL(rawURL)
	if err != nil {
		return nil, err
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &S3Fetcher{Client: s3.NewFromConfig(cfg), Bucket: bucket, Key: key}, nil
}

func (f *S3Fetcher) Fetch(ctx context.Context, dst string, prev Meta) (bool, Meta, error) {
	head, err := f.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &f.Bucket, Key: &f.Key})
	if err != nil {
		return false, prev, fmt.Errorf("head s3://%s/%s: %w", f.Bucket, f.Key, err)
	}
	if etag := aws.ToString(head.ETag); etag != "" && etag == prev.ETag {
		return false, prev, nil
	}
	obj, err := f.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: &f.Bucket, Key: &f.Key})
	if err != nil {
		return false, prev, fmt.Errorf("get s3://%s/%s: %w", f.Bucket, f.Key, err)
	}
	defer obj.Body.Close()
	if err := writeMMDB(obj.Body, dst); err != nil {
		return false, prev, err
	}
	return true, Meta{ETag: aws.ToString(obj.ETag)}, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/geodb/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/geodb go.mod go.sum
git commit -m "feat: S3 fetcher with ETag-based change detection"
```

---

### Task 5: DB Manager (cache, update loop, atomic swap)

**Files:**
- Create: `internal/geodb/manager.go`
- Test: `internal/geodb/manager_test.go`
- Modify: `go.mod` (prometheus)

**Interfaces:**
- Consumes: `config.Config`, `Fetcher`, `Meta`, `LookupPath`.
- Produces: `geodb.NewManager(cfg *config.Config, fetchers map[string]Fetcher, logger *slog.Logger, reg prometheus.Registerer) (*Manager, error)`; `(*Manager).LoadCache()`; `(*Manager).Run(ctx context.Context)` (блокирующий, циклы обновления, немедленная первая проверка); `(*Manager).Ready() bool`; `(*Manager).Lookup(db string, ip netip.Addr, path []any) (string, bool, error)`.

- [ ] **Step 1: deps**

```bash
go get github.com/prometheus/client_golang@latest
```

- [ ] **Step 2: Write the failing test**

`internal/geodb/manager_test.go`:

```go
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
	m.checkAll(context.Background())
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
	m.checkAll(context.Background())
	m.checkAll(context.Background()) // корявый файл — reader должен остаться старым
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
	m.checkAll(context.Background())
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
		m.checkAll(context.Background())
	}
	wg.Wait()
}

func TestManagerUnknownDB(t *testing.T) {
	m := newTestManager(t, t.TempDir(), &scriptedFetcher{})
	if _, _, err := m.Lookup("nope", netip.MustParseAddr("1.1.1.1"), []any{"x"}); err == nil {
		t.Error("expected error for unknown db")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/geodb/`
Expected: FAIL (compile error: `NewManager` undefined)

- [ ] **Step 4: Implement**

`internal/geodb/manager.go`:

```go
package geodb

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/netip"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/prometheus/client_golang/prometheus"

	"envoy-geoip-processor/internal/config"
)

// closeGrace is how long a replaced reader stays open so that in-flight
// lookups holding the old pointer can finish (lookups are microseconds).
const closeGrace = time.Minute

type dbState struct {
	name     string
	required bool
	interval time.Duration
	fetcher  Fetcher
	reader   atomic.Pointer[maxminddb.Reader]
	meta     Meta // touched only by the update goroutine / checkAll
}

// Manager owns all configured databases: disk cache, refresh, lookups.
type Manager struct {
	dbs      map[string]*dbState
	cacheDir string
	logger   *slog.Logger
	updates  *prometheus.CounterVec
	loadedAt *prometheus.GaugeVec
}

func NewManager(cfg *config.Config, fetchers map[string]Fetcher, logger *slog.Logger, reg prometheus.Registerer) (*Manager, error) {
	if err := os.MkdirAll(cfg.CacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("create cache dir: %w", err)
	}
	m := &Manager{
		dbs:      map[string]*dbState{},
		cacheDir: cfg.CacheDir,
		logger:   logger,
		updates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "geoip_db_update_total",
			Help: "Database update attempts by result (updated|unchanged|invalid|error).",
		}, []string{"db", "result"}),
		loadedAt: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "geoip_db_loaded_timestamp_seconds",
			Help: "Unix time the database reader was last (re)loaded.",
		}, []string{"db"}),
	}
	reg.MustRegister(m.updates, m.loadedAt)
	for name, db := range cfg.Databases {
		f, ok := fetchers[name]
		if !ok {
			return nil, fmt.Errorf("no fetcher for database %q", name)
		}
		m.dbs[name] = &dbState{
			name:     name,
			required: db.Required,
			interval: time.Duration(db.CheckInterval),
			fetcher:  f,
		}
	}
	return m, nil
}

func (m *Manager) dbPath(name string) string   { return filepath.Join(m.cacheDir, name+".mmdb") }
func (m *Manager) metaPath(name string) string { return filepath.Join(m.cacheDir, name+".meta.json") }

// LoadCache opens databases already present in the cache dir, so a restart
// is ready immediately and the first network check happens in background.
func (m *Manager) LoadCache() {
	for name, s := range m.dbs {
		r, err := maxminddb.Open(m.dbPath(name))
		if err != nil {
			continue
		}
		s.reader.Store(r)
		m.loadedAt.WithLabelValues(name).SetToCurrentTime()
		if raw, err := os.ReadFile(m.metaPath(name)); err == nil {
			json.Unmarshal(raw, &s.meta)
		}
		m.logger.Info("loaded database from cache", "db", name)
	}
}

// Run blocks: checks every database immediately, then on its interval
// (with ±10% jitter) until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	done := make(chan struct{})
	for _, s := range m.dbs {
		go func(s *dbState) {
			defer func() { done <- struct{}{} }()
			m.checkOne(ctx, s)
			for {
				jitter := time.Duration((rand.Float64()*0.2 - 0.1) * float64(s.interval))
				select {
				case <-ctx.Done():
					return
				case <-time.After(s.interval + jitter):
					m.checkOne(ctx, s)
				}
			}
		}(s)
	}
	for range m.dbs {
		<-done
	}
}

// checkAll runs one synchronous check for every database (used by tests
// and the initial foreground load).
func (m *Manager) checkAll(ctx context.Context) {
	for _, s := range m.dbs {
		m.checkOne(ctx, s)
	}
}

func (m *Manager) checkOne(ctx context.Context, s *dbState) {
	tmp := m.dbPath(s.name) + ".tmp"
	changed, next, err := s.fetcher.Fetch(ctx, tmp, s.meta)
	if err != nil {
		m.updates.WithLabelValues(s.name, "error").Inc()
		m.logger.Error("database check failed", "db", s.name, "err", err)
		return
	}
	if !changed {
		m.updates.WithLabelValues(s.name, "unchanged").Inc()
		return
	}
	r, err := maxminddb.Open(tmp)
	if err != nil {
		os.Remove(tmp)
		m.updates.WithLabelValues(s.name, "invalid").Inc()
		m.logger.Error("downloaded database is invalid", "db", s.name, "err", err)
		return
	}
	if err := os.Rename(tmp, m.dbPath(s.name)); err != nil {
		r.Close()
		m.updates.WithLabelValues(s.name, "error").Inc()
		m.logger.Error("rename failed", "db", s.name, "err", err)
		return
	}
	s.meta = next
	if raw, err := json.Marshal(next); err == nil {
		os.WriteFile(m.metaPath(s.name), raw, 0o644)
	}
	if old := s.reader.Swap(r); old != nil {
		time.AfterFunc(closeGrace, func() { old.Close() })
	}
	m.updates.WithLabelValues(s.name, "updated").Inc()
	m.loadedAt.WithLabelValues(s.name).SetToCurrentTime()
	m.logger.Info("database updated", "db", s.name, "etag", next.ETag)
}

// Ready reports whether every required database has a loaded reader.
func (m *Manager) Ready() bool {
	for _, s := range m.dbs {
		if s.required && s.reader.Load() == nil {
			return false
		}
	}
	return true
}

// Lookup resolves path for ip in the named database.
func (m *Manager) Lookup(db string, ip netip.Addr, path []any) (string, bool, error) {
	s, ok := m.dbs[db]
	if !ok {
		return "", false, fmt.Errorf("unknown database %q", db)
	}
	r := s.reader.Load()
	if r == nil {
		return "", false, fmt.Errorf("database %q not loaded yet", db)
	}
	return LookupPath(r, ip, path)
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -race ./internal/geodb/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/geodb go.mod go.sum
git commit -m "feat: database manager with cache, refresh loop and atomic swap"
```

---

### Task 6: IP resolver

**Files:**
- Create: `internal/ipsrc/resolver.go`
- Test: `internal/ipsrc/resolver_test.go`

**Interfaces:**
- Consumes: `config.IPSource`.
- Produces: `ipsrc.New(sources []config.IPSource) *Resolver`; `(*Resolver).Resolve(headers map[string]string, envoySourceAddr string) (netip.Addr, bool)` — ключи headers обязаны быть lowercase; значение с запятыми трактуется как список, берётся первый элемент; `ip:port` и `[v6]:port` парсятся, IPv4-mapped адреса анмапятся.

- [ ] **Step 1: Write the failing test**

`internal/ipsrc/resolver_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/ipsrc/`
Expected: FAIL (package does not exist)

- [ ] **Step 3: Implement**

`internal/ipsrc/resolver.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/ipsrc/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ipsrc
git commit -m "feat: client IP resolver with ordered source chain"
```

---

### Task 7: ext_proc gRPC server

**Files:**
- Create: `internal/extproc/server.go`
- Test: `internal/extproc/server_test.go`
- Modify: `go.mod` (go-control-plane, grpc)

**Interfaces:**
- Consumes: `geodb.Manager.Lookup`, `geodb.ParsePath`, `ipsrc.Resolver.Resolve`, `config.Config`.
- Produces: `extproc.New(cfg *config.Config, mgr *geodb.Manager, resolver *ipsrc.Resolver, logger *slog.Logger, reg prometheus.Registerer) (*Processor, error)`; `Processor` реализует `extprocv3.ExternalProcessorServer` (метод `Process(stream) error`). Поведение: обрабатываются только request headers; при overwrite=true несработавшие хедеры добавляются в `RemoveHeaders` (антиспуфинг); при overwrite=false — `APPEND_ACTION_ADD_IF_ABSENT`.

- [ ] **Step 1: deps**

```bash
go get github.com/envoyproxy/go-control-plane/envoy@latest google.golang.org/grpc@latest google.golang.org/protobuf@latest
```

- [ ] **Step 2: Write the failing test**

`internal/extproc/server_test.go`:

```go
package extproc

import (
	"context"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	"envoy-geoip-processor/internal/config"
	"envoy-geoip-processor/internal/geodb"
	"envoy-geoip-processor/internal/ipsrc"
)

type localFetcher struct{ src string }

func (l *localFetcher) Fetch(_ context.Context, dst string, prev geodb.Meta) (bool, geodb.Meta, error) {
	b, err := os.ReadFile(l.src)
	if err != nil {
		return false, prev, err
	}
	return true, geodb.Meta{}, os.WriteFile(dst, b, 0o644)
}

func testConfig(dir string) *config.Config {
	defaultCC := "XX"
	return &config.Config{
		CacheDir: dir,
		IPSources: []config.IPSource{
			{Header: "x-real-ip"},
			{Envoy: "source_address"},
		},
		Databases: map[string]config.Database{
			"city": {Source: "https://e/x", CheckInterval: config.Duration(time.Hour), Required: true},
		},
		Headers: map[string]config.HeaderRule{
			"x-geoip-country-code": {DB: "city", Path: "country.iso_code", Default: &defaultCC},
			"x-geoip-city":         {DB: "city", Path: "city.names.en"},
		},
	}
}

func startProcessor(t *testing.T, cfg *config.Config) extprocv3.ExternalProcessorClient {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	reg := prometheus.NewRegistry()
	mgr, err := geodb.NewManager(cfg, map[string]geodb.Fetcher{
		"city": &localFetcher{src: "../../testdata/GeoIP2-City-Test.mmdb"},
	}, logger, reg)
	if err != nil {
		t.Fatal(err)
	}
	mgr.CheckNow(context.Background())
	p, err := New(cfg, mgr, ipsrc.New(cfg.IPSources), logger, reg)
	if err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(srv, p)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return extprocv3.NewExternalProcessorClient(conn)
}

func headersReq(headers map[string]string) *extprocv3.ProcessingRequest {
	hm := &corev3.HeaderMap{}
	for k, v := range headers {
		hm.Headers = append(hm.Headers, &corev3.HeaderValue{Key: k, RawValue: []byte(v)})
	}
	return &extprocv3.ProcessingRequest{
		Request: &extprocv3.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extprocv3.HttpHeaders{Headers: hm},
		},
	}
}

func roundTrip(t *testing.T, client extprocv3.ExternalProcessorClient, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	stream, err := client.Process(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(req); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func setHeaders(resp *extprocv3.ProcessingResponse) map[string]string {
	out := map[string]string{}
	mut := resp.GetRequestHeaders().GetResponse().GetHeaderMutation()
	for _, h := range mut.GetSetHeaders() {
		out[h.GetHeader().GetKey()] = string(h.GetHeader().GetRawValue())
	}
	return out
}

func TestProcessSetsGeoHeaders(t *testing.T) {
	client := startProcessor(t, testConfig(t.TempDir()))
	resp := roundTrip(t, client, headersReq(map[string]string{"x-real-ip": "2.125.160.216"}))
	got := setHeaders(resp)
	if got["x-geoip-country-code"] != "GB" || got["x-geoip-city"] != "Boxford" {
		t.Errorf("headers: %v", got)
	}
}

func TestProcessDefaultAndRemove(t *testing.T) {
	client := startProcessor(t, testConfig(t.TempDir()))
	// IP не из базы: country получает default, city не ставится и попадает в remove (антиспуфинг).
	resp := roundTrip(t, client, headersReq(map[string]string{
		"x-real-ip":    "203.0.113.5",
		"x-geoip-city": "Spoofed",
	}))
	got := setHeaders(resp)
	if got["x-geoip-country-code"] != "XX" {
		t.Errorf("default not applied: %v", got)
	}
	if _, ok := got["x-geoip-city"]; ok {
		t.Error("x-geoip-city must not be set for unknown ip")
	}
	mut := resp.GetRequestHeaders().GetResponse().GetHeaderMutation()
	removed := false
	for _, h := range mut.GetRemoveHeaders() {
		if h == "x-geoip-city" {
			removed = true
		}
	}
	if !removed {
		t.Errorf("spoofed x-geoip-city must be removed, mutation: %v", mut)
	}
}

func TestProcessEnvoySourceAddress(t *testing.T) {
	client := startProcessor(t, testConfig(t.TempDir()))
	req := headersReq(map[string]string{})
	attrs, _ := structpb.NewStruct(map[string]any{"source.address": "2.125.160.216:55555"})
	req.Attributes = map[string]*structpb.Struct{"envoy.filters.http.ext_proc": attrs}
	got := setHeaders(roundTrip(t, client, req))
	if got["x-geoip-country-code"] != "GB" {
		t.Errorf("envoy attribute source failed: %v", got)
	}
}

func TestProcessNoIPFailsOpen(t *testing.T) {
	client := startProcessor(t, testConfig(t.TempDir()))
	resp := roundTrip(t, client, headersReq(map[string]string{}))
	if resp.GetRequestHeaders() == nil {
		t.Fatal("must still answer with a RequestHeaders response")
	}
	got := setHeaders(resp)
	// Без IP значений нет, но default всё равно применяется.
	if got["x-geoip-country-code"] != "XX" {
		t.Errorf("default must apply without ip: %v", got)
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/extproc/`
Expected: FAIL (compile error: `New` undefined; также `mgr.CheckNow` не существует)

- [ ] **Step 4: Add Manager.CheckNow**

Тест использует публичный `CheckNow` вместо приватного `checkAll`. В `internal/geodb/manager.go` переименуй `checkAll` в `CheckNow`:

```go
// CheckNow runs one synchronous update check for every database.
// Used at startup (foreground initial load) and in tests.
func (m *Manager) CheckNow(ctx context.Context) {
	for _, s := range m.dbs {
		m.checkOne(ctx, s)
	}
}
```

Обнови вызовы в `manager_test.go` (`m.checkAll` → `m.CheckNow`). Прогони `go test ./internal/geodb/` — PASS.

- [ ] **Step 5: Implement processor**

`internal/extproc/server.go`:

```go
// Package extproc implements the Envoy external processor that injects
// geoip headers into requests.
package extproc

import (
	"errors"
	"io"
	"log/slog"
	"sort"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"

	"envoy-geoip-processor/internal/config"
	"envoy-geoip-processor/internal/geodb"
	"envoy-geoip-processor/internal/ipsrc"
)

type headerRule struct {
	name string
	db   string
	path []any
	def  *string
}

type Processor struct {
	extprocv3.UnimplementedExternalProcessorServer

	mgr       *geodb.Manager
	resolver  *ipsrc.Resolver
	rules     []headerRule
	overwrite bool
	logger    *slog.Logger
	lookups   *prometheus.CounterVec
	requests  *prometheus.CounterVec
}

func New(cfg *config.Config, mgr *geodb.Manager, resolver *ipsrc.Resolver, logger *slog.Logger, reg prometheus.Registerer) (*Processor, error) {
	p := &Processor{
		mgr:       mgr,
		resolver:  resolver,
		overwrite: cfg.OverwriteEnabled(),
		logger:    logger,
		lookups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "geoip_lookups_total",
			Help: "Per-header lookups by result (hit|miss|error).",
		}, []string{"db", "result"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "geoip_requests_total",
			Help: "Processed request-header messages by ip resolution result.",
		}, []string{"ip"}),
	}
	reg.MustRegister(p.lookups, p.requests)
	for name, rule := range cfg.Headers {
		path, err := geodb.ParsePath(rule.Path)
		if err != nil {
			return nil, err
		}
		p.rules = append(p.rules, headerRule{name: name, db: rule.DB, path: path, def: rule.Default})
	}
	sort.Slice(p.rules, func(i, j int) bool { return p.rules[i].name < p.rules[j].name })
	return p, nil
}

func (p *Processor) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		var resp *extprocv3.ProcessingResponse
		switch req.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{
					HeaderMutation: p.mutate(req),
				}},
			}}
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extprocv3.HeadersResponse{},
			}}
		case *extprocv3.ProcessingRequest_RequestBody:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{
				RequestBody: &extprocv3.BodyResponse{},
			}}
		case *extprocv3.ProcessingRequest_ResponseBody:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{
				ResponseBody: &extprocv3.BodyResponse{},
			}}
		case *extprocv3.ProcessingRequest_RequestTrailers:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{
				RequestTrailers: &extprocv3.TrailersResponse{},
			}}
		case *extprocv3.ProcessingRequest_ResponseTrailers:
			resp = &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{
				ResponseTrailers: &extprocv3.TrailersResponse{},
			}}
		default:
			continue
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// mutate builds the header mutation for one request. It never fails:
// on any problem headers are simply omitted (fail-open).
func (p *Processor) mutate(req *extprocv3.ProcessingRequest) *extprocv3.HeaderMutation {
	headers := map[string]string{}
	rh := req.GetRequestHeaders()
	for _, h := range rh.GetHeaders().GetHeaders() {
		v := string(h.GetRawValue())
		if v == "" {
			v = h.GetValue()
		}
		headers[strings.ToLower(h.GetKey())] = v
	}

	ip, ipOK := p.resolver.Resolve(headers, envoySourceAddress(req))
	if ipOK {
		p.requests.WithLabelValues("found").Inc()
	} else {
		p.requests.WithLabelValues("not_found").Inc()
	}

	appendAction := corev3.HeaderValueOption_OVERWRITE_IF_EXISTS_OR_ADD
	if !p.overwrite {
		appendAction = corev3.HeaderValueOption_ADD_IF_ABSENT
	}

	mut := &extprocv3.HeaderMutation{}
	for _, r := range p.rules {
		val, has := "", false
		if ipOK {
			v, found, err := p.mgr.Lookup(r.db, ip, r.path)
			switch {
			case err != nil:
				p.lookups.WithLabelValues(r.db, "error").Inc()
				p.logger.Debug("lookup failed", "db", r.db, "header", r.name, "err", err)
			case found:
				val, has = v, true
				p.lookups.WithLabelValues(r.db, "hit").Inc()
			default:
				p.lookups.WithLabelValues(r.db, "miss").Inc()
			}
		}
		if !has && r.def != nil {
			val, has = *r.def, true
		}
		if !has {
			// Anti-spoofing: drop a client-supplied value we would otherwise trust.
			if p.overwrite {
				mut.RemoveHeaders = append(mut.RemoveHeaders, r.name)
			}
			continue
		}
		mut.SetHeaders = append(mut.SetHeaders, &corev3.HeaderValueOption{
			Header:       &corev3.HeaderValue{Key: r.name, RawValue: []byte(val)},
			AppendAction: appendAction,
		})
	}
	return mut
}

// envoySourceAddress extracts the "source.address" attribute if the filter
// was configured with request_attributes: [source.address].
func envoySourceAddress(req *extprocv3.ProcessingRequest) string {
	for _, st := range req.GetAttributes() {
		if f, ok := st.GetFields()["source.address"]; ok {
			return f.GetStringValue()
		}
	}
	return ""
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `go test -race ./internal/extproc/ ./internal/geodb/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add internal/extproc internal/geodb go.mod go.sum
git commit -m "feat: ext_proc server with geoip header mutations"
```

---

### Task 8: Admin HTTP (healthz/readyz/metrics)

**Files:**
- Create: `internal/admin/admin.go`
- Test: `internal/admin/admin_test.go`

**Interfaces:**
- Consumes: интерфейс `Readier interface{ Ready() bool }` (реализуется `*geodb.Manager`), `*prometheus.Registry`.
- Produces: `admin.Handler(r Readier, reg *prometheus.Registry) http.Handler` — mux с `/healthz`, `/readyz`, `/metrics`.

- [ ] **Step 1: Write the failing test**

`internal/admin/admin_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/admin/`
Expected: FAIL (package does not exist)

- [ ] **Step 3: Implement**

`internal/admin/admin.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -race ./internal/admin/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/admin
git commit -m "feat: admin endpoints healthz/readyz/metrics"
```

---

### Task 9: main wiring + example config

**Files:**
- Create: `cmd/geoip-processor/main.go`, `examples/config.yaml`, `.gitignore`

**Interfaces:**
- Consumes: всё из Tasks 1–8.
- Produces: бинарь `geoip-processor -config <path>`; фетчеры выбираются по схеме source (`http/https` → `HTTPFetcher`, `s3` → `NewS3Fetcher`); graceful shutdown по SIGINT/SIGTERM; на старте `LoadCache()` + фоновый `CheckNow` и `Run`.

- [ ] **Step 1: Implement main**

`cmd/geoip-processor/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"envoy-geoip-processor/internal/admin"
	"envoy-geoip-processor/internal/config"
	"envoy-geoip-processor/internal/extproc"
	"envoy-geoip-processor/internal/geodb"
	"envoy-geoip-processor/internal/ipsrc"
)

func main() {
	configPath := flag.String("config", "/etc/geoip/config.yaml", "path to config file")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(*configPath, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func buildFetchers(ctx context.Context, cfg *config.Config) (map[string]geodb.Fetcher, error) {
	out := map[string]geodb.Fetcher{}
	for name, db := range cfg.Databases {
		u, _ := url.Parse(db.Source) // validated in config.Load
		switch u.Scheme {
		case "http", "https":
			out[name] = &geodb.HTTPFetcher{URL: db.Source, BasicEnv: db.Auth.BasicEnv}
		case "s3":
			f, err := geodb.NewS3Fetcher(ctx, db.Source)
			if err != nil {
				return nil, fmt.Errorf("database %s: %w", name, err)
			}
			out[name] = f
		}
	}
	return out, nil
}

func run(configPath string, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	fetchers, err := buildFetchers(ctx, cfg)
	if err != nil {
		return err
	}
	mgr, err := geodb.NewManager(cfg, fetchers, logger, reg)
	if err != nil {
		return err
	}
	mgr.LoadCache()
	go mgr.Run(ctx) // первая проверка каждой базы выполняется сразу внутри Run

	processor, err := extproc.New(cfg, mgr, ipsrc.New(cfg.IPSources), logger, reg)
	if err != nil {
		return err
	}

	grpcSrv := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(grpcSrv, processor)
	lis, err := net.Listen("tcp", cfg.Listen.GRPC)
	if err != nil {
		return err
	}
	adminSrv := &http.Server{Addr: cfg.Listen.Admin, Handler: admin.Handler(mgr, reg)}

	errCh := make(chan error, 2)
	go func() { errCh <- grpcSrv.Serve(lis) }()
	go func() { errCh <- adminSrv.ListenAndServe() }()
	logger.Info("started", "grpc", cfg.Listen.GRPC, "admin", cfg.Listen.Admin)

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	adminSrv.Shutdown(shutdownCtx)
	grpcSrv.GracefulStop()
	return nil
}
```

`examples/config.yaml`:

```yaml
listen:
  grpc: :9000
  admin: :8080

cache_dir: /var/cache/geoip

ip_sources:
  - header: x-real-ip
  - header: x-forwarded-for
  - envoy: source_address

overwrite: true

databases:
  city:
    source: https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz
    auth:
      basic_env: MAXMIND_LICENSE   # "account_id:license_key"
    check_interval: 6h
    required: true
  asn:
    source: https://download.maxmind.com/geoip/databases/GeoLite2-ASN/download?suffix=tar.gz
    auth:
      basic_env: MAXMIND_LICENSE
    check_interval: 6h

headers:
  x-geoip-country-code: {db: city, path: country.iso_code}
  x-geoip-country-name: {db: city, path: country.names.en}
  x-geoip-region:       {db: city, path: subdivisions.0.iso_code}
  x-geoip-region-name:  {db: city, path: subdivisions.0.names.en}
  x-geoip-city:         {db: city, path: city.names.en}
  x-geoip-latitude:     {db: city, path: location.latitude}
  x-geoip-longitude:    {db: city, path: location.longitude}
  x-geoip-postal-code:  {db: city, path: postal.code}
  x-geoip-timezone:     {db: city, path: location.time_zone}
  x-geoip-asn:          {db: asn, path: autonomous_system_number}
  x-geoip-org:          {db: asn, path: autonomous_system_organization}
```

`.gitignore`:

```
/geoip-processor
/cache/
```

- [ ] **Step 2: Verify build and smoke-run**

```bash
go build ./... && go vet ./...
go test -race ./...
```

Smoke: конфиг с локальной базой (без сети):

```bash
mkdir -p /tmp/geoip-smoke && cp testdata/GeoIP2-City-Test.mmdb /tmp/geoip-smoke/city.mmdb
cat > /tmp/geoip-smoke/config.yaml <<'EOF'
cache_dir: /tmp/geoip-smoke
ip_sources: [{header: x-real-ip}]
databases:
  city: {source: "https://localhost:1/unused", check_interval: 6h, required: true}
headers:
  x-geoip-country-code: {db: city, path: country.iso_code}
EOF
go run ./cmd/geoip-processor -config /tmp/geoip-smoke/config.yaml & APP_PID=$!
sleep 1
curl -fsS localhost:8080/readyz && echo " ready"
curl -fsS localhost:8080/healthz && echo " healthy"
kill $APP_PID
```

Expected: `ready`, `healthy` (база подхватилась из кэша; фоновая проверка падает с error в логе — это ок, fail-open).

- [ ] **Step 3: Commit**

```bash
git add cmd examples .gitignore
git commit -m "feat: main wiring, example config"
```

---

### Task 10: Dockerfile + docker-compose интеграция с Envoy

**Files:**
- Create: `Dockerfile`, `deploy/compose/docker-compose.yaml`, `deploy/compose/envoy.yaml`, `deploy/compose/config.yaml`

**Interfaces:**
- Consumes: бинарь из Task 9, testdata из Task 2.
- Produces: `docker compose -f deploy/compose/docker-compose.yaml up` поднимает envoy(:10000) → ext_proc → echo; e2e проверка через curl.

- [ ] **Step 1: Dockerfile**

```dockerfile
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/geoip-processor ./cmd/geoip-processor

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/geoip-processor /geoip-processor
ENTRYPOINT ["/geoip-processor"]
```

- [ ] **Step 2: compose config**

`deploy/compose/config.yaml`:

```yaml
listen:
  grpc: :9000
  admin: :8080
cache_dir: /tmp/geoip-cache
ip_sources:
  - header: x-real-ip
  - header: x-forwarded-for
  - envoy: source_address
databases:
  city:
    source: http://dbserver/GeoIP2-City-Test.mmdb
    check_interval: 1m
    required: true
  asn:
    source: http://dbserver/GeoLite2-ASN-Test.mmdb
    check_interval: 1m
headers:
  x-geoip-country-code: {db: city, path: country.iso_code}
  x-geoip-city:         {db: city, path: city.names.en}
  x-geoip-timezone:     {db: city, path: location.time_zone}
  x-geoip-latitude:     {db: city, path: location.latitude}
  x-geoip-asn:          {db: asn, path: autonomous_system_number}
  x-geoip-org:          {db: asn, path: autonomous_system_organization}
```

`deploy/compose/envoy.yaml`:

```yaml
static_resources:
  listeners:
  - name: main
    address: {socket_address: {address: 0.0.0.0, port_value: 10000}}
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress
          route_config:
            name: local
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: {prefix: "/"}
                route: {cluster: echo}
          http_filters:
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              failure_mode_allow: true
              message_timeout: 0.5s
              processing_mode:
                request_header_mode: SEND
                response_header_mode: SKIP
              request_attributes: [source.address]
              grpc_service:
                envoy_grpc: {cluster_name: geoip}
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: geoip
    type: STRICT_DNS
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config: {http2_protocol_options: {}}
    load_assignment:
      cluster_name: geoip
      endpoints:
      - lb_endpoints:
        - endpoint: {address: {socket_address: {address: geoip, port_value: 9000}}}
  - name: echo
    type: STRICT_DNS
    load_assignment:
      cluster_name: echo
      endpoints:
      - lb_endpoints:
        - endpoint: {address: {socket_address: {address: echo, port_value: 8080}}}
```

`deploy/compose/docker-compose.yaml`:

```yaml
services:
  dbserver:
    image: nginx:alpine
    volumes:
      - ../../testdata:/usr/share/nginx/html:ro
  geoip:
    build: ../..
    command: ["-config", "/etc/geoip/config.yaml"]
    volumes:
      - ./config.yaml:/etc/geoip/config.yaml:ro
    depends_on: [dbserver]
  echo:
    image: mendhak/http-https-echo:37
  envoy:
    image: envoyproxy/envoy:v1.33-latest
    command: ["-c", "/etc/envoy/envoy.yaml"]
    volumes:
      - ./envoy.yaml:/etc/envoy/envoy.yaml:ro
    ports: ["10000:10000"]
    depends_on: [geoip, echo]
```

- [ ] **Step 3: Verify e2e**

```bash
docker compose -f deploy/compose/docker-compose.yaml up -d --build
sleep 5
curl -s -H 'x-real-ip: 2.125.160.216' localhost:10000/ | python3 -c "import json,sys; h=json.load(sys.stdin)['headers']; print(h.get('x-geoip-country-code'), h.get('x-geoip-city'), h.get('x-geoip-asn', ''))"
curl -s -H 'x-real-ip: 1.128.0.0' localhost:10000/ | python3 -c "import json,sys; h=json.load(sys.stdin)['headers']; print(h.get('x-geoip-asn'), h.get('x-geoip-org'))"
docker compose -f deploy/compose/docker-compose.yaml down
```

Expected: первый curl печатает `GB Boxford ` (ASN для этого IP в тестовой базе нет — хедер отсутствует), второй — `1221 Telstra Pty Ltd`.

- [ ] **Step 4: Commit**

```bash
git add Dockerfile deploy
git commit -m "feat: Dockerfile and docker-compose e2e stack with Envoy"
```

---

### Task 11: Helm chart + EnvoyExtensionPolicy

**Files:**
- Create: `charts/geoip-processor/Chart.yaml`, `values.yaml`, `templates/_helpers.tpl`, `templates/deployment.yaml`, `templates/service.yaml`, `templates/configmap.yaml`, `templates/envoyextensionpolicy.yaml`

**Interfaces:**
- Consumes: образ из Task 10, конфиг-схема из Task 1.
- Produces: `helm template` рендерит валидные манифесты; политика подключает сервис к Gateway.

- [ ] **Step 1: Chart files**

`charts/geoip-processor/Chart.yaml`:

```yaml
apiVersion: v2
name: geoip-processor
description: Envoy ext_proc service that enriches requests with MaxMind geoip headers
type: application
version: 0.1.0
appVersion: "0.1.0"
```

`charts/geoip-processor/values.yaml`:

```yaml
image:
  repository: geoip-processor
  tag: latest
  pullPolicy: IfNotPresent

replicas: 2

resources:
  requests: {cpu: 100m, memory: 128Mi}
  limits: {memory: 512Mi}

# Env для кредов (например MAXMIND_LICENSE из секрета)
env: []
#  - name: MAXMIND_LICENSE
#    valueFrom: {secretKeyRef: {name: maxmind, key: license}}

# Контент /etc/geoip/config.yaml (см. examples/config.yaml в репо)
config:
  listen: {grpc: ":9000", admin: ":8080"}
  cache_dir: /var/cache/geoip
  ip_sources:
    - header: x-real-ip
    - header: x-forwarded-for
    - envoy: source_address
  databases: {}
  headers: {}

envoyExtensionPolicy:
  enabled: false
  # targetRef целится в Gateway, к которому цепляем процессор
  targetRef:
    group: gateway.networking.k8s.io
    kind: Gateway
    name: my-gateway
  failOpen: true
  messageTimeout: 200ms
  # Требует Envoy Gateway >= v1.3 (передача атрибутов в ext_proc).
  requestAttributes: [source.address]
```

`charts/geoip-processor/templates/_helpers.tpl`:

```
{{- define "geoip.name" -}}
{{ .Release.Name }}-geoip-processor
{{- end }}
```

`charts/geoip-processor/templates/configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "geoip.name" . }}
data:
  config.yaml: |
{{ toYaml .Values.config | indent 4 }}
```

`charts/geoip-processor/templates/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "geoip.name" . }}
spec:
  replicas: {{ .Values.replicas }}
  selector:
    matchLabels: {app: {{ include "geoip.name" . }}}
  template:
    metadata:
      labels: {app: {{ include "geoip.name" . }}}
      annotations:
        checksum/config: {{ toYaml .Values.config | sha256sum }}
    spec:
      containers:
      - name: geoip-processor
        image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
        imagePullPolicy: {{ .Values.image.pullPolicy }}
        args: ["-config", "/etc/geoip/config.yaml"]
        ports:
        - {name: grpc, containerPort: 9000}
        - {name: admin, containerPort: 8080}
        env: {{ toYaml .Values.env | nindent 8 }}
        readinessProbe:
          httpGet: {path: /readyz, port: admin}
        livenessProbe:
          httpGet: {path: /healthz, port: admin}
        resources: {{ toYaml .Values.resources | nindent 10 }}
        volumeMounts:
        - {name: config, mountPath: /etc/geoip, readOnly: true}
        - {name: cache, mountPath: /var/cache/geoip}
      volumes:
      - name: config
        configMap: {name: {{ include "geoip.name" . }}}
      - name: cache
        emptyDir: {}
```

`charts/geoip-processor/templates/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "geoip.name" . }}
spec:
  selector: {app: {{ include "geoip.name" . }}}
  ports:
  - {name: grpc, port: 9000, targetPort: grpc, appProtocol: kubernetes.io/h2c}
  - {name: admin, port: 8080, targetPort: admin}
```

`charts/geoip-processor/templates/envoyextensionpolicy.yaml`:

```yaml
{{- if .Values.envoyExtensionPolicy.enabled }}
apiVersion: gateway.envoyproxy.io/v1alpha1
kind: EnvoyExtensionPolicy
metadata:
  name: {{ include "geoip.name" . }}
spec:
  targetRefs:
  - group: {{ .Values.envoyExtensionPolicy.targetRef.group }}
    kind: {{ .Values.envoyExtensionPolicy.targetRef.kind }}
    name: {{ .Values.envoyExtensionPolicy.targetRef.name }}
  extProc:
  - backendRefs:
    - name: {{ include "geoip.name" . }}
      port: 9000
    failOpen: {{ .Values.envoyExtensionPolicy.failOpen }}
    messageTimeout: {{ .Values.envoyExtensionPolicy.messageTimeout }}
    processingMode:
      request:
        {{- with .Values.envoyExtensionPolicy.requestAttributes }}
        attributes: {{ toYaml . | nindent 8 }}
        {{- end }}
{{- end }}
```

Примечание для исполнителя: поле `processingMode.request.attributes` появилось в Envoy Gateway v1.3. Перед включением сверь схему с установленной версией: `kubectl explain envoyextensionpolicy.spec.extProc.processingMode.request --recursive`. Если поле называется иначе или отсутствует — убери его из шаблона (цепочка ip_sources всё равно отработает по хедерам) и зафиксируй это в README.

- [ ] **Step 2: Verify**

```bash
helm template test charts/geoip-processor >/dev/null && echo OK
helm template test charts/geoip-processor --set envoyExtensionPolicy.enabled=true | grep -A5 EnvoyExtensionPolicy
```

Expected: `OK`; политика рендерится с backendRef на сервис.

- [ ] **Step 3: Commit**

```bash
git add charts
git commit -m "feat: Helm chart with EnvoyExtensionPolicy for envoy-gateway"
```

---

### Task 12: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write README**

Содержание (написать по факту реализации, на английском):
- Что это: ext_proc для Envoy Gateway, geoip-хедеры из MaxMind баз (аналог ngx_http_geoip2_module).
- Features: auto-download (HTTPS/S3), conditional refresh, произвольный маппинг хедеров (таблица с примером полного набора из `examples/config.yaml`), цепочка ip_sources, fail-open + readiness-гейт, метрики.
- Quickstart: `docker compose -f deploy/compose/docker-compose.yaml up` + пример curl из Task 10.
- Config reference: таблица всех полей из `internal/config`.
- Deploy: helm install + включение EnvoyExtensionPolicy, требование Envoy Gateway >= v1.3 для `source.address`, секрет с MAXMIND_LICENSE.
- Metrics: список метрик и лейблов.

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: README with quickstart and config reference"
```
