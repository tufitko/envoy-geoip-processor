// Package config loads and validates the processor YAML configuration.
package config

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// dbNameRE restricts database map keys to characters safe to use verbatim
// as a cache filename component (see dbPath/metaPath in internal/geodb).
var dbNameRE = regexp.MustCompile(`^[a-z0-9_-]+$`)

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
	if parsed <= 0 {
		return fmt.Errorf("duration must be positive, got %q", s)
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
	if err := cfg.checkDuplicateKeys(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	return &cfg, nil
}

// checkDuplicateKeys catches header names and ip_sources header values that
// collide only once lowercased, before applyDefaults silently folds them
// together (map key last-write-wins for c.Headers). Run before applyDefaults
// so the original casing is available for the error message.
func (c *Config) checkDuplicateKeys() error {
	seenHeaders := map[string]string{}
	for name := range c.Headers {
		lower := strings.ToLower(name)
		if prev, ok := seenHeaders[lower]; ok && prev != name {
			return fmt.Errorf("headers: %q and %q collide once lowercased", prev, name)
		}
		seenHeaders[lower] = name
	}
	seenSources := map[string]string{}
	for _, s := range c.IPSources {
		if s.Header == "" {
			continue
		}
		lower := strings.ToLower(s.Header)
		if prev, ok := seenSources[lower]; ok && prev != s.Header {
			return fmt.Errorf("ip_sources: %q and %q collide once lowercased", prev, s.Header)
		}
		seenSources[lower] = s.Header
	}
	return nil
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
		if !dbNameRE.MatchString(name) {
			return fmt.Errorf("database name %q: must match [a-z0-9_-]+ (it is used as a cache filename)", name)
		}
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
