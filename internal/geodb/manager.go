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

// fetchTimeout bounds a single download attempt so a hung or slow origin
// can't stall a database's refresh goroutine forever.
const fetchTimeout = 5 * time.Minute

type dbState struct {
	name     string
	required bool
	interval time.Duration
	fetcher  Fetcher
	reader   atomic.Pointer[maxminddb.Reader]
	meta     Meta // touched only by the update goroutine / CheckNow
}

// Manager owns all configured databases: disk cache, refresh, lookups.
type Manager struct {
	dbs       map[string]*dbState
	cacheDir  string
	logger    *slog.Logger
	updates   *prometheus.CounterVec
	loadedAt  *prometheus.GaugeVec
	lastCheck *prometheus.GaugeVec
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
		lastCheck: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "geoip_db_last_check_timestamp_seconds",
			Help: "Unix time of the last completed update check.",
		}, []string{"db"}),
	}
	reg.MustRegister(m.updates, m.loadedAt, m.lastCheck)
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

// CheckNow runs one synchronous update check for every database.
// Used at startup (foreground initial load) and in tests.
func (m *Manager) CheckNow(ctx context.Context) {
	for _, s := range m.dbs {
		m.checkOne(ctx, s)
	}
}

func (m *Manager) checkOne(ctx context.Context, s *dbState) {
	defer m.lastCheck.WithLabelValues(s.name).SetToCurrentTime()

	tmp := m.dbPath(s.name) + ".tmp"
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	changed, next, err := s.fetcher.Fetch(fetchCtx, tmp, s.meta)
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
