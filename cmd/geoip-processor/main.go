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
