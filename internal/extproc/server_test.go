package extproc

import (
	"context"
	"log/slog"
	"net"
	"os"
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
	// IP absent from the database: country gets its default, city is not set and lands in remove (anti-spoofing).
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
	// Without an IP there are no lookup values, but defaults still apply.
	if got["x-geoip-country-code"] != "XX" {
		t.Errorf("default must apply without ip: %v", got)
	}
}
