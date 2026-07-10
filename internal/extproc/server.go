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
		}, []string{"result"}),
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
