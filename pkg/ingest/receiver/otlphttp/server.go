package otlphttp

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricscollector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracecollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type SubmitFunc func(context.Context, []*event.Event) error

type Metrics interface {
	limits.Hook
}

type Config struct {
	Listen string
	Limits limits.Config
}

type Receiver struct {
	cfg       Config
	submit    SubmitFunc
	logger    *slog.Logger
	metrics   Metrics
	server    *http.Server
	listen    atomic.Value
	ready     chan struct{}
	readyOnce sync.Once
	startErr  atomic.Value
}

func New(cfg Config, submit SubmitFunc, logger *slog.Logger, metrics Metrics) (*Receiver, error) {
	if cfg.Listen == "" {
		return nil, nil
	}
	if _, _, err := net.SplitHostPort(cfg.Listen); err != nil {
		return nil, fmt.Errorf("otlp http listen: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	r := &Receiver{
		cfg:     cfg,
		submit:  submit,
		logger:  logger,
		metrics: metrics,
		ready:   make(chan struct{}),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", r.handleLogs)
	mux.HandleFunc("POST /v1/traces", r.handleTraces)
	mux.HandleFunc("POST /v1/metrics", r.handleMetrics)
	handler := limits.DualLimitMiddleware(cfg.Limits, metrics)(mux)
	r.server = &http.Server{Addr: cfg.Listen, Handler: handler}
	return r, nil
}

func (r *Receiver) Start(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", r.cfg.Listen)
	if err != nil {
		r.markReady(err)
		return fmt.Errorf("otlp http listen: %w", err)
	}
	r.listen.Store(ln.Addr().String())
	go func() {
		<-ctx.Done()
		_ = r.server.Shutdown(context.Background())
	}()
	r.markReady(nil)
	r.logger.Info("OTLP HTTP receiver started", "addr", r.Addr())
	if err := r.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (r *Receiver) Stop(ctx context.Context) error {
	return r.server.Shutdown(ctx)
}

func (r *Receiver) WaitReady() {
	<-r.ready
}

func (r *Receiver) ReadyError() error {
	if v := r.startErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

func (r *Receiver) Addr() string {
	if v := r.listen.Load(); v != nil {
		return v.(string)
	}
	return r.cfg.Listen
}

func (r *Receiver) markReady(err error) {
	if err != nil {
		r.startErr.Store(err)
	}
	r.readyOnce.Do(func() { close(r.ready) })
}

func (r *Receiver) handleLogs(w http.ResponseWriter, req *http.Request) {
	var export logscollector.ExportLogsServiceRequest
	encoding, ok := decodeRequest(w, req, &export)
	if !ok {
		return
	}
	events := LogsToEvents(export.GetResourceLogs())
	if len(events) > 0 {
		if err := r.submit(req.Context(), events); err != nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
	}
	writeProtoResponse(w, encoding, &logscollector.ExportLogsServiceResponse{})
}

func (r *Receiver) handleTraces(w http.ResponseWriter, req *http.Request) {
	encoding := requestEncoding(req)
	writeProtoResponse(w, encoding, &tracecollector.ExportTraceServiceResponse{})
}

func (r *Receiver) handleMetrics(w http.ResponseWriter, req *http.Request) {
	encoding := requestEncoding(req)
	writeProtoResponse(w, encoding, &metricscollector.ExportMetricsServiceResponse{})
}

func decodeRequest(w http.ResponseWriter, req *http.Request, msg proto.Message) (string, bool) {
	encoding := requestEncoding(req)
	body := req.Body
	defer body.Close()

	switch encoding {
	case "protobuf":
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return encoding, false
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			http.Error(w, "invalid protobuf: "+err.Error(), http.StatusBadRequest)
			return encoding, false
		}
	case "json":
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return encoding, false
		}
		if err := protojson.Unmarshal(data, msg); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return encoding, false
		}
	default:
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return encoding, false
	}
	return encoding, true
}

func writeProtoResponse(w http.ResponseWriter, encoding string, msg proto.Message) {
	if encoding == "json" {
		w.Header().Set("Content-Type", "application/json")
		b, _ := protojson.Marshal(msg)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	b, _ := proto.Marshal(msg)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)
}

func requestEncoding(req *http.Request) string {
	ct := strings.ToLower(strings.TrimSpace(strings.Split(req.Header.Get("Content-Type"), ";")[0]))
	switch ct {
	case "application/x-protobuf", "application/protobuf":
		return "protobuf"
	case "application/json", "":
		return "json"
	default:
		return "unsupported"
	}
}
