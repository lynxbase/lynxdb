package otlphttp

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/limits"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlpcommon"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type SubmitFunc func(context.Context, []*event.Event) error

type Metrics interface {
	limits.Hook
	RecordOTLPRequest(signal, encoding, result string, bytes int)
	RecordOTLPRecords(signal, result string, count int)
	RecordDroppedRecords(source, reason string, count int)
}

type Config struct {
	Listen string
	Limits limits.Config
	// Middleware, when set, wraps every request before its body is read.
	Middleware func(http.Handler) http.Handler
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
	// Only logs are served: an exporter of traces or metrics gets 404 and
	// reports the data it could not send.
	mux.HandleFunc("POST /v1/logs", r.handleLogs)
	handler := limits.DualLimitMiddleware(cfg.Limits, metrics)(mux)
	if cfg.Middleware != nil {
		handler = cfg.Middleware(handler)
	}
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.server.Shutdown(shutdownCtx)
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
	encoding, bytes, ok := decodeRequest(w, req, &export)
	if ok && encoding == "json" {
		if err := idsFromHex(export.GetResourceLogs()); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			ok = false
		}
	}
	if !ok {
		r.recordRequest("logs", encoding, "error", bytes)
		return
	}
	total := otlpcommon.CountLogRecords(export.GetResourceLogs())
	events := otlpcommon.LogsToEvents(export.GetResourceLogs())
	dropped := total - len(events)
	r.recordRecords("logs", "accepted", len(events))
	r.recordRecords("logs", "dropped", dropped)
	r.recordDropped("otlp", "empty_body", dropped)
	if len(events) > 0 {
		if err := r.submit(req.Context(), events); err != nil {
			w.Header().Set("Retry-After", "1")
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			r.recordRequest("logs", encoding, "error", bytes)
			return
		}
	}
	r.recordRequest("logs", encoding, "success", bytes)
	writeProtoResponse(w, encoding, &logscollector.ExportLogsServiceResponse{})
}

func decodeRequest(w http.ResponseWriter, req *http.Request, msg proto.Message) (string, int, bool) {
	encoding := requestEncoding(req)
	body := req.Body
	defer func() {
		_ = body.Close()
	}()

	switch encoding {
	case "protobuf":
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return encoding, len(data), false
		}
		if err := proto.Unmarshal(data, msg); err != nil {
			http.Error(w, "invalid protobuf: "+err.Error(), http.StatusBadRequest)
			return encoding, len(data), false
		}
		return encoding, len(data), true
	case "json":
		data, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return encoding, len(data), false
		}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, msg); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return encoding, len(data), false
		}
		return encoding, len(data), true
	default:
		http.Error(w, "unsupported content type", http.StatusUnsupportedMediaType)
		return encoding, 0, false
	}
}

// idsFromHex sets the trace and span IDs of records read from OTLP/JSON to
// the bytes their text names. OTLP/JSON writes these IDs in hex, where the
// JSON mapping of protobuf that protojson reads writes bytes in base64. Hex
// text is valid base64, so protojson decoded it as such; encoding those
// bytes in base64 again gives back the hex text.
func idsFromHex(resourceLogs []*logspb.ResourceLogs) error {
	for _, rl := range resourceLogs {
		for _, sl := range rl.GetScopeLogs() {
			for _, rec := range sl.GetLogRecords() {
				var err error
				if rec.TraceId, err = hexID(rec.TraceId); err != nil {
					return fmt.Errorf("traceId: %w", err)
				}
				if rec.SpanId, err = hexID(rec.SpanId); err != nil {
					return fmt.Errorf("spanId: %w", err)
				}
			}
		}
	}
	return nil
}

func hexID(decoded []byte) ([]byte, error) {
	if len(decoded) == 0 {
		return decoded, nil
	}
	return hex.DecodeString(base64.StdEncoding.EncodeToString(decoded))
}

func (r *Receiver) recordRequest(signal, encoding, result string, bytes int) {
	if r.metrics != nil {
		r.metrics.RecordOTLPRequest(signal, encoding, result, bytes)
	}
}

func (r *Receiver) recordRecords(signal, result string, count int) {
	if r.metrics != nil {
		r.metrics.RecordOTLPRecords(signal, result, count)
	}
}

func (r *Receiver) recordDropped(source, reason string, count int) {
	if r.metrics != nil {
		r.metrics.RecordDroppedRecords(source, reason, count)
	}
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
	case "application/x-protobuf":
		return "protobuf"
	case "application/json":
		return "json"
	default:
		return "unsupported"
	}
}
