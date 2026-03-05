package receiver

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

// EventSink is the destination for processed events.
type EventSink interface {
	Write(events []*event.Event) error
}

// HTTPReceiver handles HTTP ingestion endpoints.
type HTTPReceiver struct {
	server     *http.Server
	pipeline   *pipeline.Pipeline
	sink       EventSink
	logger     *slog.Logger
	listenAddr atomic.Value // stores resolved listen address (string)
	ready      chan struct{}
	ingestSem  chan struct{} // semaphore for concurrent ingestion requests
}

// defaultMaxConcurrentIngests is the maximum number of concurrent ingest requests.
const defaultMaxConcurrentIngests = 64

// NewHTTPReceiver creates a new HTTP receiver.
func NewHTTPReceiver(addr string, pipe *pipeline.Pipeline, sink EventSink, logger *slog.Logger) *HTTPReceiver {
	r := &HTTPReceiver{
		pipeline:  pipe,
		sink:      sink,
		logger:    logger,
		ready:     make(chan struct{}),
		ingestSem: make(chan struct{}, defaultMaxConcurrentIngests),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/ingest", r.handleEvents)
	mux.HandleFunc("POST /api/v1/ingest/raw", r.handleRawEvents)
	mux.HandleFunc("POST /api/v1/ingest/hec", r.handleHEC)
	mux.HandleFunc("GET /health", r.handleHealth)

	r.server = &http.Server{
		Addr:              addr,
		Handler:           mux,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}

	return r
}

// Start starts the HTTP server.
func (r *HTTPReceiver) Start(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", r.server.Addr)
	if err != nil {
		return fmt.Errorf("receiver: listen: %w", err)
	}
	r.listenAddr.Store(ln.Addr().String())

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := r.server.Shutdown(shutdownCtx); err != nil {
			r.logger.Warn("HTTP receiver shutdown error (connections may not have drained)", "error", err)
		}
	}()

	close(r.ready)
	r.logger.Info("HTTP receiver started", "addr", r.Addr())
	if err := r.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// WaitReady blocks until the receiver has completed initialization and is ready.
func (r *HTTPReceiver) WaitReady() {
	<-r.ready
}

// Addr returns the address the server is listening on.
func (r *HTTPReceiver) Addr() string {
	if v := r.listenAddr.Load(); v != nil {
		return v.(string)
	}

	return r.server.Addr
}

// POST /api/v1/ingest — JSON array of events.
func (r *HTTPReceiver) handleEvents(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(io.LimitReader(req.Body, 10<<20)) // 10MB limit
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)

		return
	}

	var payload []EventPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)

		return
	}

	events := make([]*event.Event, len(payload))
	for i, p := range payload {
		events[i] = p.ToEvent()
	}

	if err := r.processAndWrite(events); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"count":  len(events),
	})
}

// POST /api/v1/ingest/raw — newline-delimited raw logs (streaming).
func (r *HTTPReceiver) handleRawEvents(w http.ResponseWriter, req *http.Request) {
	source := req.Header.Get("X-Source")
	if source == "" {
		source = "http"
	} else if len(source) > 256 {
		source = source[:256]
	}
	sourceType := req.Header.Get("X-Source-Type")
	if sourceType == "" {
		sourceType = "raw"
	} else if len(sourceType) > 256 {
		sourceType = sourceType[:256]
	}

	const batchSize = 1000
	scanner := bufio.NewScanner(req.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	batch := make([]*event.Event, 0, batchSize)
	total := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		e := event.NewEvent(time.Time{}, line)
		e.Source = source
		e.SourceType = sourceType
		batch = append(batch, e)

		if len(batch) >= batchSize {
			if err := r.processAndWrite(batch); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
			total += len(batch)
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		http.Error(w, fmt.Sprintf("read error: %v", err), http.StatusBadRequest)

		return
	}

	if len(batch) > 0 {
		if err := r.processAndWrite(batch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
		total += len(batch)
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"count":  total,
	})
}

// POST /api/v1/ingest/hec — Splunk HEC compatible endpoint (streaming).
func (r *HTTPReceiver) handleHEC(w http.ResponseWriter, req *http.Request) {
	const batchSize = 1000
	scanner := bufio.NewScanner(req.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	batch := make([]*event.Event, 0, batchSize)
	total := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var hec HECEvent
		if err := json.Unmarshal([]byte(line), &hec); err != nil {
			continue
		}
		batch = append(batch, hec.ToEvent())

		if len(batch) >= batchSize {
			if err := r.processAndWrite(batch); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)

				return
			}
			total += len(batch)
			batch = batch[:0]
		}
	}

	if err := scanner.Err(); err != nil {
		http.Error(w, fmt.Sprintf("read error: %v", err), http.StatusBadRequest)

		return
	}

	if total == 0 && len(batch) == 0 {
		http.Error(w, "no valid events", http.StatusBadRequest)

		return
	}

	if len(batch) > 0 {
		if err := r.processAndWrite(batch); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)

			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"text": "Success",
		"code": 0,
	})
}

func (r *HTTPReceiver) handleHealth(w http.ResponseWriter, req *http.Request) {
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func (r *HTTPReceiver) processAndWrite(events []*event.Event) error {
	select {
	case r.ingestSem <- struct{}{}:
		defer func() { <-r.ingestSem }()
	default:
		return fmt.Errorf("server overloaded: too many concurrent ingest requests (max %d)", defaultMaxConcurrentIngests)
	}

	processed, err := r.pipeline.Process(events)
	if err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}

	if err := r.sink.Write(processed); err != nil {
		return fmt.Errorf("sink: %w", err)
	}

	return nil
}

// EventPayload is the JSON format for /api/v1/ingest.
type EventPayload struct {
	Time       *float64               `json:"time,omitempty"`
	Raw        string                 `json:"event"`
	Source     string                 `json:"source,omitempty"`
	SourceType string                 `json:"sourcetype,omitempty"`
	Host       string                 `json:"host,omitempty"`
	Index      string                 `json:"index,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

func (p EventPayload) ToEvent() *event.Event {
	var ts time.Time
	if p.Time != nil {
		ts = time.Unix(int64(*p.Time), int64((*p.Time-float64(int64(*p.Time)))*1e9))
	}
	e := event.NewEvent(ts, p.Raw)
	e.Source = p.Source
	e.SourceType = p.SourceType
	e.Host = p.Host
	e.Index = p.Index
	for k, v := range p.Fields {
		switch val := v.(type) {
		case string:
			e.SetField(k, event.StringValue(val))
		case float64:
			if val == float64(int64(val)) {
				e.SetField(k, event.IntValue(int64(val)))
			} else {
				e.SetField(k, event.FloatValue(val))
			}
		case bool:
			e.SetField(k, event.BoolValue(val))
		}
	}

	return e
}

// HECEvent is the Splunk HEC event format.
type HECEvent struct {
	Time       *float64               `json:"time,omitempty"`
	Event      interface{}            `json:"event"`
	Source     string                 `json:"source,omitempty"`
	SourceType string                 `json:"sourcetype,omitempty"`
	Host       string                 `json:"host,omitempty"`
	Index      string                 `json:"index,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

func (h HECEvent) ToEvent() *event.Event {
	var ts time.Time
	if h.Time != nil {
		ts = time.Unix(int64(*h.Time), int64((*h.Time-float64(int64(*h.Time)))*1e9))
	}

	var raw string
	switch v := h.Event.(type) {
	case string:
		raw = v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			raw = fmt.Sprintf("%v", v)
		} else {
			raw = string(b)
		}
	}

	e := event.NewEvent(ts, raw)
	e.Source = h.Source
	e.SourceType = h.SourceType
	e.Host = h.Host
	e.Index = h.Index
	for k, v := range h.Fields {
		switch val := v.(type) {
		case string:
			e.SetField(k, event.StringValue(val))
		case float64:
			e.SetField(k, event.IntValue(int64(val)))
		}
	}

	return e
}
