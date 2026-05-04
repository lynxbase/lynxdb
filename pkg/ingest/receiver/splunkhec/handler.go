package splunkhec

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
)

type SubmitFunc func(context.Context, []*event.Event) error

type Config struct {
	Auth               AuthConfig
	MaxBatchSize       int
	MaxLineBytes       int
	AckStore           *AckStore
	AckFlush           func(context.Context) error
	RespondIngestError func(http.ResponseWriter, error)
}

type Handler struct {
	cfg    Config
	submit SubmitFunc
}

func NewHandler(cfg Config, submit SubmitFunc) *Handler {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 1000
	}
	if cfg.MaxLineBytes <= 0 {
		cfg.MaxLineBytes = 1 << 20
	}
	return &Handler{cfg: cfg, submit: submit}
}

func (h *Handler) HandleEvent(w http.ResponseWriter, r *http.Request) {
	if err := h.cfg.Auth.Authorize(r); err != nil {
		respondAuthError(w, err)
		return
	}
	skipped, err := h.scan(r, func(line string) (*event.Event, bool) {
		var hec Event
		if err := json.Unmarshal([]byte(line), &hec); err != nil {
			return nil, false
		}
		return hec.ToEvent(), true
	})
	if err != nil {
		h.respondIngestError(w, err)
		return
	}
	resp := map[string]interface{}{"text": "Success", "code": 0}
	if ackID, err := h.recordAckAfterFlush(r); err != nil {
		h.respondIngestError(w, err)
		return
	} else if ackID > 0 {
		resp["ackId"] = ackID
	}
	if skipped > 0 {
		resp["skipped_lines"] = skipped
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleRaw(w http.ResponseWriter, r *http.Request) {
	if err := h.cfg.Auth.Authorize(r); err != nil {
		respondAuthError(w, err)
		return
	}
	source := r.URL.Query().Get("source")
	sourceType := r.URL.Query().Get("sourcetype")
	host := r.URL.Query().Get("host")
	index := r.URL.Query().Get("index")
	_, err := h.scan(r, func(line string) (*event.Event, bool) {
		return RawEvent(line, source, sourceType, host, index), true
	})
	if err != nil {
		h.respondIngestError(w, err)
		return
	}
	resp := map[string]interface{}{"text": "Success", "code": 0}
	if ackID, err := h.recordAckAfterFlush(r); err != nil {
		h.respondIngestError(w, err)
		return
	} else if ackID > 0 {
		resp["ackId"] = ackID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	respond(w, http.StatusOK, "HEC is healthy", 17)
}

func (h *Handler) HandleAck(w http.ResponseWriter, r *http.Request) {
	if err := h.cfg.Auth.Authorize(r); err != nil {
		respondAuthError(w, err)
		return
	}
	var req struct {
		Acks []int `json:"acks"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respond(w, http.StatusBadRequest, "Invalid ack request", 5)
		return
	}
	channel := r.Header.Get("X-Splunk-Request-Channel")
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"acks": h.cfg.AckStore.Check(channel, req.Acks),
	})
}

func (h *Handler) recordAckAfterFlush(r *http.Request) (int, error) {
	channel := r.Header.Get("X-Splunk-Request-Channel")
	if channel == "" || h.cfg.AckStore == nil {
		return 0, nil
	}
	if h.cfg.AckFlush != nil {
		if err := h.cfg.AckFlush(r.Context()); err != nil {
			return 0, err
		}
	}
	return h.cfg.AckStore.Record(channel, true), nil
}

func (h *Handler) scan(r *http.Request, build func(string) (*event.Event, bool)) (int, error) {
	scanner := bufio.NewScanner(r.Body)
	scanner.Buffer(make([]byte, 64*1024), h.cfg.MaxLineBytes)

	batch := make([]*event.Event, 0, h.cfg.MaxBatchSize)
	skipped := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := h.submit(r.Context(), batch)
		batch = batch[:0]
		return err
	}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		ev, ok := build(line)
		if !ok || ev == nil {
			skipped++
			continue
		}
		batch = append(batch, ev)
		if len(batch) >= h.cfg.MaxBatchSize {
			if err := flush(); err != nil {
				return skipped, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return skipped, err
	}
	return skipped, flush()
}

func respondAuthError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrMissingToken):
		respond(w, http.StatusUnauthorized, "Token is required", 2)
	case errors.Is(err, ErrInvalidToken):
		respond(w, http.StatusUnauthorized, "Invalid token", 3)
	default:
		respond(w, http.StatusUnauthorized, "Unauthorized", 2)
	}
}

func (h *Handler) respondIngestError(w http.ResponseWriter, err error) {
	if h.cfg.RespondIngestError != nil {
		h.cfg.RespondIngestError(w, err)
		return
	}
	respond(w, http.StatusServiceUnavailable, "Ingest failed", 9)
}

func respond(w http.ResponseWriter, status int, text string, code int) {
	writeJSON(w, status, map[string]interface{}{"text": text, "code": code})
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
