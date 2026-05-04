package splunkhec

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestHandler_EventRequiresSplunkToken(t *testing.T) {
	h := NewHandler(Config{Auth: AuthConfig{Enabled: true}}, func(context.Context, []*event.Event) error { return nil })
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/services/collector/event", strings.NewReader(`{"event":"hello"}`))

	h.HandleEvent(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rr.Code)
	}
}

func TestHandler_Raw_LinePerEvent_AllParsed(t *testing.T) {
	var submitted int
	h := NewHandler(Config{Auth: AuthConfig{Enabled: true}}, func(_ context.Context, events []*event.Event) error {
		submitted += len(events)
		return nil
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/services/collector/raw?source=raw-src", strings.NewReader("one\ntwo\n"))
	req.Header.Set("Authorization", "Splunk token")

	h.HandleRaw(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if submitted != 2 {
		t.Fatalf("submitted = %d, want 2", submitted)
	}
}

func TestHandler_EventSubmitError_UsesConfiguredResponder(t *testing.T) {
	submitErr := errors.New("backpressure")
	h := NewHandler(Config{
		Auth: AuthConfig{Enabled: true},
		RespondIngestError: func(w http.ResponseWriter, err error) {
			if !errors.Is(err, submitErr) {
				t.Fatalf("err = %v, want submitErr", err)
			}
			w.Header().Set("Retry-After", "5")
			respond(w, http.StatusServiceUnavailable, "custom", 9)
		},
	}, func(context.Context, []*event.Event) error {
		return submitErr
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/services/collector/event", strings.NewReader(`{"event":"hello"}`))
	req.Header.Set("Authorization", "Splunk token")

	h.HandleEvent(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
}

func TestHandler_AckRoundTrip(t *testing.T) {
	store := NewAckStore(10)
	var flushed bool
	h := NewHandler(Config{
		Auth:     AuthConfig{Enabled: true},
		AckStore: store,
		AckFlush: func(context.Context) error {
			flushed = true
			return nil
		},
	}, func(context.Context, []*event.Event) error { return nil })
	eventRR := httptest.NewRecorder()
	eventReq := httptest.NewRequest(http.MethodPost, "/services/collector/event", strings.NewReader(`{"event":"hello"}`))
	eventReq.Header.Set("Authorization", "Splunk token")
	eventReq.Header.Set("X-Splunk-Request-Channel", "channel-a")

	h.HandleEvent(eventRR, eventReq)

	if eventRR.Code != http.StatusOK {
		t.Fatalf("event status = %d, want 200", eventRR.Code)
	}
	var eventBody map[string]interface{}
	if err := json.NewDecoder(eventRR.Body).Decode(&eventBody); err != nil {
		t.Fatalf("decode event response: %v", err)
	}
	ackID, ok := eventBody["ackId"].(float64)
	if !ok || ackID == 0 {
		t.Fatalf("ackId = %#v, want positive number", eventBody["ackId"])
	}
	if !flushed {
		t.Fatal("AckFlush was not called before ackId response")
	}

	ackRR := httptest.NewRecorder()
	ackReq := httptest.NewRequest(http.MethodPost, "/services/collector/ack", strings.NewReader(`{"acks":[1]}`))
	ackReq.Header.Set("Authorization", "Splunk token")
	ackReq.Header.Set("X-Splunk-Request-Channel", "channel-a")

	h.HandleAck(ackRR, ackReq)

	if ackRR.Code != http.StatusOK {
		t.Fatalf("ack status = %d, want 200", ackRR.Code)
	}
	var ackBody struct {
		Acks map[string]bool `json:"acks"`
	}
	if err := json.NewDecoder(ackRR.Body).Decode(&ackBody); err != nil {
		t.Fatalf("decode ack response: %v", err)
	}
	if !ackBody.Acks["1"] {
		t.Fatalf("acks[1] = false, want true")
	}
}

func TestHandler_AckFlushError_UsesConfiguredResponder(t *testing.T) {
	flushErr := errors.New("flush failed")
	h := NewHandler(Config{
		Auth:     AuthConfig{Enabled: true},
		AckStore: NewAckStore(10),
		AckFlush: func(context.Context) error {
			return flushErr
		},
		RespondIngestError: func(w http.ResponseWriter, err error) {
			if !errors.Is(err, flushErr) {
				t.Fatalf("err = %v, want flushErr", err)
			}
			respond(w, http.StatusServiceUnavailable, "custom", 9)
		},
	}, func(context.Context, []*event.Event) error { return nil })
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/services/collector/event", strings.NewReader(`{"event":"hello"}`))
	req.Header.Set("Authorization", "Splunk token")
	req.Header.Set("X-Splunk-Request-Channel", "channel-a")

	h.HandleEvent(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if acks := h.cfg.AckStore.Check("channel-a", []int{1}); acks[1] {
		t.Fatal("ack was recorded after flush failure")
	}
}

func TestHandler_Health_Returns200(t *testing.T) {
	h := NewHandler(Config{}, nil)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/services/collector/health", nil)

	h.HandleHealth(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
}
