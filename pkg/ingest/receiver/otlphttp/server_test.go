package otlphttp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func newTestReceiver(t *testing.T, submitted *[]*event.Event) *Receiver {
	t.Helper()
	r, err := New(Config{Listen: "127.0.0.1:0"}, func(_ context.Context, events []*event.Event) error {
		*submitted = append(*submitted, events...)
		return nil
	}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func post(r *Receiver, path, contentType string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rr := httptest.NewRecorder()
	r.server.Handler.ServeHTTP(rr, req)
	return rr
}

func TestHandler_LogsProtobuf_SubmitsEvents(t *testing.T) {
	var submitted []*event.Event
	r := newTestReceiver(t, &submitted)
	body, err := proto.Marshal(&logscollector.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TraceId: []byte{0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f, 0xc6, 0x0c},
					Body:    &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	rr := post(r, "/v1/logs", "application/x-protobuf", body)

	if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "application/x-protobuf" {
		t.Fatalf("status = %d, content type %q", rr.Code, rr.Header().Get("Content-Type"))
	}
	if len(submitted) != 1 {
		t.Fatalf("submitted = %d, want 1", len(submitted))
	}
	if got := submitted[0].GetField("trace_id").AsString(); got != "5b8efff798038103d269b633813fc60c" {
		t.Fatalf("trace_id = %q", got)
	}
}

// otlpJSON is a request as an OpenTelemetry SDK writes it: hex trace and
// span IDs, an integer severity, 64-bit integers as strings, and a field
// that this version of the protocol does not know.
const otlpJSON = `{
  "resourceLogs": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]},
    "scopeLogs": [{
      "scope": {"name": "app.logger", "version": "1.2.0"},
      "logRecords": [{
        "timeUnixNano": "1736928000000000000",
        "severityNumber": 17,
        "traceId": "5B8EFFF798038103D269B633813FC60C",
        "spanId": "eee19b7ec3c1b174",
        "body": {"stringValue": "payment failed"},
        "attributes": [{"key": "order.id", "value": {"intValue": "42"}}],
        "eventName": "payment.failure",
        "someFutureField": {"x": 1}
      }]
    }]
  }]
}`

func TestHandler_LogsJSON_MapsLikeProtobuf(t *testing.T) {
	var submitted []*event.Event
	r := newTestReceiver(t, &submitted)

	rr := post(r, "/v1/logs", "application/json; charset=utf-8", []byte(otlpJSON))

	if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status = %d, content type %q: %s", rr.Code, rr.Header().Get("Content-Type"), rr.Body)
	}
	if len(submitted) != 1 {
		t.Fatalf("submitted = %d, want 1", len(submitted))
	}
	ev := submitted[0]
	if !ev.Time.Equal(time.Unix(1736928000, 0)) || ev.Raw != "payment failed" || ev.Source != "checkout" || ev.SourceType != "otlp" {
		t.Fatalf("event: time %v raw %q source %q sourcetype %q", ev.Time, ev.Raw, ev.Source, ev.SourceType)
	}
	for field, want := range map[string]string{
		"level":         "error",
		"trace_id":      "5b8efff798038103d269b633813fc60c",
		"span_id":       "eee19b7ec3c1b174",
		"order.id":      "42",
		"scope.name":    "app.logger",
		"scope.version": "1.2.0",
	} {
		if got := ev.GetField(field).AsString(); got != want {
			t.Errorf("%s = %q, want %q", field, got, want)
		}
	}
}

func TestHandler_LogsJSON_RefusesIDsThatAreNotHex(t *testing.T) {
	var submitted []*event.Event
	r := newTestReceiver(t, &submitted)
	body := strings.Replace(otlpJSON, `"eee19b7ec3c1b174"`, `"7uGbfsPBsXQ="`, 1)

	rr := post(r, "/v1/logs", "application/json", []byte(body))

	if rr.Code != http.StatusBadRequest || len(submitted) != 0 {
		t.Fatalf("status = %d, submitted %d; want 400 and none", rr.Code, len(submitted))
	}
}

func TestHandler_LogsRefusesOtherContentTypes(t *testing.T) {
	for _, contentType := range []string{"", "text/plain", "application/protobuf"} {
		var submitted []*event.Event
		r := newTestReceiver(t, &submitted)

		rr := post(r, "/v1/logs", contentType, []byte(otlpJSON))

		if rr.Code != http.StatusUnsupportedMediaType || len(submitted) != 0 {
			t.Errorf("content type %q: status = %d, submitted %d; want 415 and none", contentType, rr.Code, len(submitted))
		}
	}
}

func TestHandler_TracesAndMetricsAreNotFound(t *testing.T) {
	for _, path := range []string{"/v1/traces", "/v1/metrics"} {
		for _, contentType := range []string{"application/x-protobuf", "application/json"} {
			r := newTestReceiver(t, new([]*event.Event))

			rr := post(r, path, contentType, []byte(`{}`))

			if rr.Code != http.StatusNotFound {
				t.Errorf("%s (%s): status = %d, want 404", path, contentType, rr.Code)
			}
		}
	}
}
