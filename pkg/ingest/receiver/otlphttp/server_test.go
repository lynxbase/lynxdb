package otlphttp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestHandler_LogsProtobuf_SubmitsEvents(t *testing.T) {
	var submitted int
	r, err := New(Config{Listen: "127.0.0.1:0"}, func(_ context.Context, events []*event.Event) error {
		submitted += len(events)
		return nil
	}, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	body, err := proto.Marshal(&logscollector.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/logs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rr := httptest.NewRecorder()

	r.handleLogs(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if submitted != 1 {
		t.Fatalf("submitted = %d, want 1", submitted)
	}
}
