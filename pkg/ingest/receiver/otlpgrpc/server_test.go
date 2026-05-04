package otlpgrpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/staging"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	metricscollector "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	tracecollector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestReceiver_ExportLogs_SubmitsEvents(t *testing.T) {
	var submitted int
	r := startTestReceiver(t, func(_ context.Context, events []*event.Event) error {
		submitted += len(events)
		return nil
	})
	conn := dialReceiver(t, r)
	defer conn.Close()

	_, err := logscollector.NewLogsServiceClient(conn).Export(context.Background(), testLogsRequest("hello"))
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if submitted != 1 {
		t.Fatalf("submitted = %d, want 1", submitted)
	}
}

func TestReceiver_ExportLogs_OverflowResourceExhausted(t *testing.T) {
	r := startTestReceiver(t, func(context.Context, []*event.Event) error {
		return staging.ErrBufferOverflow
	})
	conn := dialReceiver(t, r)
	defer conn.Close()

	_, err := logscollector.NewLogsServiceClient(conn).Export(context.Background(), testLogsRequest("hello"))
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code = %v, want ResourceExhausted; err=%v", status.Code(err), err)
	}
}

func TestReceiver_ExportLogs_InternalError(t *testing.T) {
	r := startTestReceiver(t, func(context.Context, []*event.Event) error {
		return errors.New("sink failed")
	})
	conn := dialReceiver(t, r)
	defer conn.Close()

	_, err := logscollector.NewLogsServiceClient(conn).Export(context.Background(), testLogsRequest("hello"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("code = %v, want Internal; err=%v", status.Code(err), err)
	}
}

func TestReceiver_TracesAndMetrics_ReturnEmptyResponses(t *testing.T) {
	r := startTestReceiver(t, func(context.Context, []*event.Event) error { return nil })
	conn := dialReceiver(t, r)
	defer conn.Close()

	if _, err := tracecollector.NewTraceServiceClient(conn).Export(context.Background(), &tracecollector.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("trace Export: %v", err)
	}
	if _, err := metricscollector.NewMetricsServiceClient(conn).Export(context.Background(), &metricscollector.ExportMetricsServiceRequest{}); err != nil {
		t.Fatalf("metrics Export: %v", err)
	}
}

func startTestReceiver(t *testing.T, submit SubmitFunc) *Receiver {
	t.Helper()
	r, err := New(Config{Listen: "127.0.0.1:0"}, submit, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Start(ctx) }()
	r.WaitReady()
	if err := r.ReadyError(); err != nil {
		cancel()
		t.Fatalf("ReadyError: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		_ = r.Stop(stopCtx)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Start returned error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("receiver did not stop")
		}
	})
	return r
}

func dialReceiver(t *testing.T, r *Receiver) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, r.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	return conn
}

func testLogsRequest(body string) *logscollector.ExportLogsServiceRequest {
	return &logscollector.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
				}},
			}},
		}},
	}
}
