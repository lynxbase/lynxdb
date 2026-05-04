package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/config"
	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func TestIntegration_ShippersList_RecordsBulkUserAgent(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := []byte(`{"index":{"_index":"logs"}}
{"message":"shipper registry hello"}
{"index":{"_index":"logs"}}
{"message":"shipper registry world"}
`)
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/_bulk", srv.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("User-Agent", "Filebeat/8.15.0 (linux; amd64)")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("bulk status = %d, want 200", resp.StatusCode)
	}

	assertShipperListed(t, srv, "filebeat", "8.15.0", "/_bulk", 2)
}

func TestIntegration_ShippersList_RecordsOTLPHTTPReceiver(t *testing.T) {
	ingestCfg := config.DefaultConfig().Ingest
	ingestCfg.OTLP.HTTPListen = "127.0.0.1:0"
	ingestCfg.OTLP.GRPCListen = ""
	ingestCfg.Staging.Enabled = false
	srv, cleanup := startTestServerWithConfig(t, Config{Ingest: ingestCfg})
	defer cleanup()

	body, err := proto.Marshal(&logscollector.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "shipper otlp http"}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/v1/logs", srv.otlpHTTPReceiver.Addr()), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "opentelemetry-collector-contrib/0.105.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("otlp status = %d, want 200", resp.StatusCode)
	}

	assertShipperListed(t, srv, "otelcol", "0.105.0", "/v1/logs", 1)
}

func assertShipperListed(t *testing.T, srv *Server, tool, version, endpoint string, eventsPerMin int64) {
	t.Helper()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/shippers", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("shippers status = %d, want 200", resp.StatusCode)
	}

	var out struct {
		Data []struct {
			Tool         string `json:"tool"`
			Version      string `json:"version"`
			Status       string `json:"status"`
			Endpoint     string `json:"endpoint"`
			EventsPerMin int64  `json:"events_per_min"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(out.Data) != 1 {
		t.Fatalf("shipper count = %d, want 1: %#v", len(out.Data), out.Data)
	}
	got := out.Data[0]
	if got.Tool != tool {
		t.Fatalf("shipper tool = %s, want %s", got.Tool, tool)
	}
	if version != "" && got.Version != version {
		t.Fatalf("shipper = %s %s, want %s %s", got.Tool, got.Version, tool, version)
	}
	if got.Status != "healthy" {
		t.Fatalf("status = %q, want healthy", got.Status)
	}
	if got.Endpoint != endpoint {
		t.Fatalf("endpoint = %q, want %s", got.Endpoint, endpoint)
	}
	if got.EventsPerMin != eventsPerMin {
		t.Fatalf("events_per_min = %d, want %d", got.EventsPerMin, eventsPerMin)
	}
}
