package rest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	logscollector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
)

// scopedKeys creates a key with the ingest scope and one with the query scope.
func scopedKeys(t *testing.T, ks *auth.KeyStore) (ingest, query *auth.CreatedKey) {
	t.Helper()
	ingest, err := ks.CreateKeyWithOpts(auth.CreateKeyOpts{Name: "shipper", Scope: auth.ScopeIngest})
	if err != nil {
		t.Fatal(err)
	}
	query, err = ks.CreateKeyWithOpts(auth.CreateKeyOpts{Name: "dashboards", Scope: auth.ScopeQuery})
	if err != nil {
		t.Fatal(err)
	}

	return ingest, query
}

// send makes a request with an optional Authorization header and returns
// the status and body of the response.
func send(t *testing.T, method, url, authorization, body string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, resp.Header, string(out)
}

func TestAuth_SplunkHECChecksKeys(t *testing.T) {
	ingestCfg := config.DefaultConfig().Ingest
	ingestCfg.OTLP.HTTPListen = ""
	srv, ks, rootToken, cleanup := startAuthServerWithConfig(t, Config{Ingest: ingestCfg})
	defer cleanup()
	ingest, query := scopedKeys(t, ks)
	base := "http://" + srv.Addr()
	event := `{"event":"hello"}`

	tests := []struct {
		name          string
		method, path  string
		authorization string
		body          string
		status        int
		code          float64
	}{
		{"no header", "POST", "/services/collector/event", "", event, http.StatusUnauthorized, 2},
		{"bogus token", "POST", "/services/collector/event", "Splunk bogus", event, http.StatusUnauthorized, 3},
		{"key in another scheme", "POST", "/services/collector/event", "Bearer " + ingest.Token, event, http.StatusUnauthorized, 2},
		{"query key", "POST", "/services/collector/event", "Splunk " + query.Token, event, http.StatusForbidden, 1},
		{"ingest key", "POST", "/services/collector/event", "Splunk " + ingest.Token, event, http.StatusOK, 0},
		{"ingest key on the bare route", "POST", "/services/collector", "Splunk " + ingest.Token, event, http.StatusOK, 0},
		{"root key on raw", "POST", "/services/collector/raw", "Splunk " + rootToken, "raw line", http.StatusOK, 0},
		{"bogus token on ack", "POST", "/services/collector/ack", "Splunk bogus", `{"acks":[1]}`, http.StatusUnauthorized, 3},
		{"health without a key", "GET", "/services/collector/health", "", "", http.StatusOK, 17},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, _, body := send(t, tt.method, base+tt.path, tt.authorization, tt.body)
			if status != tt.status {
				t.Fatalf("status = %d, want %d, body %s", status, tt.status, body)
			}
			var resp struct {
				Code float64 `json:"code"`
			}
			if err := json.Unmarshal([]byte(body), &resp); err != nil || resp.Code != tt.code {
				t.Fatalf("body = %s, want code %v", body, tt.code)
			}
		})
	}
}

func TestAuth_ElasticsearchRootRoutesCheckKeys(t *testing.T) {
	srv, ks, _, cleanup := startAuthServer(t)
	defer cleanup()
	ingest, query := scopedKeys(t, ks)
	base := "http://" + srv.Addr()
	bulk := "{\"index\":{\"_index\":\"logs\"}}\n{\"message\":\"hi\"}\n"
	basic := func(token string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte("elastic:"+token))
	}
	apiKey := func(id, token string) string {
		return "ApiKey " + base64.StdEncoding.EncodeToString([]byte(id+":"+token))
	}

	tests := []struct {
		name          string
		method, path  string
		authorization string
		body          string
		status        int
	}{
		{"bulk without a key", "POST", "/_bulk", "", bulk, http.StatusUnauthorized},
		{"bulk with a bogus key", "POST", "/_bulk", "Bearer lynx_ak_bogus", bulk, http.StatusUnauthorized},
		{"bulk with Bearer", "POST", "/_bulk", "Bearer " + ingest.Token, bulk, http.StatusOK},
		{"bulk with ApiKey", "POST", "/_bulk", apiKey(ingest.ID, ingest.Token), bulk, http.StatusOK},
		{"bulk with Basic", "POST", "/_bulk", basic(ingest.Token), bulk, http.StatusOK},
		{"bulk with a query key", "POST", "/_bulk", "Bearer " + query.Token, bulk, http.StatusForbidden},
		{"index bulk with ApiKey", "POST", "/logs/_bulk", apiKey(ingest.ID, ingest.Token), bulk, http.StatusOK},
		{"data stream bulk without a key", "POST", "/_data_stream/logs-app/_bulk", "", bulk, http.StatusUnauthorized},
		{"doc with an ingest key", "POST", "/logs/_doc", "Bearer " + ingest.Token, `{"message":"hi"}`, http.StatusCreated},
		{"doc with a query key", "POST", "/logs/_doc", "Bearer " + query.Token, `{"message":"hi"}`, http.StatusForbidden},
		{"handshake without a key", "GET", "/", "", "", http.StatusUnauthorized},
		{"handshake with a query key", "GET", "/", "Bearer " + query.Token, "", http.StatusOK},
		{"index probe without a key", "HEAD", "/logs-generic-default", "", "", http.StatusUnauthorized},
		{"index probe with a key", "HEAD", "/logs-generic-default", basic(query.Token), "", http.StatusOK},
		{"stub without a key", "GET", "/_cluster/health", "", "", http.StatusUnauthorized},
		{"stub with a key", "GET", "/_cluster/health", "Bearer " + query.Token, "", http.StatusOK},
		{"unknown path without a key", "GET", "/nothing/here", "", "", http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, header, body := send(t, tt.method, base+tt.path, tt.authorization, tt.body)
			if status != tt.status {
				t.Fatalf("status = %d, want %d, body %s", status, tt.status, body)
			}
			if got := header.Get("X-Elastic-Product"); tt.path != "/nothing/here" && got != "Elasticsearch" {
				t.Fatalf("X-Elastic-Product = %q, want Elasticsearch", got)
			}
			if status == http.StatusUnauthorized {
				if !strings.HasPrefix(header.Get("WWW-Authenticate"), "Basic ") {
					t.Fatalf("WWW-Authenticate = %q, want a Basic challenge", header.Values("WWW-Authenticate"))
				}
				if tt.method != "HEAD" && !strings.Contains(body, "security_exception") {
					t.Fatalf("body = %s, want a security_exception", body)
				}
			}
		})
	}
}

func TestAuth_ProbesStayOpen(t *testing.T) {
	srv, _, _, cleanup := startAuthServer(t)
	defer cleanup()

	for _, path := range []string{"/health", "/metrics"} {
		status, _, body := send(t, "GET", "http://"+srv.Addr()+path, "", "")
		if status != http.StatusOK {
			t.Errorf("GET %s = %d, want 200, body %s", path, status, body)
		}
	}
}

func TestAuth_OTLPListenersCheckKeys(t *testing.T) {
	ingestCfg := config.DefaultConfig().Ingest
	ingestCfg.OTLP.HTTPListen = "127.0.0.1:0"
	ingestCfg.OTLP.GRPCListen = "127.0.0.1:0"
	srv, ks, _, cleanup := startAuthServerWithConfig(t, Config{Ingest: ingestCfg})
	defer cleanup()
	ingest, query := scopedKeys(t, ks)

	logs := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":"otlp with a key"}}]}]}]}`
	url := "http://" + srv.OTLPHTTPAddr() + "/v1/logs"
	for _, tt := range []struct {
		name          string
		authorization string
		status        int
	}{
		{"no key", "", http.StatusUnauthorized},
		{"bogus key", "Bearer bogus", http.StatusUnauthorized},
		{"query key", "Bearer " + query.Token, http.StatusForbidden},
		{"ingest key", "Bearer " + ingest.Token, http.StatusOK},
	} {
		t.Run("http "+tt.name, func(t *testing.T) {
			status, _, body := send(t, "POST", url, tt.authorization, logs)
			if status != tt.status {
				t.Fatalf("status = %d, want %d, body %s", status, tt.status, body)
			}
		})
	}

	conn, err := grpc.NewClient("passthrough:///"+srv.OTLPGRPCAddr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := logscollector.NewLogsServiceClient(conn)
	req := &logscollector.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "otlp grpc with a key"}},
		}}}},
	}}}
	for _, tt := range []struct {
		name          string
		authorization string
		code          codes.Code
	}{
		{"no key", "", codes.Unauthenticated},
		{"query key", "Bearer " + query.Token, codes.PermissionDenied},
		{"ingest key", "Bearer " + ingest.Token, codes.OK},
	} {
		t.Run("grpc "+tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.authorization != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, "authorization", tt.authorization)
			}
			_, err := client.Export(ctx, req, grpc.WaitForReady(true))
			if got := status.Code(err); got != tt.code {
				t.Fatalf("code = %v, want %v (%v)", got, tt.code, err)
			}
		})
	}

	srv.syncForRead()
	if got := storedRows(srv); got != 2 {
		t.Fatalf("stored rows = %d, want the two sent with an ingest key", got)
	}
}

func TestRoutes_PrefixedDuplicatesAreGone(t *testing.T) {
	srv, cleanup := startHECTestServer(t)
	defer cleanup()

	for _, tt := range []struct{ method, path string }{
		{"POST", "/api/v1/ingest/hec"},
		{"POST", "/api/v1/es/_bulk"},
		{"POST", "/api/v1/es/logs/_doc"},
		{"GET", "/api/v1/es"},
		{"GET", "/api/v1/es/_cluster/health"},
	} {
		if status, _, _ := send(t, tt.method, "http://"+srv.Addr()+tt.path, "", ""); status != http.StatusNotFound && status != http.StatusMethodNotAllowed {
			t.Errorf("%s %s = %d, want 404 or 405", tt.method, tt.path, status)
		}
	}
}
