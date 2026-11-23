package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadNDJSON_Basic(t *testing.T) {
	input := `{"message":"line1","level":"info"}
{"message":"line2","level":"error"}
{"__meta":{"total":2,"took_ms":5}}
`
	var lines []json.RawMessage

	meta, err := readNDJSON(strings.NewReader(input), func(line json.RawMessage) error {
		lines = append(lines, line)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if meta == nil {
		t.Fatal("meta is nil")
	}
	if meta.Total != 2 {
		t.Errorf("meta.Total = %d, want 2", meta.Total)
	}
	if meta.TookMS != 5 {
		t.Errorf("meta.TookMS = %d, want 5", meta.TookMS)
	}
}

func TestReadNDJSON_StreamError(t *testing.T) {
	input := `{"message":"ok"}
{"__error":{"code":"STREAM_ERROR","message":"scan failed"}}
`
	var lines []json.RawMessage

	_, err := readNDJSON(strings.NewReader(input), func(line json.RawMessage) error {
		lines = append(lines, line)

		return nil
	})
	if err == nil {
		t.Fatal("expected error")
	}

	if !strings.Contains(err.Error(), "scan failed") {
		t.Errorf("error = %q, want to contain 'scan failed'", err.Error())
	}
	if len(lines) != 1 {
		t.Errorf("got %d lines before error, want 1", len(lines))
	}
}

func TestReadNDJSON_EmptyLines(t *testing.T) {
	input := `
{"message":"line1"}

{"message":"line2"}

{"__meta":{"total":2,"scanned":50,"took_ms":1}}
`
	var count int

	meta, err := readNDJSON(strings.NewReader(input), func(_ json.RawMessage) error {
		count++

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	if meta == nil || meta.Total != 2 {
		t.Errorf("meta = %+v", meta)
	}
}

func TestQueryStream_StripsUnsupportedFields(t *testing.T) {
	var rawBody map[string]interface{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query/stream" {
			t.Fatalf("path = %q, want /api/v1/query/stream", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := json.Unmarshal(body, &rawBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"message\":\"ok\"}\n{\"__meta\":{\"total\":1,\"took_ms\":1}}\n")
	}))
	defer srv.Close()

	c := NewClient(WithBaseURL(srv.URL))
	meta, err := c.QueryStream(context.Background(), QueryRequest{
		Q:      "source=main | head 10",
		Limit:  10,
		Offset: 20,
		Format: "json",
	}, func(_ json.RawMessage) error {
		return nil
	})
	if err != nil {
		t.Fatalf("QueryStream: %v", err)
	}
	if meta == nil || meta.Total != 1 {
		t.Fatalf("meta = %+v, want total=1", meta)
	}
	if _, ok := rawBody["limit"]; ok {
		t.Fatalf("request body unexpectedly included limit: %v", rawBody)
	}
	if _, ok := rawBody["offset"]; ok {
		t.Fatalf("request body unexpectedly included offset: %v", rawBody)
	}
	if _, ok := rawBody["format"]; ok {
		t.Fatalf("request body unexpectedly included format: %v", rawBody)
	}
	if _, ok := rawBody["profile"]; ok {
		t.Fatalf("request body unexpectedly included profile: %v", rawBody)
	}
	if got := rawBody["q"]; got != "source=main | head 10" {
		t.Fatalf("q = %v, want query text", got)
	}
}
