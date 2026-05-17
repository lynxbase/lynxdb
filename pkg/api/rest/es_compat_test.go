package rest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func postESBulk(t *testing.T, addr, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/_bulk", addr),
		"application/x-ndjson",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST _bulk: %v", err)
	}

	return resp
}

func decodeESBulkResponse(t *testing.T, resp *http.Response) esBulkResponse {
	t.Helper()
	defer resp.Body.Close()
	var result esBulkResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode bulk response: %v", err)
	}

	return result
}

func TestESBulk_BasicIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"hello","level":"info"}
{"index":{"_index":"logs"}}
{"message":"world","level":"error"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
	if result.Took < 0 {
		t.Fatalf("took: %d", result.Took)
	}
	for i, item := range result.Items {
		if item.Index == nil {
			t.Fatalf("item %d: Index is nil", i)
		}
		if item.Index.Status != 201 {
			t.Fatalf("item %d: status %d", i, item.Index.Status)
		}
		if item.Index.ID == "" {
			t.Fatalf("item %d: empty _id", i)
		}
		if item.Index.Result != "created" {
			t.Fatalf("item %d: result = %q, want created", i, item.Index.Result)
		}
	}

	// Verify ES _index routes to the physical LynxDB index.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM logs"}`)
	if n != 2 {
		t.Fatalf("logs events: got %d, want 2", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESBulk_UnprefixedRoute(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/_bulk", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader("{\"index\":{\"_index\":\"logs\"}}\n{\"message\":\"drop in\"}\n"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors || len(result.Items) != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Items[0].Index.Index != "logs" {
		t.Fatalf("_index = %q, want logs", result.Items[0].Index.Index)
	}
}

func TestESBulk_PathIndexFallback(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/path-index/_bulk", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader("{\"index\":{}}\n{\"message\":\"from path\"}\n"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Items[0].Index.Index != "path-index" {
		t.Fatalf("_index = %q, want path-index", result.Items[0].Index.Index)
	}
}

func TestESBulk_TargetIndexFallback(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp := postESBulk(t, srv.Addr(), "{\"index\":{}}\n{\"message\":\"from target\",\"target_index\":\"doc-target\"}\n")
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if result.Items[0].Index.Index != "doc-target" {
		t.Fatalf("_index = %q, want doc-target", result.Items[0].Index.Index)
	}

	time.Sleep(200 * time.Millisecond)
	events := queryEvents(t, srv.Addr(), `{"q":"FROM doc-target | table target_index | head 1"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if got := events[0]["target_index"]; got != "doc-target" {
		t.Fatalf("target_index = %v, want doc-target", got)
	}
}

func TestESBulk_DataStreamRouteUsesPathName(t *testing.T) {
	ingestCfg := config.DefaultConfig().Ingest
	ingestCfg.OTLP.HTTPListen = ""
	ingestCfg.OTLP.GRPCListen = ""
	srv, cleanup := startTestServerWithConfig(t, Config{Ingest: ingestCfg})
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/_data_stream/logs-generic-default/_bulk", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader("{\"index\":{\"_index\":\"ignored-meta\"}}\n{\"message\":\"from data stream\"}\n"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Items[0].Index.Index != "logs-generic-default" {
		t.Fatalf("_index = %q, want logs-generic-default", result.Items[0].Index.Index)
	}
}

func TestESBulk_CreateAction(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"create":{"_index":"logs"}}
{"message":"created doc"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(result.Items))
	}
	if result.Items[0].Create == nil {
		t.Fatal("expected Create action in response")
	}
	if result.Items[0].Create.Status != 201 {
		t.Fatalf("status: %d", result.Items[0].Create.Status)
	}
	if result.Items[0].Create.Result != "created" {
		t.Fatalf("result: got %q, want created", result.Items[0].Create.Result)
	}
}

func TestESBulk_UpdateRejected(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"ok"}
{"update":{"_index":"logs","_id":"123"}}
{"doc":{"message":"updated"}}
{"index":{"_index":"logs"}}
{"message":"also ok"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 3 {
		t.Fatalf("items: got %d, want 3", len(result.Items))
	}
	// First and third should succeed.
	if result.Items[0].Index == nil || result.Items[0].Index.Status != 201 {
		t.Fatalf("item 0: expected 201")
	}
	// Second (update) should fail.
	if result.Items[1].Index == nil || result.Items[1].Index.Status != 400 {
		t.Fatalf("item 1: expected 400, got %+v", result.Items[1])
	}
	if result.Items[1].Index.Error == nil || result.Items[1].Index.Error.Type != "action_request_validation_exception" {
		t.Fatalf("item 1: wrong error type")
	}
	// Third should succeed.
	if result.Items[2].Index == nil || result.Items[2].Index.Status != 201 {
		t.Fatalf("item 2: expected 201")
	}
}

func TestESBulk_DeleteRejected(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	// delete has no data line, next action/data pair should parse correctly.
	body := `{"delete":{"_index":"logs","_id":"abc"}}
{"index":{"_index":"logs"}}
{"message":"after delete"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
	if result.Items[0].Index == nil || result.Items[0].Index.Status != 400 {
		t.Fatalf("item 0: expected 400 for delete")
	}
	if result.Items[1].Index == nil || result.Items[1].Index.Status != 201 {
		t.Fatalf("item 1: expected 201")
	}
}

func TestESBulk_MalformedAction(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `not-json
{"message":"data line consumed"}
{"index":{"_index":"logs"}}
{"message":"ok"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
	if result.Items[0].Index.Status != 400 {
		t.Fatalf("item 0: expected 400")
	}
	if result.Items[1].Index.Status != 201 {
		t.Fatalf("item 1: expected 201")
	}
}

func TestESBulk_MalformedData(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
not-valid-json
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(result.Items))
	}
	if result.Items[0].Index.Status != 400 {
		t.Fatalf("expected 400")
	}
}

func TestESBulk_InvalidIndexNames(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	longIndex := strings.Repeat("a", 256)
	body := fmt.Sprintf(`{"index":{"_index":""}}
{"message":"empty"}
{"index":{"_index":"bad/name"}}
{"message":"slash"}
{"index":{"_index":"../escape"}}
{"message":"traversal"}
{"index":{"_index":"BadName"}}
{"message":"uppercase"}
{"index":{"_index":%q}}
{"message":"too long"}
{"index":{"_index":"valid-index"}}
{"message":"ok"}
`, longIndex)
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 6 {
		t.Fatalf("items: got %d, want 6", len(result.Items))
	}
	for i := 0; i < 5; i++ {
		if result.Items[i].Index == nil {
			t.Fatalf("item %d: Index is nil", i)
		}
		if result.Items[i].Index.Status != http.StatusBadRequest {
			t.Fatalf("item %d status = %d, want 400", i, result.Items[i].Index.Status)
		}
		if result.Items[i].Index.Error == nil || result.Items[i].Index.Error.Type != "invalid_index_name_exception" {
			t.Fatalf("item %d error = %+v, want invalid_index_name_exception", i, result.Items[i].Index.Error)
		}
	}
	if result.Items[5].Index == nil || result.Items[5].Index.Status != http.StatusCreated {
		t.Fatalf("item 5 = %+v, want created", result.Items[5])
	}

	time.Sleep(200 * time.Millisecond)
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM valid-index"}`); n != 1 {
		t.Fatalf("valid-index events: got %d, want 1", n)
	}
}

func TestESBulk_OrphanAction(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(result.Items))
	}
	if result.Items[0].Index.Status != 400 {
		t.Fatalf("expected 400")
	}
}

func TestESBulk_EmptyBody(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp := postESBulk(t, srv.Addr(), "")
	result := decodeESBulkResponse(t, resp)

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 0 {
		t.Fatalf("items: got %d, want 0", len(result.Items))
	}
}

func TestESBulk_TimestampMapping(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"@timestamp":"2026-02-14T14:00:00Z","message":"ts test"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 1 {
		t.Fatalf("items: got %d, want 1", len(result.Items))
	}
	if result.Items[0].Index.Status != 201 {
		t.Fatalf("status: %d", result.Items[0].Index.Status)
	}
}

func TestESBulk_TimestampEpoch(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"@timestamp":1739538000.123,"message":"epoch ts"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
}

func TestESBulk_IndexMapping(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"filebeat-2026.02.17"}}
{"message":"index test"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if result.Errors {
		t.Fatal("expected errors=false")
	}

	// Verify event was routed to the ES index, not main.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM filebeat-2026.02.17"}`)
	if n != 1 {
		t.Fatalf("filebeat-2026.02.17 events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESBulk_FilebeatPathMapsToSource(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"filebeat-2026.05.17"}}
{"@timestamp":"2026-05-17T10:00:00Z","message":"filebeat path","log":{"file":{"path":"/var/log/app/nginx_access.log"}}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if result.Errors {
		t.Fatal("expected errors=false")
	}

	time.Sleep(200 * time.Millisecond)
	events := queryEvents(t, srv.Addr(), `{"q":"FROM filebeat-2026.05.17 | table _source | head 1"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if got := events[0]["_source"]; got != "/var/log/app/nginx_access.log" {
		t.Fatalf("_source = %v, want /var/log/app/nginx_access.log", got)
	}
}

func TestESBulk_TopLevelSourceMapsToSource(t *testing.T) {
	ev := esDocToEventWithMapping(
		map[string]interface{}{"message": "top source", "source": "/var/log/app/app.log"},
		"filebeat-2026.05.17",
		esFieldMapping{TimeField: "@timestamp"},
	)
	if ev.Source != "/var/log/app/app.log" {
		t.Fatalf("Source = %q, want /var/log/app/app.log", ev.Source)
	}
	if ev.Index != "filebeat-2026.05.17" {
		t.Fatalf("Index = %q, want filebeat-2026.05.17", ev.Index)
	}
	if _, ok := ev.Fields["source"]; ok {
		t.Fatal("source user field should be consumed into Event.Source")
	}
}

func TestESBulk_LogFilePathPreservedAsField(t *testing.T) {
	ev := esDocToEventWithMapping(
		map[string]interface{}{
			"message": "filebeat path",
			"log": map[string]interface{}{
				"file": map[string]interface{}{
					"path": "/var/log/app/nginx_access.log",
				},
			},
		},
		"filebeat-2026.05.17",
		esFieldMapping{TimeField: "@timestamp"},
	)
	if ev.Source != "/var/log/app/nginx_access.log" {
		t.Fatalf("Source = %q, want /var/log/app/nginx_access.log", ev.Source)
	}
	path, ok := ev.Fields["log.file.path"]
	if !ok {
		t.Fatal("log.file.path field missing")
	}
	if got := path.AsString(); got != "/var/log/app/nginx_access.log" {
		t.Fatalf("log.file.path = %q, want /var/log/app/nginx_access.log", got)
	}
}

func TestESBulk_LogstashFormat_DateStripped(t *testing.T) {
	ev := esDocToEventWithMapping(
		map[string]interface{}{"message": "logstash format"},
		"fluent-bit-2026.05.04",
		esFieldMapping{TimeField: "@timestamp", StripLogstashDateSuffix: true},
	)
	if ev.Source != "" {
		t.Fatalf("Source = %q, want empty", ev.Source)
	}
	if ev.Index != "fluent-bit-2026.05.04" {
		t.Fatalf("Index = %q, want fluent-bit-2026.05.04", ev.Index)
	}
}

func TestESBulk_LogstashFormat_NoStripWhenDisabled(t *testing.T) {
	ev := esDocToEventWithMapping(
		map[string]interface{}{"message": "logstash format"},
		"fluent-bit-2026.05.04",
		esFieldMapping{TimeField: "@timestamp"},
	)
	if ev.Source != "" {
		t.Fatalf("Source = %q, want empty", ev.Source)
	}
	if ev.Index != "fluent-bit-2026.05.04" {
		t.Fatalf("Index = %q, want fluent-bit-2026.05.04", ev.Index)
	}
}

func TestESBulk_LogstashFormat_NonDateSuffixUnchanged(t *testing.T) {
	got := stripLogstashDateSuffix("fluent-bit-2026.99")
	if got != "fluent-bit-2026.99" {
		t.Fatalf("stripLogstashDateSuffix = %q, want original", got)
	}
}

func TestESBulk_NestedHost(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"nested host","host":{"name":"web-01"}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}

	// Verify event was ingested into its ES index.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM logs"}`)
	if n != 1 {
		t.Fatalf("logs events: got %d, want 1", n)
	}
}

func TestESBulk_FilebeatTargetIndexRoutesDocuments(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{}}
{"@timestamp":"2026-05-17T10:00:00Z","message":"apache access","target_index":"apache-access","log":{"file":{"path":"/var/log/app/apache_access.log"}}}
{"index":{}}
{"@timestamp":"2026-05-17T10:01:00Z","message":"nginx error","target_index":"nginx-error","log":{"file":{"path":"/var/log/app/nginx_error.log"}}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if got := result.Items[0].Index.Index; got != "apache-access" {
		t.Fatalf("item 0 _index = %q, want apache-access", got)
	}
	if got := result.Items[1].Index.Index; got != "nginx-error" {
		t.Fatalf("item 1 _index = %q, want nginx-error", got)
	}

	time.Sleep(200 * time.Millisecond)
	apache := queryEvents(t, srv.Addr(), `{"q":"FROM apache-access | table index, target_index, _source | head 10"}`)
	if len(apache) != 1 {
		t.Fatalf("apache-access events: got %d, want 1", len(apache))
	}
	if apache[0]["target_index"] != "apache-access" {
		t.Fatalf("apache target_index = %v, want apache-access", apache[0]["target_index"])
	}
	if apache[0]["_source"] != "/var/log/app/apache_access.log" {
		t.Fatalf("apache _source = %v, want /var/log/app/apache_access.log", apache[0]["_source"])
	}

	nginx := queryEvents(t, srv.Addr(), `{"q":"FROM nginx-error | table index, target_index, _source | head 10"}`)
	if len(nginx) != 1 {
		t.Fatalf("nginx-error events: got %d, want 1", len(nginx))
	}
	if nginx[0]["target_index"] != "nginx-error" {
		t.Fatalf("nginx target_index = %v, want nginx-error", nginx[0]["target_index"])
	}
	if nginx[0]["_source"] != "/var/log/app/nginx_error.log" {
		t.Fatalf("nginx _source = %v, want /var/log/app/nginx_error.log", nginx[0]["_source"])
	}
	bySource := queryEvents(t, srv.Addr(), `{"q":"FROM nginx-error | where _source=\"/var/log/app/nginx_error.log\" | table index, target_index, _source | head 10"}`)
	if len(bySource) != 1 {
		t.Fatalf("nginx-error _source filter events: got %d, want 1", len(bySource))
	}
	if bySource[0]["index"] != "nginx-error" {
		t.Fatalf("filtered index = %v, want nginx-error", bySource[0]["index"])
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESBulk_OtelCollectorElasticsearchTemplateIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"${target_index}"}}
{"@timestamp":"2026-05-17T10:00:00Z","body":"nginx access","target_index":"nginx-access","log.file.path":"/var/log/app/nginx_access.log"}
{"index":{"_index":"prefix-${target_index}"}}
{"@timestamp":"2026-05-17T10:01:00Z","body":"postgres","target_index":"postgres","log.file.path":"/var/log/app/postgres.log"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if got := result.Items[0].Index.Index; got != "nginx-access" {
		t.Fatalf("item 0 _index = %q, want nginx-access", got)
	}
	if got := result.Items[1].Index.Index; got != "prefix-postgres" {
		t.Fatalf("item 1 _index = %q, want prefix-postgres", got)
	}

	time.Sleep(200 * time.Millisecond)
	nginx := queryEvents(t, srv.Addr(), `{"q":"FROM nginx-access | table target_index, _source | head 1"}`)
	if len(nginx) != 1 {
		t.Fatalf("nginx-access events: got %d, want 1", len(nginx))
	}
	if nginx[0]["target_index"] != "nginx-access" {
		t.Fatalf("nginx target_index = %v, want nginx-access", nginx[0]["target_index"])
	}
	if nginx[0]["_source"] != "/var/log/app/nginx_access.log" {
		t.Fatalf("nginx _source = %v, want /var/log/app/nginx_access.log", nginx[0]["_source"])
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM prefix-postgres"}`); n != 1 {
		t.Fatalf("prefix-postgres events: got %d, want 1", n)
	}
}

func TestESBulk_SourcePathFilterBeforeParseCombined(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"nginx-access"}}
{"@timestamp":"2026-05-17T21:45:56Z","message":"192.168.1.203 - - [17/May/2026:21:45:56 +0000] \"OPTIONS /static/style.css HTTP/1.1\" 404 109 \"https://google.com/search?q=test\" \"kube-probe/1.30\" 0.752 0.747","target_index":"nginx-access","log":{"file":{"path":"/var/log/app/nginx_access.log"}}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}

	time.Sleep(200 * time.Millisecond)
	events := queryEvents(t, srv.Addr(), `{"q":"FROM nginx-access | where _source=\"/var/log/app/nginx_access.log\" | parse combined(message) | limit 1"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if events[0]["index"] != "nginx-access" {
		t.Fatalf("index = %v, want nginx-access", events[0]["index"])
	}
	if events[0]["_source"] != "/var/log/app/nginx_access.log" {
		t.Fatalf("_source = %v, want /var/log/app/nginx_access.log", events[0]["_source"])
	}
	if events[0]["method"] != "OPTIONS" {
		t.Fatalf("method = %v, want OPTIONS", events[0]["method"])
	}
	if fmt.Sprint(events[0]["status"]) != "404" {
		t.Fatalf("status = %v, want 404", events[0]["status"])
	}
}

func TestESBulk_OtelCollectorAttributesTargetIndexFallback(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{}}
{"body":"postgres","attributes":{"target_index":"postgres","log.file.path":"/var/log/app/postgres.log"}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if got := result.Items[0].Index.Index; got != "postgres" {
		t.Fatalf("_index = %q, want postgres", got)
	}

	time.Sleep(200 * time.Millisecond)
	events := queryEvents(t, srv.Addr(), `{"q":"FROM postgres | table target_index, _source | head 1"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if events[0]["target_index"] != "postgres" {
		t.Fatalf("target_index = %v, want postgres", events[0]["target_index"])
	}
	if events[0]["_source"] != "/var/log/app/postgres.log" {
		t.Fatalf("_source = %v, want /var/log/app/postgres.log", events[0]["_source"])
	}
}

func TestESBulk_FluentdTargetIndexKeyRoutesDocuments(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{}}
{"message":"fluentd nginx","target_index":"nginx-access","tag":"nginx-access"}
{"index":{"_index":"postgres"}}
{"message":"fluentd postgres","tag":"postgres"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if got := result.Items[0].Index.Index; got != "nginx-access" {
		t.Fatalf("item 0 _index = %q, want nginx-access", got)
	}
	if got := result.Items[1].Index.Index; got != "postgres" {
		t.Fatalf("item 1 _index = %q, want postgres", got)
	}

	time.Sleep(200 * time.Millisecond)
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM nginx-access"}`); n != 1 {
		t.Fatalf("nginx-access events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM postgres"}`); n != 1 {
		t.Fatalf("postgres events: got %d, want 1", n)
	}
}

func TestESBulk_LargeBatch(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	var buf bytes.Buffer
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&buf, "{\"index\":{\"_index\":\"bench\"}}\n")
		fmt.Fprintf(&buf, "{\"message\":\"event %d\",\"seq\":%d}\n", i, i)
	}

	resp := postESBulk(t, srv.Addr(), buf.String())
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 5000 {
		t.Fatalf("items: got %d, want 5000", len(result.Items))
	}
	for _, item := range result.Items {
		if item.Index == nil || item.Index.Status != 201 {
			t.Fatal("expected all 201")
		}
	}
}

func TestESBulk_PartialFailure(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"ok1"}
{"index":{"_index":"logs"}}
not-json
{"index":{"_index":"logs"}}
{"message":"ok2"}
{"index":{"_index":"logs"}}
also-not-json
{"index":{"_index":"logs"}}
{"message":"ok3"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 5 {
		t.Fatalf("items: got %d, want 5", len(result.Items))
	}

	successes := 0
	failures := 0
	for _, item := range result.Items {
		if item.Index != nil {
			if item.Index.Status == 201 {
				successes++
			} else {
				failures++
			}
		}
	}
	if successes != 3 {
		t.Fatalf("successes: got %d, want 3", successes)
	}
	if failures != 2 {
		t.Fatalf("failures: got %d, want 2", failures)
	}
}

func TestESBulk_ClientProvidedID(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs","_id":"my-custom-id"}}
{"message":"custom id"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if result.Items[0].Index.ID != "my-custom-id" {
		t.Fatalf("_id: got %q, want %q", result.Items[0].Index.ID, "my-custom-id")
	}
}

func TestESBulk_BlankLinesBetweenPairs(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"one"}

{"index":{"_index":"logs"}}
{"message":"two"}

`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
}

func TestESIndexDoc_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"message":"hello doc","level":"info"}`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/logs/_doc", srv.Addr()),
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, b)
	}

	var result esIndexDocResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.ID == "" {
		t.Fatal("empty _id")
	}
	if result.Index != "logs" {
		t.Fatalf("_index: got %q, want %q", result.Index, "logs")
	}
	if result.Result != "created" {
		t.Fatalf("result: got %q", result.Result)
	}

	// Verify queryable from the route index.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM logs"}`)
	if n != 1 {
		t.Fatalf("logs events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESIndexDoc_InvalidJSON(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/logs/_doc", srv.Addr()),
		"application/json",
		strings.NewReader("not-json"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestESIndexDoc_InvalidIndexName(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/BadName/_doc", srv.Addr()),
		"application/json",
		strings.NewReader(`{"message":"invalid index"}`),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, b)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	errObj, ok := result["error"].(map[string]interface{})
	if !ok {
		t.Fatalf("error = %#v, want object", result["error"])
	}
	if got := errObj["type"]; got != "invalid_index_name_exception" {
		t.Fatalf("error.type = %v, want invalid_index_name_exception", got)
	}
}

func TestESIndexDoc_PathIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"message":"myapp event"}`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/myapp/_doc", srv.Addr()),
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	// Verify queryable from the path index.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM myapp"}`)
	if n != 1 {
		t.Fatalf("myapp events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESClusterInfo_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es/", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result esClusterInfoResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if !strings.HasPrefix(result.Name, "lynxdb-") {
		t.Fatalf("name: %q", result.Name)
	}
	if result.ClusterName != "lynxdb" {
		t.Fatalf("cluster_name: %q", result.ClusterName)
	}
	if result.Version.Number != "8.15.0" {
		t.Fatalf("version.number: %q", result.Version.Number)
	}
	if result.Version.MinimumWireCompatibilityVersion != "7.17.0" {
		t.Fatalf("minimum_wire_compatibility_version: %q", result.Version.MinimumWireCompatibilityVersion)
	}
}

func TestESClusterInfo_NoTrailingSlash(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result esClusterInfoResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if !strings.HasPrefix(result.Name, "lynxdb-") {
		t.Fatalf("name: %q", result.Name)
	}
}

func TestESClusterInfo_UnprefixedRoot(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/", srv.Addr()))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product: got %q, want Elasticsearch", got)
	}
	var result esClusterInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.Version.Number != "8.15.0" {
		t.Fatalf("version.number: %q", result.Version.Number)
	}
}

func TestESClusterInfo_UnprefixedHead(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, err := http.NewRequest(http.MethodHead, fmt.Sprintf("http://%s/", srv.Addr()), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("HEAD: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.ContentLength > 0 {
		t.Fatalf("content length = %d, want no body", resp.ContentLength)
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product: got %q, want Elasticsearch", got)
	}
}

func TestESBulk_QueryAfterIngest(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	var buf bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&buf, "{\"index\":{\"_index\":\"nginx\"}}\n")
		fmt.Fprintf(&buf, "{\"message\":\"request %d\",\"level\":\"error\"}\n", i)
	}

	resp := postESBulk(t, srv.Addr(), buf.String())
	result := decodeESBulkResponse(t, resp)
	if result.Errors {
		t.Fatal("ingest failed")
	}

	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM nginx"}`)
	if n != 10 {
		t.Fatalf("nginx events: got %d, want 10", n)
	}
}

func TestESBulk_GzipCompressed(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"@timestamp":"2026-03-05T10:00:00Z","message":"gzip hello"}
{"index":{"_index":"logs"}}
{"@timestamp":"2026-03-05T10:01:00Z","message":"gzip world"}
`
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte(body))
	gz.Close()

	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/es/_bulk", srv.Addr()),
		&buf,
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, b)
	}

	var result esBulkResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
	for i, item := range result.Items {
		if item.Index == nil || item.Index.Status != 201 {
			t.Fatalf("item %d: expected 201", i)
		}
	}

	// Verify events queryable from the ES index.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM logs"}`)
	if n != 2 {
		t.Fatalf("logs events: got %d, want 2", n)
	}
}

func TestESBulk_ZstdCompressed(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := []byte(`{"index":{"_index":"logs"}}
{"@timestamp":"2026-03-05T10:00:00Z","message":"zstd hello"}
{"index":{"_index":"logs"}}
{"@timestamp":"2026-03-05T10:01:00Z","message":"zstd world"}
`)
	req, err := http.NewRequest("POST",
		fmt.Sprintf("http://%s/_bulk", srv.Addr()),
		encodeTestBody(t, "zstd", body),
	)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "zstd")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, b)
	}

	var result esBulkResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
}

func TestESBulk_InvalidGzip(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/es/_bulk", srv.Addr()),
		strings.NewReader("not-gzip-data"),
	)
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("status: %d, want 400", resp.StatusCode)
	}
}

func TestESStub_ILMPolicy(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es/_ilm/policy/filebeat", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusNotFound)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected empty object, got %v", result)
	}

	if h := resp.Header.Get("X-Elastic-Product"); h != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product: got %q, want Elasticsearch", h)
	}
}

func TestESStub_IndexTemplate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es/_index_template/filebeat-8.11.0", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if templates, ok := result["index_templates"].([]interface{}); !ok || len(templates) != 0 {
		t.Fatalf("index_templates = %#v, want empty array", result["index_templates"])
	}
}

func TestESStub_ExpandedProbeRoutes(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	tests := []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/_cluster/health", http.StatusOK},
		{http.MethodGet, "/_search", http.StatusOK},
		{http.MethodGet, "/_cat/indices", http.StatusOK},
		{http.MethodPut, "/_data_stream/logs-generic-default", http.StatusOK},
		{http.MethodHead, "/logs-generic-default", http.StatusOK},
		{http.MethodPost, "/_security/user/_authenticate", http.StatusOK},
		{http.MethodGet, "/api/v1/es/_cluster/health", http.StatusOK},
		{http.MethodHead, "/api/v1/es/logs-generic-default", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req, _ := http.NewRequest(tt.method, fmt.Sprintf("http://%s%s", srv.Addr(), tt.path), nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.status {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.status)
			}
			if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
				t.Fatalf("X-Elastic-Product = %q", got)
			}
		})
	}
}

func TestESStub_IndexHeadProbe_DoesNotShadowTopLevelRoutes(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	for _, path := range []string{"/health", "/metrics"} {
		t.Run(path, func(t *testing.T) {
			req, _ := http.NewRequest(http.MethodHead, fmt.Sprintf("http://%s%s", srv.Addr(), path), nil)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if got := resp.Header.Get("X-Elastic-Product"); got != "" {
				t.Fatalf("X-Elastic-Product = %q, want empty", got)
			}
		})
	}
}

func TestESStub_IngestPipeline(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es/_ingest/pipeline/filebeat-test", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestESStub_License(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/_license", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	license := result["license"].(map[string]interface{})
	if license["status"] != "active" || license["type"] != "basic" {
		t.Fatalf("license = %#v, want active basic", license)
	}
}

func TestESStub_IndexTemplatePut(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest(http.MethodPut,
		fmt.Sprintf("http://%s/_index_template/filebeat", srv.Addr()),
		strings.NewReader(`{"index_patterns":["filebeat-*"]}`),
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["acknowledged"] != true {
		t.Fatalf("acknowledged = %#v, want true", result["acknowledged"])
	}
}

func TestESBulk_DisabledByReload_Returns503(t *testing.T) {
	runtimeCfg := config.DefaultConfig()
	srv, cleanup := startTestServerWithConfig(t, Config{
		RuntimeConfig: runtimeCfg,
		Ingest:        runtimeCfg.Ingest,
	})
	defer cleanup()

	updated := *runtimeCfg
	updated.Ingest.ESCompat.Enabled = false
	if restart, err := srv.ReloadConfig(&updated); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	} else if len(restart) != 0 {
		t.Fatalf("restartRequired = %v, want none", restart)
	}

	resp, err := http.Post(
		fmt.Sprintf("http://%s/_bulk", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader(`{"index":{"_index":"logs"}}
{"message":"hello"}
`),
	)
	if err != nil {
		t.Fatalf("POST _bulk: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
}

func TestESStub_PutIndex(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	req, _ := http.NewRequest("PUT",
		fmt.Sprintf("http://%s/api/v1/es/myindex", srv.Addr()),
		strings.NewReader(`{"settings":{}}`),
	)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status: %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
}

func TestESClusterInfo_XElasticProductHeader(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/es/", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if h := resp.Header.Get("X-Elastic-Product"); h != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product: got %q, want Elasticsearch", h)
	}
}

func TestESBulk_MsgFieldParam(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"@timestamp":"2026-03-05T10:00:00Z","message":"hello msg field","extra":"data"}
`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/_bulk?_msg_field=message", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result esBulkResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Errors {
		t.Fatal("expected errors=false")
	}
}

func TestESBulk_TimeFieldParam(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"ts":"2026-03-05T10:00:00Z","message":"custom time field"}
`
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/es/_bulk?_time_field=ts", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var result esBulkResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.Errors {
		t.Fatal("expected errors=false")
	}
}

func TestESIndexDoc_GzipCompressed(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"message":"gzip doc","level":"info"}`
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte(body))
	gz.Close()

	req, _ := http.NewRequest("POST",
		fmt.Sprintf("http://%s/api/v1/es/logs/_doc", srv.Addr()),
		&buf,
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status: %d, body: %s", resp.StatusCode, b)
	}

	var result esIndexDocResponse
	json.NewDecoder(resp.Body).Decode(&result)
	if result.ID == "" {
		t.Fatal("empty _id")
	}
}

// postQuery is a helper for querying.
func postQuery(t *testing.T, addr, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(
		fmt.Sprintf("http://%s/api/v1/query", addr),
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}

	return resp
}

func queryEvents(t *testing.T, addr, body string) []map[string]interface{} {
	t.Helper()
	resp := postQuery(t, addr, body)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var qr map[string]interface{}
	if err := json.Unmarshal(raw, &qr); err != nil {
		t.Fatalf("decode query response: %v (body: %s)", err, raw)
	}
	data, ok := qr["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data in query response: %s", raw)
	}
	rawEvents, ok := data["events"].([]interface{})
	if !ok {
		t.Fatalf("missing events in query response: %s", raw)
	}
	events := make([]map[string]interface{}, 0, len(rawEvents))
	for _, rawEvent := range rawEvents {
		event, ok := rawEvent.(map[string]interface{})
		if !ok {
			t.Fatalf("event has type %T, want object", rawEvent)
		}
		events = append(events, event)
	}

	return events
}

// queryEventCount runs a query and returns the count of events.
func queryEventCount(t *testing.T, addr, body string) int {
	t.Helper()
	resp := postQuery(t, addr, body)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var qr map[string]interface{}
	if err := json.Unmarshal(raw, &qr); err != nil {
		t.Fatalf("decode query response: %v (body: %s)", err, raw)
	}
	data, ok := qr["data"].(map[string]interface{})
	if !ok {
		return 0
	}
	events, ok := data["events"].([]interface{})
	if !ok {
		return 0
	}

	return len(events)
}
