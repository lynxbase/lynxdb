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

// esClusterInfo is the handshake document of the Elasticsearch root route.
type esClusterInfo struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	Version     struct {
		Number                          string `json:"number"`
		MinimumWireCompatibilityVersion string `json:"minimum_wire_compatibility_version"`
	} `json:"version"`
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

	n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`)
	if n != 2 {
		t.Fatalf("logs events: got %d, want 2", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESBulk_DurableBeforeSuccess(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{DataDir: t.TempDir()})
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"durable","level":"info"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	ix, ok := indexStats(srv, "logs")
	if !ok || ix.Rows != 1 || ix.Parts == 0 {
		t.Fatalf("logs after successful bulk: %+v, want 1 row written", ix)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 1 {
		t.Fatalf("logs events immediately after bulk: got %d, want 1", n)
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
		strings.NewReader("{\"index\":{}}\n{\"message\":\"from path\"}\n{\"index\":{\"_index\":\"action-index\"}}\n{\"message\":\"from action\"}\n"),
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
	if result.Items[1].Index.Index != "action-index" {
		t.Fatalf("_index = %q, want action-index", result.Items[1].Index.Index)
	}
	for _, index := range []string{"path-index", "action-index"} {
		if n := queryEventCount(t, srv.Addr(), fmt.Sprintf(`{"q":"source=%s"}`, index)); n != 1 {
			t.Fatalf("%s events: got %d, want 1", index, n)
		}
	}
}

func TestESBulk_DataStreamRouteUsesPathName(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/_data_stream/logs-generic-default/_bulk", srv.Addr()),
		"application/x-ndjson",
		strings.NewReader("{\"create\":{}}\n{\"message\":\"from data stream\"}\n{\"create\":{\"_index\":\"named-meta\"}}\n{\"message\":\"from action\"}\n"),
	)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors || len(result.Items) != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.Items[0].Create.Index != "logs-generic-default" {
		t.Fatalf("_index = %q, want logs-generic-default", result.Items[0].Create.Index)
	}
	if result.Items[1].Create.Index != "named-meta" {
		t.Fatalf("_index = %q, want named-meta", result.Items[1].Create.Index)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs-generic-default"}`); n != 1 {
		t.Fatalf("logs-generic-default events: got %d, want 1", n)
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
	if result.Items[0].Index == nil || result.Items[0].Index.Status != 201 {
		t.Fatalf("item 0: expected 201")
	}
	if result.Items[1].Index == nil || result.Items[1].Index.Status != 400 {
		t.Fatalf("item 1: expected 400, got %+v", result.Items[1])
	}
	if result.Items[1].Index.Error == nil || result.Items[1].Index.Error.Type != "action_request_validation_exception" {
		t.Fatalf("item 1: wrong error type")
	}
	if result.Items[2].Index == nil || result.Items[2].Index.Status != 201 {
		t.Fatalf("item 2: expected 201")
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 2 {
		t.Fatalf("logs events: got %d, want 2 (the update's document is not stored)", n)
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
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 1 {
		t.Fatalf("logs events: got %d, want 1", n)
	}
}

func TestESBulk_MalformedData(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
not-valid-json
{"index":{"_index":"logs"}}
["an","array"]
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if !result.Errors {
		t.Fatal("expected errors=true")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}
	for i, item := range result.Items {
		if item.Index.Status != 400 {
			t.Fatalf("item %d: status %d, want 400", i, item.Index.Status)
		}
		if item.Index.Error == nil || item.Index.Error.Type != "mapper_parsing_exception" {
			t.Fatalf("item %d: error = %+v, want mapper_parsing_exception", i, item.Index.Error)
		}
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 0 {
		t.Fatalf("logs events: got %d, want 0", n)
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

	if n := queryEventCount(t, srv.Addr(), `{"q":"source=valid-index"}`); n != 1 {
		t.Fatalf("valid-index events: got %d, want 1", n)
	}
	if names := srv.DB().Names(); len(names) != 1 || names[0] != "valid-index" {
		t.Fatalf("indexes = %v, want only valid-index", names)
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

func TestESBulk_TimestampIsEventTime(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"@timestamp":"2026-02-14T14:00:00Z","message":"ts test"}
{"index":{"_index":"logs"}}
{"timestamp":"2026-02-14T15:00:00Z","message":"timestamp test"}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)

	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if len(result.Items) != 2 {
		t.Fatalf("items: got %d, want 2", len(result.Items))
	}

	events := queryEvents(t, srv.Addr(), `{"q":"source=logs | sort _time"}`)
	if len(events) != 2 {
		t.Fatalf("events: got %d, want 2", len(events))
	}
	if events[0]["_time"] != "2026-02-14T14:00:00Z" || events[0]["@timestamp"] != "2026-02-14T14:00:00Z" {
		t.Fatalf("event 0 = %v, want _time and @timestamp 2026-02-14T14:00:00Z", events[0])
	}
	if events[1]["_time"] != "2026-02-14T15:00:00Z" || events[1]["timestamp"] != "2026-02-14T15:00:00Z" {
		t.Fatalf("event 1 = %v, want _time and timestamp 2026-02-14T15:00:00Z", events[1])
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

	n := queryEventCount(t, srv.Addr(), `{"q":"source=filebeat-2026.02.17"}`)
	if n != 1 {
		t.Fatalf("filebeat-2026.02.17 events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=main"}`); n != 0 {
		t.Fatalf("main events: got %d, want 0", n)
	}
}

func TestESBulk_DocumentKeepsItsFields(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"filebeat-2026.05.17"}}
{"@timestamp":"2026-05-17T10:00:00Z","message":"filebeat path","host":{"name":"web-01"},"source":"/var/log/app/app.log","target_index":"elsewhere","log":{"file":{"path":"/var/log/app/nginx_access.log"}}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if result.Errors {
		t.Fatal("expected errors=false")
	}
	if got := result.Items[0].Index.Index; got != "filebeat-2026.05.17" {
		t.Fatalf("_index = %q, want filebeat-2026.05.17", got)
	}

	events := queryEvents(t, srv.Addr(),
		`{"q":"source=filebeat-2026.05.17 | fields message, host.name, source, target_index, log.file.path"}`)
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	want := map[string]interface{}{
		"message":       "filebeat path",
		"host.name":     "web-01",
		"source":        "/var/log/app/app.log",
		"target_index":  "elsewhere",
		"log.file.path": "/var/log/app/nginx_access.log",
	}
	for k, v := range want {
		if events[0][k] != v {
			t.Errorf("%s = %v, want %v", k, events[0][k], v)
		}
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=elsewhere"}`); n != 0 {
		t.Fatalf("elsewhere events: got %d, want 0", n)
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

	events := queryEvents(t, srv.Addr(), `{"q":"source=logs | where host.name = \"web-01\""}`)
	if len(events) != 1 {
		t.Fatalf("logs events with host.name web-01: got %d, want 1", len(events))
	}
}

func TestESBulk_FieldFilterBeforeParse(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"nginx-access"}}
{"@timestamp":"2026-05-17T21:45:56Z","message":"192.168.1.203 - - [17/May/2026:21:45:56 +0000] \"OPTIONS /static/style.css HTTP/1.1\" 404 109 \"https://google.com/search?q=test\" \"kube-probe/1.30\" 0.752 0.747","log":{"file":{"path":"/var/log/app/nginx_access.log"}}}
{"index":{"_index":"nginx-access"}}
{"@timestamp":"2026-05-17T21:45:57Z","message":"other file","log":{"file":{"path":"/var/log/app/other.log"}}}
`
	resp := postESBulk(t, srv.Addr(), body)
	result := decodeESBulkResponse(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if result.Errors {
		t.Fatal("expected errors=false")
	}

	const parsed = `source=nginx-access | where log.file.path = "/var/log/app/nginx_access.log" | parse message '.* "(?<referer>[^"]*)" "(?<user_agent>[^"]*)" .*' | head 1`
	queryBody := func(q string) string {
		b, _ := json.Marshal(map[string]string{"q": q})
		return string(b)
	}

	events := queryEvents(t, srv.Addr(), queryBody(parsed))
	if len(events) != 1 {
		t.Fatalf("events: got %d, want 1", len(events))
	}
	if events[0]["log.file.path"] != "/var/log/app/nginx_access.log" {
		t.Fatalf("log.file.path = %v, want /var/log/app/nginx_access.log", events[0]["log.file.path"])
	}
	if events[0]["referer"] != "https://google.com/search?q=test" {
		t.Fatalf("referer = %v, want https://google.com/search?q=test", events[0]["referer"])
	}

	tableOnly := queryEvents(t, srv.Addr(), queryBody(parsed+" | fields referer, user_agent"))
	if len(tableOnly) != 1 {
		t.Fatalf("table-only events: got %d, want 1", len(tableOnly))
	}
	if len(tableOnly[0]) != 2 {
		t.Fatalf("table-only event = %v, want only referer and user_agent", tableOnly[0])
	}
	if tableOnly[0]["referer"] != "https://google.com/search?q=test" {
		t.Fatalf("table-only referer = %v, want https://google.com/search?q=test", tableOnly[0]["referer"])
	}
	if tableOnly[0]["user_agent"] != "kube-probe/1.30" {
		t.Fatalf("table-only user_agent = %v, want kube-probe/1.30", tableOnly[0]["user_agent"])
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
	if ix, ok := indexStats(srv, "bench"); !ok || ix.Rows != 5000 {
		t.Fatalf("bench: %+v, want 5000 rows", ix)
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

	wantStatus := []int{201, 400, 201, 400, 201}
	for i, item := range result.Items {
		if item.Index == nil || item.Index.Status != wantStatus[i] {
			t.Fatalf("item %d = %+v, want status %d", i, item.Index, wantStatus[i])
		}
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 3 {
		t.Fatalf("logs events: got %d, want 3", n)
	}
}

func TestESBulk_ItemsKeepRequestOrderAcrossIndexes(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"a","_id":"1"}}
{"n":1}
{"index":{"_index":"b","_id":"2"}}
{"n":2}
{"index":{"_index":"a","_id":"3"}}
{"n":3}
{"create":{"_index":"c","_id":"4"}}
{"n":4}
`
	result := decodeESBulkResponse(t, postESBulk(t, srv.Addr(), body))
	if result.Errors || len(result.Items) != 4 {
		t.Fatalf("unexpected result: %+v", result)
	}
	for i, want := range []struct{ id, index string }{{"1", "a"}, {"2", "b"}, {"3", "a"}} {
		if got := result.Items[i].Index; got == nil || got.ID != want.id || got.Index != want.index {
			t.Fatalf("item %d = %+v, want _id %s in %s", i, got, want.id, want.index)
		}
	}
	if got := result.Items[3].Create; got == nil || got.ID != "4" || got.Index != "c" {
		t.Fatalf("item 3 = %+v, want create _id 4 in c", got)
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

func TestESBulk_RejectsUnsupportedContentType(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	resp, err := http.Post(fmt.Sprintf("http://%s/_bulk", srv.Addr()), "text/plain",
		strings.NewReader("{\"index\":{\"_index\":\"logs\"}}\n{\"message\":\"text\"}\n"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errObj, _ := result["error"].(map[string]interface{}); errObj["type"] != "content_type_exception" {
		t.Fatalf("error = %v, want content_type_exception", result["error"])
	}
	if names := srv.DB().Names(); len(names) != 0 {
		t.Fatalf("indexes = %v, want none", names)
	}
}

func TestESBulk_BodyOverLimit_Returns413(t *testing.T) {
	srv, cleanup := startTestServerWithConfig(t, Config{Ingest: config.IngestConfig{
		Limits: config.IngestLimitsConfig{MaxCompressedBodyBytes: 1024},
	}})
	defer cleanup()

	var buf bytes.Buffer
	for buf.Len() <= 4096 {
		buf.WriteString("{\"index\":{\"_index\":\"logs\"}}\n{\"message\":\"filler\"}\n")
	}
	resp := postESBulk(t, srv.Addr(), buf.String())
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	if names := srv.DB().Names(); len(names) != 0 {
		t.Fatalf("indexes = %v, want none", names)
	}
}

func TestESBulk_RecordsMetrics(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{"index":{"_index":"logs"}}
{"message":"ok"}
{"create":{"_index":"logs"}}
{"message":"created"}
{"index":{"_index":"logs"}}
not-json
`
	result := decodeESBulkResponse(t, postESBulk(t, srv.Addr(), body))
	if !result.Errors {
		t.Fatal("expected errors=true")
	}

	families := gatherMetrics(t, srv.promMetrics)
	assertCounterVecLabelValue(t, families, "lynxdb_ingest_es_bulk_requests_total", "result", "partial", 1)
	assertHistogramCount(t, families, "lynxdb_ingest_es_bulk_duration_seconds", 1)
	items := map[string]float64{}
	for _, m := range families["lynxdb_ingest_es_bulk_items_total"].GetMetric() {
		labels := map[string]string{}
		for _, lp := range m.GetLabel() {
			labels[lp.GetName()] = lp.GetValue()
		}
		items[labels["action"]+"/"+labels["result"]] = m.GetCounter().GetValue()
	}
	want := map[string]float64{"index/ok": 1, "create/ok": 1, "index/rejected": 1}
	if fmt.Sprint(items) != fmt.Sprint(want) {
		t.Fatalf("bulk items = %v, want %v", items, want)
	}
	assertCounterValue(t, families, "lynxdb_ingest_events_total", 2)
}

func TestESIndexDoc_Basic(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()

	body := `{
  "message": "hello doc",
  "level": "info"
}`
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

	events := queryEvents(t, srv.Addr(), `{"q":"source=logs"}`)
	if len(events) != 1 || events[0]["message"] != "hello doc" || events[0]["level"] != "info" {
		t.Fatalf("logs events = %v, want the document", events)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=main"}`); n != 0 {
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

	n := queryEventCount(t, srv.Addr(), `{"q":"source=myapp"}`)
	if n != 1 {
		t.Fatalf("myapp events: got %d, want 1", n)
	}
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=main"}`); n != 0 {
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

	var result esClusterInfo
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

	var result esClusterInfo
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
	var result esClusterInfo
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

	n := queryEventCount(t, srv.Addr(), `{"q":"source=nginx | where level = \"error\""}`)
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

	n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`)
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
	if n := queryEventCount(t, srv.Addr(), `{"q":"source=logs"}`); n != 2 {
		t.Fatalf("logs events: got %d, want 2", n)
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
	runtimeCfg.Ingest.OTLP.HTTPListen = ""
	runtimeCfg.Ingest.OTLP.GRPCListen = ""
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
	events := queryEvents(t, srv.Addr(), `{"q":"source=logs"}`)
	if len(events) != 1 || events[0]["message"] != "gzip doc" {
		t.Fatalf("logs events = %v, want the gzip document", events)
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
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query status: %d, body: %s", resp.StatusCode, raw)
	}
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

	return len(queryEvents(t, addr, body))
}
