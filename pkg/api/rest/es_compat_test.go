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
	}

	// Verify events are queryable.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 2 {
		t.Fatalf("expected at least 2 events, got %d", n)
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

	// Verify event was ingested.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 1 {
		t.Fatal("expected at least 1 event")
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

	// Verify event was ingested.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 1 {
		t.Fatal("expected at least 1 event")
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

	// Verify queryable.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 1 {
		t.Fatal("expected at least 1 event")
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

	// Verify queryable.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 1 {
		t.Fatal("expected at least 1 event")
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
	if result.Name != "lynxdb" {
		t.Fatalf("name: %q", result.Name)
	}
	if result.ClusterName != "lynxdb" {
		t.Fatalf("cluster_name: %q", result.ClusterName)
	}
	if result.Version.Number != "8.11.0" {
		t.Fatalf("version.number: %q", result.Version.Number)
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
	if result.Name != "lynxdb" {
		t.Fatalf("name: %q", result.Name)
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
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 10 {
		t.Fatalf("expected 10 events, got %d", n)
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

	// Verify events queryable.
	time.Sleep(200 * time.Millisecond)
	n := queryEventCount(t, srv.Addr(), `{"q":"FROM main"}`)
	if n < 2 {
		t.Fatalf("expected at least 2 events, got %d", n)
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

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if len(result) != 0 {
		t.Fatalf("expected empty JSON object, got %v", result)
	}

	// Check X-Elastic-Product header.
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

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
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

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
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

	if resp.StatusCode != 200 {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
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
