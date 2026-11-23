//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

// esRequest sends a request to an Elasticsearch-compatible route at the
// root of the server and decodes the JSON answer into out.
func esRequest(t *testing.T, h *Harness, method, path, contentType, body string, out any) int {
	t.Helper()
	req, err := http.NewRequest(method, h.BaseURL()+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("%s %s: decode %s: %v", method, path, data, err)
	}

	return resp.StatusCode
}

func TestE2E_ES_ClusterInfo(t *testing.T) {
	h := NewHarness(t)

	var info struct {
		Name    string `json:"name"`
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if status := esRequest(t, h, http.MethodGet, "/", "", "", &info); status != http.StatusOK {
		t.Fatalf("GET /: status %d", status)
	}
	if info.Name == "" {
		t.Error("expected non-empty cluster name")
	}
	if info.Version.Number == "" {
		t.Error("expected non-empty version number")
	}
	t.Logf("ES compat: name=%s, version=%s", info.Name, info.Version.Number)
}

func TestE2E_ES_BulkIngest(t *testing.T) {
	h := NewHarness(t)

	// ES bulk format: action\ndata\n pairs.
	bulk := `{"index":{"_index":"es_test"}}
{"host":"web-01","status":200,"message":"ok"}
{"index":{"_index":"es_test"}}
{"host":"web-02","status":404,"message":"not found"}
{"index":{"_index":"es_test"}}
{"host":"web-03","status":500,"message":"error"}
`
	var result struct {
		Errors bool `json:"errors"`
		Items  []struct {
			Index struct {
				ID     string `json:"_id"`
				Status int    `json:"status"`
			} `json:"index"`
		} `json:"items"`
	}
	if status := esRequest(t, h, http.MethodPost, "/_bulk", "application/x-ndjson", bulk, &result); status != http.StatusOK {
		t.Fatalf("POST /_bulk: status %d", status)
	}
	if result.Errors {
		t.Errorf("expected Errors=false, got true; items=%+v", result.Items)
	}
	if len(result.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(result.Items))
	}
	for i, item := range result.Items {
		if item.Index.Status != 201 || item.Index.ID == "" {
			t.Errorf("item %d: expected status 201 with an id, got %+v", i, item.Index)
		}
	}

	// Each document keeps its own fields.
	r := h.MustQuery(`source=es_test | sort host | fields host, status, message`)
	rows := EventRows(r)
	want := []struct {
		host    string
		status  int
		message string
	}{{"web-01", 200, "ok"}, {"web-02", 404, "not found"}, {"web-03", 500, "error"}}
	if len(rows) != len(want) {
		t.Fatalf("expected %d events, got %d: %v", len(want), len(rows), rows)
	}
	for i, w := range want {
		row := rows[i]
		if fmt.Sprint(row["host"]) != w.host || toInt(row["status"]) != w.status || fmt.Sprint(row["message"]) != w.message {
			t.Errorf("event %d: expected %+v, got %v", i, w, row)
		}
	}
}

func TestE2E_ES_IndexDoc(t *testing.T) {
	h := NewHarness(t)

	doc := `{"host":"web-01","status":200,"message":"indexed via ES compat"}`
	var result struct {
		Result string `json:"result"`
	}
	if status := esRequest(t, h, http.MethodPost, "/es_single_test/_doc", "application/json", doc, &result); status != http.StatusCreated {
		t.Errorf("expected status 201, got %d", status)
	}

	r := h.MustQuery(`source=es_single_test | fields host, status, message`)
	requireEventCount(t, r, 1)
	if GetStr(r, "host") != "web-01" || GetInt(r, "status") != 200 || GetStr(r, "message") != "indexed via ES compat" {
		t.Errorf("expected the document's own fields, got %v", EventRows(r))
	}
}
