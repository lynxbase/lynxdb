//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestE2E_ES_ClusterInfo(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	info, err := h.Client().ESClusterInfo(ctx)
	if err != nil {
		t.Fatalf("ESClusterInfo: %v", err)
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
	ctx := context.Background()

	// ES bulk format: action\ndata\n pairs.
	bulk := `{"index":{"_index":"es_test"}}
{"host":"web-01","status":200,"message":"ok"}
{"index":{"_index":"es_test"}}
{"host":"web-02","status":404,"message":"not found"}
{"index":{"_index":"es_test"}}
{"host":"web-03","status":500,"message":"error"}
`
	result, err := h.Client().ESBulk(ctx, strings.NewReader(bulk))
	if err != nil {
		t.Fatalf("ESBulk: %v", err)
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
	ctx := context.Background()

	doc := `{"host":"web-01","status":200,"message":"indexed via ES compat"}`
	resp, err := h.Client().ESIndexDoc(ctx, "es_single_test", strings.NewReader(doc))
	if err != nil {
		t.Fatalf("ESIndexDoc: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		t.Errorf("expected status 201, got %d", resp.StatusCode)
	}

	r := h.MustQuery(`source=es_single_test | fields host, status, message`)
	requireEventCount(t, r, 1)
	if GetStr(r, "host") != "web-01" || GetInt(r, "status") != 200 || GetStr(r, "message") != "indexed via ES compat" {
		t.Errorf("expected the document's own fields, got %v", EventRows(r))
	}
}
