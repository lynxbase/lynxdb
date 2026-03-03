//go:build e2e

package e2e

import (
	"context"
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
		t.Errorf("expected Errors=false, got true; items=%d", len(result.Items))
	}
	if len(result.Items) != 3 {
		t.Errorf("expected 3 items, got %d", len(result.Items))
	}
	t.Logf("ES bulk: took=%dms, items=%d", result.Took, len(result.Items))
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

	if resp.StatusCode >= 400 {
		t.Errorf("expected success status, got %d", resp.StatusCode)
	}
	t.Logf("ES index doc: status=%d", resp.StatusCode)
}
