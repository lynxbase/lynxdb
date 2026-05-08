package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestIntegration_QueryStats_RangeBSICounters_ExposedInMetaStats(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	ingestRangeBSIRestEvents(t, srv, 1024)

	body, _ := json.Marshal(map[string]interface{}{
		"q": `FROM main | where status >= 500 AND status <= 599 | table status`,
	})
	resp, err := http.Post(fmt.Sprintf("http://%s/api/v1/query", srv.Addr()), "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST query: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, b)
	}

	var envelope map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	stats := responseMetaStats(t, envelope)
	if got := numericMetaStat(t, stats, "range_bsi_checks"); got <= 0 {
		t.Fatalf("meta.stats.range_bsi_checks = %v, want > 0", got)
	}
	if raw, ok := stats["range_bsi_skips"]; ok {
		if got, ok := raw.(float64); !ok || got < 0 {
			t.Fatalf("meta.stats.range_bsi_skips = %v (%T), want JSON number >= 0", raw, raw)
		}
	}
	if got := numericMetaStat(t, stats, "range_bsi_mask_bytes"); got <= 0 {
		t.Fatalf("meta.stats.range_bsi_mask_bytes = %v, want > 0", got)
	}
}

func TestIntegration_QueryExplainAnalyze_RangePredicateReportsBSIStrategy(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	ingestRangeBSIRestEvents(t, srv, 1024)

	u := fmt.Sprintf("http://%s/api/v1/query/explain?q=%s&analyze=true", srv.Addr(),
		url.QueryEscape(`FROM main | where status >= 500 AND status <= 599 | table status`))
	resp, err := http.Get(u)
	if err != nil {
		t.Fatalf("GET explain analyze: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, b)
	}

	var envelope map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	data := envelope["data"].(map[string]interface{})
	parsed := data["parsed"].(map[string]interface{})
	preds, ok := parsed["range_predicates"].([]interface{})
	if !ok || len(preds) == 0 {
		t.Fatalf("range_predicates = %#v, want at least one predicate", parsed["range_predicates"])
	}
	pred := preds[0].(map[string]interface{})
	if pred["rg_filter_strategy"] != "bsi" {
		// BUG: EXPLAIN currently renders range predicates from planner hints before
		// the server lowers predicates against live segment metadata during execution.
		t.Fatalf("rg_filter_strategy = %v, want bsi", pred["rg_filter_strategy"])
	}
	if pred["row_vm_strategy"] != "handled_by=bsi" {
		t.Fatalf("row_vm_strategy = %v, want handled_by=bsi", pred["row_vm_strategy"])
	}
	if pred["lowered_to_bsi"] != true {
		t.Fatalf("lowered_to_bsi = %v, want true", pred["lowered_to_bsi"])
	}
}

func ingestRangeBSIRestEvents(t *testing.T, srv *Server, n int) {
	t.Helper()
	base := time.Date(2026, 5, 8, 17, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		status := int64(200 + i%500)
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("status=%d row=%d", status, i))
		e.Index = "main"
		e.Source = "/var/log/range-bsi-rest.log"
		e.SourceType = "json"
		e.Host = "range-bsi-rest-host"
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}
	if err := srv.Engine().Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
}

func responseMetaStats(t *testing.T, envelope map[string]interface{}) map[string]interface{} {
	t.Helper()
	meta, ok := envelope["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing meta object in response: %#v", envelope)
	}
	stats, ok := meta["stats"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing meta.stats object in response meta: %#v", meta)
	}

	return stats
}

func numericMetaStat(t *testing.T, stats map[string]interface{}, key string) float64 {
	t.Helper()
	raw, ok := stats[key]
	if !ok {
		t.Fatalf("missing meta.stats.%s in %#v", key, stats)
	}
	got, ok := raw.(float64)
	if !ok {
		t.Fatalf("meta.stats.%s = %T, want JSON number", key, raw)
	}

	return got
}
