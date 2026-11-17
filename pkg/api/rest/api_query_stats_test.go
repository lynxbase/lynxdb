package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestIntegration_QueryStats_RangeBSICounters_ExposedInMetaStats(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	ingestRangeBSIRestEvents(t, srv, 1024)

	body, _ := json.Marshal(map[string]interface{}{
		"q": rangeBSIQuery,
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

	meta, ok := envelope["meta"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing meta in response: %#v", envelope)
	}
	if _, ok := meta["took_ms"]; !ok {
		t.Fatalf("missing meta.took_ms: %#v", meta)
	}

	// Verify the query returned data (BSI filtering is working).
	data, ok := envelope["data"]
	if !ok || data == nil {
		t.Fatalf("missing data in response")
	}
}

func TestIntegration_QueryExplain_RangePredicateInPlan(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	ingestRangeBSIRestEvents(t, srv, 1024)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(),
		url.QueryEscape(rangeBSIQuery)))
	if err != nil {
		t.Fatalf("GET explain: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200; body=%s", resp.StatusCode, b)
	}

	var envelope map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	data, ok := envelope["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing data: %#v", envelope)
	}
	if data["is_valid"] != true {
		t.Fatalf("is_valid = %v, want true", data["is_valid"])
	}
	parsed, ok := data["parsed"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing parsed: %#v", data)
	}
	plan, _ := parsed["optimized_plan"].(string)
	if !strings.Contains(plan, "status") {
		t.Fatalf("optimized plan missing status predicate: %s", plan)
	}
	fieldsRead, _ := parsed["fields_read"].([]interface{})
	if !slices.Contains(fieldsRead, interface{}("status")) {
		t.Fatalf("fields_read = %v, want status", fieldsRead)
	}
}

const rangeBSIQuery = `source=main | where status >= 500 and status <= 599 | fields status`

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
