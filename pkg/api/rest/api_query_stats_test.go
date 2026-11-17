package rest

import (
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

func TestIntegration_QueryExplain_RangePredicateInPlan(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	appendStatusRangeEvents(t, srv, 1024)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/query/explain?q=%s", srv.Addr(),
		url.QueryEscape(statusRangeQuery)))
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
	scope, _ := parsed["source_scope"].(map[string]interface{})
	if scope["total_sources_available"] != float64(1) {
		t.Fatalf("source_scope = %v, want one index available", parsed["source_scope"])
	}
}

func TestIntegration_Query_RangePredicate(t *testing.T) {
	srv, cleanup := startTestServer(t)
	defer cleanup()
	appendStatusRangeEvents(t, srv, 1024)

	events := queryEvents(t, srv.Addr(), fmt.Sprintf(`{"q":%q}`, statusRangeQuery))
	// Statuses cycle through 200..699; two full cycles reach 500..599.
	if len(events) != 200 {
		t.Fatalf("events: got %d, want 200", len(events))
	}
	for _, ev := range events {
		status, _ := ev["status"].(float64)
		if status < 500 || status > 599 || len(ev) != 1 {
			t.Fatalf("event %v, want only a status in [500, 599]", ev)
		}
	}
}

const statusRangeQuery = `source=main | where status >= 500 and status <= 599 | fields status`

func appendStatusRangeEvents(t *testing.T, srv *Server, n int) {
	t.Helper()
	base := time.Date(2026, 5, 8, 17, 0, 0, 0, time.UTC)
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		status := int64(200 + i%500)
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("status=%d row=%d", status, i))
		e.Index = "main"
		e.Source = "/var/log/range-rest.log"
		e.SourceType = "json"
		e.Host = "range-rest-host"
		e.SetField("status", event.IntValue(status))
		events[i] = e
	}
	if err := srv.DB().AppendEvents(events); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
}
