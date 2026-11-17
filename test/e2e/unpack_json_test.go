//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// ingestJSONEvents ingests NDJSON events into an index via IngestRaw.
func ingestJSONEvents(t *testing.T, h *Harness, index string, events []string) {
	t.Helper()

	ctx := context.Background()
	body := strings.NewReader(strings.Join(events, "\n"))
	result, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       index,
		Source:      index,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("ingest into %s: %v", index, err)
	}
	if result.Accepted != len(events) {
		t.Fatalf("expected %d accepted for %s, got %d", len(events), index, result.Accepted)
	}
}

// TestE2E_Spath_WhereStatsCount ingests JSON logs, extracts a field with
// spath, filters, and aggregates.
func TestE2E_Spath_WhereStatsCount(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"level":"error","service":"auth","message":"login failed"}`,
		`{"level":"info","service":"api","message":"request completed"}`,
		`{"level":"error","service":"api","message":"timeout"}`,
		`{"level":"warn","service":"auth","message":"rate limited"}`,
		`{"level":"error","service":"db","message":"connection refused"}`,
	}
	ingestJSONEvents(t, h, "jsonlogs", events)

	r := h.MustQuery(`source=jsonlogs | spath input=_raw path=level | where level = "error" | stats count() as count`)
	requireAggValue(t, r, "count", 3)
}

// TestE2E_Spath_StatsByService verifies group-by works on extracted fields.
func TestE2E_Spath_StatsByService(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"level":"error","service":"auth"}`,
		`{"level":"error","service":"api"}`,
		`{"level":"error","service":"auth"}`,
	}
	ingestJSONEvents(t, h, "svclog", events)

	r := h.MustQuery(`source=svclog | spath input=_raw path=level | spath input=_raw path=service | where level = "error" | stats count() as count by service | sort -count`)
	rows := AggRows(r)
	if len(rows) < 2 {
		t.Fatalf("expected >= 2 rows, got %d", len(rows))
	}
	// auth=2 should be first (sorted descending).
	if GetStr(r, "service") != "auth" || GetInt(r, "count") != 2 {
		t.Errorf("first row: expected service=auth count=2, got service=%s count=%d",
			GetStr(r, "service"), GetInt(r, "count"))
	}
}

// TestE2E_Logfmt_Filter verifies that logfmt fields extracted at ingest are
// queryable.
func TestE2E_Logfmt_Filter(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`level=error msg="disk full" host=web-01`,
		`level=info msg="request ok" host=web-02`,
		`level=error msg="timeout" host=web-01`,
	}
	ingestJSONEvents(t, h, "logfmt_idx", events)

	r := h.MustQuery(`source=logfmt_idx | where level = "error" | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_GrokCombined_StatusFilter verifies combined access log parsing.
func TestE2E_GrokCombined_StatusFilter(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`192.168.1.1 - alice [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0"`,
		`10.0.0.1 - bob [14/Feb/2026:14:23:02 +0000] "POST /login HTTP/1.1" 500 567 "-" "curl/7.68"`,
		`10.0.0.2 - - [14/Feb/2026:14:23:03 +0000] "GET /health HTTP/1.1" 502 890 "-" "Go-http-client"`,
	}
	ingestJSONEvents(t, h, "access_idx", events)

	r := h.MustQuery(`source=access_idx | grok _raw "%{COMBINEDAPACHELOG}" | where cast(response as int) >= 500 | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_Spath_SelectivePaths verifies spath extraction of a nested path.
func TestE2E_Spath_SelectivePaths(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"user":{"id":1,"name":"alice"},"request":{"method":"GET","path":"/api"}}`,
		`{"user":{"id":2,"name":"bob"},"request":{"method":"POST","path":"/login"}}`,
	}
	ingestJSONEvents(t, h, "json_paths", events)

	r := h.MustQuery(`source=json_paths | spath input=_raw path=user.id output=uid | stats count() by uid`)
	rows := AggRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (uid=1 and uid=2), got %d", len(rows))
	}
}

// TestE2E_Expand_StatsCount verifies array expansion with expand.
func TestE2E_Expand_StatsCount(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"order":"A","items":[{"sku":"x1","qty":1},{"sku":"x2","qty":2}]}`,
		`{"order":"B","items":[{"sku":"x1","qty":3}]}`,
	}
	ingestJSONEvents(t, h, "unroll_idx", events)

	r := h.MustQuery(`source=unroll_idx | spath input=_raw output=doc | eval items = doc.items | expand items | stats count() as count`)
	// 2 items from order A + 1 from order B = 3 rows.
	requireAggValue(t, r, "count", 3)
}

// TestE2E_JsonObject_RoundTrip verifies field assembly into a JSON string.
func TestE2E_JsonObject_RoundTrip(t *testing.T) {
	h := NewHarness(t)

	ctx := context.Background()
	events := []client.IngestEvent{
		{
			Event:  `{"level":"error","service":"auth","host":"web-01"}`,
			Host:   "web-01",
			Fields: map[string]interface{}{"level": "error", "service": "auth"},
		},
		{
			Event:  `{"level":"info","service":"api","host":"web-02"}`,
			Host:   "web-02",
			Fields: map[string]interface{}{"level": "info", "service": "api"},
		},
	}
	result, err := h.Client().IngestEvents(ctx, events)
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if result.Accepted != 2 {
		t.Fatalf("expected 2 accepted, got %d", result.Accepted)
	}

	r := h.MustQuery(`source=main | eval output = json_object("level", level, "service", service) | where json_valid(output) | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_JsonValid verifies json_valid end-to-end.
func TestE2E_JsonValid(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"data":{"a":1,"b":2},"status":"ok"}`,
		`not valid json`,
		`{"data":{"c":3},"status":"fail"}`,
	}
	ingestJSONEvents(t, h, "json_fn_idx", events)

	r := h.MustQuery(`source=json_fn_idx | where json_valid(_raw) | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_DotNotation verifies dot-notation access into a parsed object.
func TestE2E_DotNotation_InlineWhere(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"request":{"method":"GET","path":"/api"},"status":200}`,
		`{"request":{"method":"POST","path":"/login"},"status":201}`,
		`{"request":{"method":"GET","path":"/health"},"status":200}`,
	}
	ingestJSONEvents(t, h, "dot_idx", events)

	r := h.MustQuery(`source=dot_idx | spath input=_raw output=doc | where doc.request.method = "POST" | stats count() as count`)
	requireAggValue(t, r, "count", 1)
}
