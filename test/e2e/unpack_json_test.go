//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// ingestLines sends lines of text through raw ingest into an index. A line
// that is a JSON object is stored with its own fields and no _raw.
func ingestLines(t *testing.T, h *Harness, index string, lines []string) {
	t.Helper()

	ctx := context.Background()
	body := strings.NewReader(strings.Join(lines, "\n"))
	result, err := h.Client().IngestRaw(ctx, body, client.IngestOpts{
		Index:       index,
		Source:      index,
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("ingest into %s: %v", index, err)
	}
	if result.Accepted != len(lines) {
		t.Fatalf("expected %d accepted for %s, got %d", len(lines), index, result.Accepted)
	}
}

// ingestRawTexts stores each text as the _raw of one event in an index.
func ingestRawTexts(t *testing.T, h *Harness, index string, texts []string) {
	t.Helper()

	events := make([]client.IngestEvent, len(texts))
	for i, text := range texts {
		events[i] = client.IngestEvent{Event: text, Index: index}
	}
	result, err := h.Client().IngestEvents(context.Background(), events)
	if err != nil {
		t.Fatalf("ingest into %s: %v", index, err)
	}
	if result.Accepted != len(texts) {
		t.Fatalf("expected %d accepted for %s, got %d", len(texts), index, result.Accepted)
	}
}

// TestE2E_Spath_WhereStatsCount extracts a field from JSON text with spath,
// filters, and aggregates.
func TestE2E_Spath_WhereStatsCount(t *testing.T) {
	h := NewHarness(t)

	ingestRawTexts(t, h, "jsonlogs", []string{
		`{"level":"error","service":"auth","message":"login failed"}`,
		`{"level":"info","service":"api","message":"request completed"}`,
		`{"level":"error","service":"api","message":"timeout"}`,
		`{"level":"warn","service":"auth","message":"rate limited"}`,
		`{"level":"error","service":"db","message":"connection refused"}`,
	})

	r := h.MustQuery(`source=jsonlogs | spath input=_raw path=level | where level = "error" | stats count() as count`)
	requireAggValue(t, r, "count", 3)
}

// TestE2E_Spath_StatsByService verifies group-by works on extracted fields.
func TestE2E_Spath_StatsByService(t *testing.T) {
	h := NewHarness(t)

	ingestRawTexts(t, h, "svclog", []string{
		`{"level":"error","service":"auth"}`,
		`{"level":"error","service":"api"}`,
		`{"level":"error","service":"auth"}`,
	})

	r := h.MustQuery(`source=svclog | spath input=_raw path=level | spath input=_raw path=service | where level = "error" | stats count() as count by service | sort -count`)
	rows := AggRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
	if GetStr(r, "service") != "auth" || GetInt(r, "count") != 2 {
		t.Errorf("first row: expected service=auth count=2, got service=%s count=%d",
			GetStr(r, "service"), GetInt(r, "count"))
	}
	if fmt.Sprint(rows[1]["service"]) != "api" || toInt(rows[1]["count"]) != 1 {
		t.Errorf("second row: expected service=api count=1, got %v", rows[1])
	}
}

// TestE2E_Logfmt_Filter verifies that logfmt fields extracted at ingest are
// queryable.
func TestE2E_Logfmt_Filter(t *testing.T) {
	h := NewHarness(t)

	ingestLines(t, h, "logfmt_idx", []string{
		`level=error msg="disk full" host=web-01`,
		`level=info msg="request ok" host=web-02`,
		`level=error msg="timeout" host=web-01`,
	})

	r := h.MustQuery(`source=logfmt_idx | where level = "error" | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_GrokCombined_StatusFilter verifies combined access log parsing.
func TestE2E_GrokCombined_StatusFilter(t *testing.T) {
	h := NewHarness(t)

	ingestLines(t, h, "access_idx", []string{
		`192.168.1.1 - alice [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0"`,
		`10.0.0.1 - bob [14/Feb/2026:14:23:02 +0000] "POST /login HTTP/1.1" 500 567 "-" "curl/7.68"`,
		`10.0.0.2 - - [14/Feb/2026:14:23:03 +0000] "GET /health HTTP/1.1" 502 890 "-" "Go-http-client"`,
	})

	r := h.MustQuery(`source=access_idx | grok _raw "%{COMBINEDAPACHELOG}" | where cast(response as int) >= 500 | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_Spath_SelectivePaths verifies spath extraction of a nested path.
func TestE2E_Spath_SelectivePaths(t *testing.T) {
	h := NewHarness(t)

	ingestRawTexts(t, h, "json_paths", []string{
		`{"user":{"id":1,"name":"alice"},"request":{"method":"GET","path":"/api"}}`,
		`{"user":{"id":2,"name":"bob"},"request":{"method":"POST","path":"/login"}}`,
	})

	r := h.MustQuery(`source=json_paths | spath input=_raw path=user.id output=uid | stats count() by uid | sort uid`)
	rows := AggRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (uid=1 and uid=2), got %d: %v", len(rows), rows)
	}
	for i, want := range []int{1, 2} {
		if toInt(rows[i]["uid"]) != want || toInt(rows[i]["count()"]) != 1 {
			t.Errorf("row %d: expected uid=%d count()=1, got %v", i, want, rows[i])
		}
	}
}

// TestE2E_Expand_StatsCount verifies array expansion of a JSON event's array
// field.
func TestE2E_Expand_StatsCount(t *testing.T) {
	h := NewHarness(t)

	ingestLines(t, h, "unroll_idx", []string{
		`{"order":"A","items":[{"sku":"x1","qty":1},{"sku":"x2","qty":2}]}`,
		`{"order":"B","items":[{"sku":"x1","qty":3}]}`,
	})

	// 2 items from order A + 1 from order B = 3 rows.
	requireAggValue(t, h.MustQuery(`source=unroll_idx | expand items | stats count() as count`), "count", 3)

	r := h.MustQuery(`source=unroll_idx | expand items | flatten items | stats sum(qty) as units by sku | sort sku`)
	rows := AggRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 sku rows, got %d: %v", len(rows), rows)
	}
	for i, want := range []struct {
		sku   string
		units int
	}{{"x1", 4}, {"x2", 2}} {
		if fmt.Sprint(rows[i]["sku"]) != want.sku || toInt(rows[i]["units"]) != want.units {
			t.Errorf("row %d: expected sku=%s units=%d, got %v", i, want.sku, want.units, rows[i])
		}
	}
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

	ingestRawTexts(t, h, "json_fn_idx", []string{
		`{"data":{"a":1,"b":2},"status":"ok"}`,
		`not valid json`,
		`{"data":{"c":3},"status":"fail"}`,
	})

	r := h.MustQuery(`source=json_fn_idx | where json_valid(_raw) | stats count() as count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_DotNotation_InlineWhere verifies that the members of a JSON
// event's nested object are fields named by their dotted path.
func TestE2E_DotNotation_InlineWhere(t *testing.T) {
	h := NewHarness(t)

	ingestLines(t, h, "dot_idx", []string{
		`{"request":{"method":"GET","path":"/api"},"status":200}`,
		`{"request":{"method":"POST","path":"/login"},"status":201}`,
		`{"request":{"method":"GET","path":"/health"},"status":200}`,
	})

	r := h.MustQuery(`source=dot_idx | where request.method = "POST" | stats count() as count`)
	requireAggValue(t, r, "count", 1)
}

// TestE2E_DotNotation_SpathFlatten verifies that flatten turns the object
// spath parses into fields named by their dotted path.
func TestE2E_DotNotation_SpathFlatten(t *testing.T) {
	h := NewHarness(t)

	ingestRawTexts(t, h, "dot_raw_idx", []string{
		`{"request":{"method":"GET","path":"/api"},"status":200}`,
		`{"request":{"method":"POST","path":"/login"},"status":201}`,
		`{"request":{"method":"GET","path":"/health"},"status":200}`,
	})

	r := h.MustQuery(`source=dot_raw_idx | spath input=_raw output=doc | flatten doc | where request.method = "POST" | stats count() as count`)
	requireAggValue(t, r, "count", 1)
}
