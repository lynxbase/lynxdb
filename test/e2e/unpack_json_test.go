//go:build e2e

package e2e

import (
	"context"
	"fmt"
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

// TestE2E_UnpackJSON_WhereStatsCount ingests JSON logs, extracts with unpack_json,
// filters, and aggregates.
func TestE2E_UnpackJSON_WhereStatsCount(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"level":"error","service":"auth","message":"login failed"}`,
		`{"level":"info","service":"api","message":"request completed"}`,
		`{"level":"error","service":"api","message":"timeout"}`,
		`{"level":"warn","service":"auth","message":"rate limited"}`,
		`{"level":"error","service":"db","message":"connection refused"}`,
	}
	ingestJSONEvents(t, h, "jsonlogs", events)

	r := h.MustQuery(`FROM jsonlogs | unpack_json | where level="error" | STATS count`)
	requireAggValue(t, r, "count", 3)
}

// TestE2E_UnpackJSON_StatsByService verifies group-by works on extracted fields.
func TestE2E_UnpackJSON_StatsByService(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"level":"error","service":"auth"}`,
		`{"level":"error","service":"api"}`,
		`{"level":"error","service":"auth"}`,
	}
	ingestJSONEvents(t, h, "svclog", events)

	r := h.MustQuery(`FROM svclog | unpack_json | where level="error" | STATS count by service | sort -count`)
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

// TestE2E_UnpackLogfmt verifies logfmt extraction works end-to-end.
func TestE2E_UnpackLogfmt_Filter(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`level=error msg="disk full" host=web-01`,
		`level=info msg="request ok" host=web-02`,
		`level=error msg="timeout" host=web-01`,
	}
	ingestJSONEvents(t, h, "logfmt_idx", events)

	r := h.MustQuery(`FROM logfmt_idx | unpack_logfmt | where level="error" | STATS count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_UnpackCombined verifies combined access log parsing.
func TestE2E_UnpackCombined_StatusFilter(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`192.168.1.1 - alice [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0"`,
		`10.0.0.1 - bob [14/Feb/2026:14:23:02 +0000] "POST /login HTTP/1.1" 500 567 "-" "curl/7.68"`,
		`10.0.0.2 - - [14/Feb/2026:14:23:03 +0000] "GET /health HTTP/1.1" 502 890 "-" "Go-http-client"`,
	}
	ingestJSONEvents(t, h, "access_idx", events)

	r := h.MustQuery(`FROM access_idx | unpack_combined | where status >= 500 | STATS count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_JsonCmd_SelectivePaths verifies | json with specific paths.
func TestE2E_JsonCmd_SelectivePaths(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"user":{"id":1,"name":"alice"},"request":{"method":"GET","path":"/api"}}`,
		`{"user":{"id":2,"name":"bob"},"request":{"method":"POST","path":"/login"}}`,
	}
	ingestJSONEvents(t, h, "json_paths", events)

	r := h.MustQuery(`FROM json_paths | json path="user.id" AS uid | STATS count by uid`)
	rows := AggRows(r)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (uid=1 and uid=2), got %d", len(rows))
	}
}

// TestE2E_Unroll verifies array explosion with unroll.
func TestE2E_Unroll_StatsCount(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"order":"A","items":[{"sku":"x1","qty":1},{"sku":"x2","qty":2}]}`,
		`{"order":"B","items":[{"sku":"x1","qty":3}]}`,
	}
	ingestJSONEvents(t, h, "unroll_idx", events)

	r := h.MustQuery(`FROM unroll_idx | unpack_json | unroll field=items | STATS count`)
	// 2 items from order A + 1 from order B = 3 rows.
	requireAggValue(t, r, "count", 3)
}

// TestE2E_PackJson verifies field assembly into a JSON string.
func TestE2E_PackJson_RoundTrip(t *testing.T) {
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

	r := h.MustQuery(`FROM main | pack_json level, service into output | STATS count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_JsonFunctions verifies json_valid and json_keys work end-to-end.
func TestE2E_JsonFunctions(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"data":{"a":1,"b":2},"status":"ok"}`,
		`not valid json`,
		`{"data":{"c":3},"status":"fail"}`,
	}
	ingestJSONEvents(t, h, "json_fn_idx", events)

	// Count events where _raw is valid JSON.
	r := h.MustQuery(`FROM json_fn_idx | eval valid=json_valid(_raw) | where valid=true | STATS count`)
	requireAggValue(t, r, "count", 2)
}

// TestE2E_DotNotation verifies inline dot-notation field access.
func TestE2E_DotNotation_InlineWhere(t *testing.T) {
	h := NewHarness(t)

	events := []string{
		`{"request":{"method":"GET","path":"/api"},"status":200}`,
		`{"request":{"method":"POST","path":"/login"},"status":201}`,
		`{"request":{"method":"GET","path":"/health"},"status":200}`,
	}
	ingestJSONEvents(t, h, "dot_idx", events)

	r := h.MustQuery(fmt.Sprintf(`FROM dot_idx | unpack_json | where request.method="POST" | STATS count`))
	requireAggValue(t, r, "count", 1)
}
