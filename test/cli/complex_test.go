//go:build clitest

package cli_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Helpers for complex tests

// generateBackendEvents creates NDJSON lines mimicking backend_server.log
// with services: api, payment, gateway, auth.  Each event has level, service,
// status, duration_ms, and optionally error and amount_cents fields.
func generateBackendEvents(n int) string {
	levels := []string{"INFO", "INFO", "INFO", "WARN", "ERROR"}
	services := []string{"api-service", "payment-service", "gateway-service", "auth-service"}
	statuses := []int{200, 200, 201, 400, 500}

	var sb strings.Builder
	for i := 0; i < n; i++ {
		level := levels[i%len(levels)]
		svc := services[i%len(services)]
		status := statuses[i%len(statuses)]
		dur := 10 + (i * 37 % 5000)

		ev := map[string]interface{}{
			"timestamp":   fmt.Sprintf("2026-03-01T%02d:%02d:%02d.000Z", (i/60)%24, i%60, i%30),
			"level":       level,
			"service":     svc,
			"status":      status,
			"duration_ms": dur,
			"message":     fmt.Sprintf("request-%d processed", i),
			"request_id":  fmt.Sprintf("req-%04d", i),
			"client_ip":   fmt.Sprintf("10.0.%d.%d", i%5, i%254+1),
			"memory_mb":   100 + float64(i%500),
			"cpu_pct":     5 + float64(i%90),
		}

		if level == "ERROR" {
			ev["error"] = "internal server error"
		}
		if svc == "payment-service" && status == 200 {
			ev["amount_cents"] = 1000 + i*100
		}

		b, _ := json.Marshal(ev)
		sb.Write(b)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// jsonFieldFloat extracts a float64 field from the first NDJSON row.
func jsonFieldFloat(t *testing.T, stdout, field string) float64 {
	t.Helper()
	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatalf("expected at least 1 JSON row, got 0")
	}
	v, ok := rows[0][field]
	if !ok {
		t.Fatalf("row missing field %q, keys: %v", field, mapKeys(rows[0]))
	}
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	default:
		t.Fatalf("unexpected type %T for field %q: %v", v, field, v)
		return 0
	}
}

// Subquery Tests

func TestComplex_Subquery_InFilter_MatchesDirectWhere(t *testing.T) {
	// generateBackendEvents gives every ERROR event status 500, so the IN
	// subquery selects exactly the ERROR level: n/5 events.
	input := generateBackendEvents(50)

	direct := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level = "ERROR" | stats count() as n`)
	if direct.ExitCode != 0 {
		t.Fatalf("direct query exit %d, stderr: %s", direct.ExitCode, direct.Stderr)
	}
	directN := int(jsonFieldFloat(t, direct.Stdout, "n"))

	sub := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level in [ source=main | where status = 500 | fields level ] | stats count() as n`)
	if sub.ExitCode != 0 {
		t.Fatalf("subquery exit %d, stderr: %s", sub.ExitCode, sub.Stderr)
	}
	subN := int(jsonFieldFloat(t, sub.Stdout, "n"))

	if directN != 10 {
		t.Errorf("direct WHERE count = %d, want 10", directN)
	}
	if subN != directN {
		t.Errorf("IN subquery count (%d) != direct WHERE count (%d)", subN, directN)
	}
}

func TestComplex_Subquery_Scalar_MatchesEventstats(t *testing.T) {
	input := generateBackendEvents(50)

	sub := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where duration_ms > [ source=main | stats avg(duration_ms) ] | stats count() as n`)
	if sub.ExitCode != 0 {
		t.Fatalf("scalar subquery exit %d, stderr: %s", sub.ExitCode, sub.Stderr)
	}

	es := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eventstats avg(duration_ms) as avg_dur | where duration_ms > avg_dur | stats count() as n`)
	if es.ExitCode != 0 {
		t.Fatalf("eventstats exit %d, stderr: %s", es.ExitCode, es.Stderr)
	}

	subN := int(jsonFieldFloat(t, sub.Stdout, "n"))
	esN := int(jsonFieldFloat(t, es.Stdout, "n"))
	if subN == 0 {
		t.Fatal("scalar subquery matched no events above the average")
	}
	if subN != esN {
		t.Errorf("scalar subquery count (%d) != eventstats count (%d)", subN, esN)
	}
}

// JOIN Tests

func TestComplex_Join_ByField_Count(t *testing.T) {
	// The left side has one row per service with its total; the right side one
	// row per service with its errors. Every service of the fixture has ERROR
	// events, so the inner join keeps all four services.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats count() as total by service | join type=inner service [ source=main | where level = "ERROR" | stats count() as errs by service ] | stats count() as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 4 {
		t.Errorf("JOIN rows = %d, want 4 (one per service). stdout: %s", got, r.Stdout)
	}
}

// APPEND and UNION Tests

func TestComplex_Append_TotalIsSum(t *testing.T) {
	// Two one-row aggregates concatenated via append must emit both rows.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level = "ERROR" | stats count() as n | append [ source=main | where level = "WARN" | stats count() as n ]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 2 {
		t.Fatalf("append rows = %d, want 2. stdout: %s", len(rows), r.Stdout)
	}
	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["n"], 0))
	}
	if total != 20 {
		t.Errorf("append total = %d, want 20 (10 ERROR + 10 WARN)", total)
	}
}

func TestComplex_Union_CountIsSum(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`union [ search source=main | where level = "ERROR" ] [ search source=main | where level = "WARN" ] | stats count() as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 20 {
		t.Errorf("union count = %d, want 20 (10 ERROR + 10 WARN)", got)
	}
}

// Deep Pipeline Tests

func TestComplex_DeepPipeline_CountsAddUp(t *testing.T) {
	// where -> eval -> stats -> sort -> head -> eval -> fields; the groups of
	// the non-INFO events must add up to the 40 WARN and ERROR events.
	input := generateBackendEvents(100)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level != "INFO" | eval bucket = case(duration_ms < 50, "fast", duration_ms < 500, "medium" else "slow") | stats count() as count by level, bucket | sort - count | head 20 | eval total_group = count | fields level, bucket, total_group`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from deep pipeline")
	}

	total := 0
	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level'", i)
		}
		if _, ok := row["bucket"]; !ok {
			t.Errorf("row %d missing 'bucket'", i)
		}
		if _, ok := row["total_group"]; !ok {
			t.Errorf("row %d missing 'total_group'", i)
		}
		total += int(toFloatOr(row["total_group"], 0))
	}
	if total != 40 {
		t.Errorf("groups add up to %d, want 40 non-INFO events", total)
	}
}

func TestComplex_DeepPipeline_SortOrderCorrect(t *testing.T) {
	// stats -> eval -> sort desc -> head: error_rate must come out descending.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval is_error = if(level = "ERROR", 1, 0) | stats sum(is_error) as errors, count() as total by service | eval error_rate = round(errors * 100.0 / total, 1) | sort - error_rate | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	for i := 1; i < len(rows); i++ {
		prevF, ok1 := toFloat(rows[i-1]["error_rate"])
		currF, ok2 := toFloat(rows[i]["error_rate"])
		if !ok1 || !ok2 {
			t.Fatalf("row %d: error_rate is not numeric: %v, %v", i, rows[i-1]["error_rate"], rows[i]["error_rate"])
		}
		if prevF < currF-0.01 {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prevF, currF)
		}
	}
}

// Nested Aggregation Tests

func TestComplex_NestedAgg_TwoPhaseConsistent(t *testing.T) {
	// stats -> eval -> stats: the second stats groups the services of the first.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats count() as events, avg(duration_ms) as avg_dur by service | eval is_slow = if(avg_dur > 500, "slow", "fast") | stats count() as group_count by is_slow`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from nested aggregation")
	}

	totalGroups := 0
	for _, row := range rows {
		totalGroups += int(toFloatOr(row["group_count"], 0))
	}

	if totalGroups != 4 {
		t.Errorf("group_count adds up to %d, want 4 services", totalGroups)
	}
}

// Eval Function Tests

func TestComplex_EvalReplace_OutputIsValid(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval clean = replace(message, "[^a-zA-Z0-9 ]", "") | fields clean | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	for i, row := range rows {
		clean, _ := row["clean"].(string)
		if clean == "" {
			t.Errorf("row %d: replace produced empty string", i)
		}
		for _, ch := range clean {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == ' ') {
				t.Errorf("row %d: replace left non-alphanumeric char %q in %q", i, ch, clean)
				break
			}
		}
	}
}

func TestComplex_EvalLike_PrefixAndSubstring(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval is_request = if(like(message, "request%"), "yes", "no"), has_dash = if(like(message, "%-%"), "yes", "no") | stats count() by is_request, has_dash | sort is_request, has_dash`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from like")
	}

	// Every message is "request-<n> processed".
	for _, row := range rows {
		if row["is_request"] != "yes" || row["has_dash"] != "yes" {
			t.Errorf("expected is_request=yes and has_dash=yes for all rows, got %v", row)
		}
	}
}

func TestComplex_EvalIsNotNull_NullHandling(t *testing.T) {
	// Some events have error/amount_cents, others don't.
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval has_error = if(isnotnull(error), "yes", "no"), has_amount = if(isnotnull(amount_cents), "yes", "no") | stats count() as count by has_error, has_amount | sort has_error, has_amount`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 groups, got %d", len(rows))
	}

	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["count"], 0))
	}
	if total != 20 {
		t.Errorf("expected total count=20, got %d", total)
	}
}

func TestComplex_EvalCoalesce(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval err = coalesce(error, "none"), amt = coalesce(amount_cents, 0) | stats count() by err | sort err`)

	if r.ExitCode != 0 {
		t.Fatalf("coalesce: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	foundNone := false
	for _, row := range rows {
		if row["err"] == "none" {
			foundNone = true
		}
	}
	if !foundNone {
		t.Error("expected 'none' group from coalesce, not found")
	}
}

// Percentile Aggregation Tests

func TestComplex_Percentiles_Ordered(t *testing.T) {
	input := generateBackendEvents(100)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats percentile(duration_ms, 25) as p25, p50(duration_ms) as p50, p75(duration_ms) as p75, p90(duration_ms) as p90, p99(duration_ms) as p99`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	names := []string{"p25", "p50", "p75", "p90", "p99"}
	values := make([]float64, len(names))
	for i, name := range names {
		values[i] = jsonFieldFloat(t, r.Stdout, name)
	}
	for i := 1; i < len(values); i++ {
		if values[i-1] > values[i] {
			t.Errorf("%s (%.1f) > %s (%.1f)", names[i-1], values[i-1], names[i], values[i])
		}
	}
}

// Search Expression Tests (AND, OR, NOT)

func TestComplex_SearchTerms_CountMatchesRegexpWhere(t *testing.T) {
	input := generateBackendEvents(50)

	searchResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`search source=main request processed | stats count() as count`)
	if searchResult.ExitCode != 0 {
		t.Fatalf("search exit %d: %s", searchResult.ExitCode, searchResult.Stderr)
	}

	whereResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where regexp_match(message, "request") and regexp_match(message, "processed") | stats count() as count`)
	if whereResult.ExitCode != 0 {
		t.Fatalf("where exit %d: %s", whereResult.ExitCode, whereResult.Stderr)
	}

	searchCount := jsonCount(t, searchResult.Stdout)
	whereCount := jsonCount(t, whereResult.Stdout)

	if searchCount != 50 {
		t.Errorf("search count = %d, want 50", searchCount)
	}
	if searchCount != whereCount {
		t.Errorf("search AND count (%d) != where regexp_match AND count (%d)", searchCount, whereCount)
	}
}

func TestComplex_SearchOR_ReturnsAtLeastOnePart(t *testing.T) {
	input := generateBackendEvents(50)

	onlyErrors := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level = "ERROR" | stats count() as count`)
	if onlyErrors.ExitCode != 0 {
		t.Fatalf("exit %d: %s", onlyErrors.ExitCode, onlyErrors.Stderr)
	}
	errorCount := jsonCount(t, onlyErrors.Stdout)

	orResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`search source=main ERROR OR WARN | stats count() as count`)
	if orResult.ExitCode != 0 {
		t.Fatalf("search OR exit %d: %s", orResult.ExitCode, orResult.Stderr)
	}

	orCount := jsonCount(t, orResult.Stdout)
	if orCount < errorCount {
		t.Errorf("OR count (%d) < ERROR-only count (%d)", orCount, errorCount)
	}
}

// WHERE BETWEEN Tests

func TestComplex_WhereBetween_MinMaxInRange(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where duration_ms between 50 and 2000 | stats min(duration_ms) as min_dur, max(duration_ms) as max_dur, count()`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	minDur := jsonFieldFloat(t, r.Stdout, "min_dur")
	maxDur := jsonFieldFloat(t, r.Stdout, "max_dur")

	if minDur < 50 {
		t.Errorf("min_dur (%.1f) below lower bound (50)", minDur)
	}
	if maxDur > 2000 {
		t.Errorf("max_dur (%.1f) above upper bound (2000)", maxDur)
	}
}

// Streamstats Monotonicity Tests

func TestComplex_Streamstats_CumulativeMonotonic(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | sort duration_ms | streamstats sum(duration_ms) as cum_dur | fields duration_ms, cum_dur | head 15`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	prevCum := 0.0
	for i, row := range rows {
		cum, ok := toFloat(row["cum_dur"])
		if !ok {
			t.Fatalf("row %d: cum_dur is not a number: %v", i, row["cum_dur"])
		}
		if cum < prevCum-0.01 {
			t.Errorf("row %d: cumulative (%.1f) < previous (%.1f)", i, cum, prevCum)
		}
		prevCum = cum
	}
}

// Dedup Multi-Field Tests

func TestComplex_DedupMulti_CountMatchesDistinctGroups(t *testing.T) {
	input := generateBackendEvents(50)

	dedupResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | dedup level, service | stats count() as unique_pairs`)
	if dedupResult.ExitCode != 0 {
		t.Fatalf("dedup exit %d: %s", dedupResult.ExitCode, dedupResult.Stderr)
	}

	dcResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats dc(level) as levels, dc(service) as services`)
	if dcResult.ExitCode != 0 {
		t.Fatalf("dc exit %d: %s", dcResult.ExitCode, dcResult.Stderr)
	}

	dedupCount := int(jsonFieldFloat(t, dedupResult.Stdout, "unique_pairs"))
	levels := int(jsonFieldFloat(t, dcResult.Stdout, "levels"))
	services := int(jsonFieldFloat(t, dcResult.Stdout, "services"))

	maxPairs := levels * services
	if dedupCount > maxPairs {
		t.Errorf("dedup count (%d) > max possible pairs levels(%d)*services(%d)=%d",
			dedupCount, levels, services, maxPairs)
	}
	if dedupCount == 0 {
		t.Error("dedup returned 0 rows -- expected at least 1")
	}
}

// CASE Eval -- No Null Buckets

func TestComplex_EvalCase_NoNullBuckets(t *testing.T) {
	// CASE with an else branch puts every event in a bucket.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval bucket = case(duration_ms < 50, "fast", duration_ms < 500, "medium" else "slow") | stats count() as count by bucket`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 bucket")
	}

	total := 0
	for _, row := range rows {
		bucket, ok := row["bucket"].(string)
		if !ok || bucket == "" {
			t.Errorf("found null/empty bucket in row %v", row)
		}
		total += int(toFloatOr(row["count"], 0))
	}

	if total != 50 {
		t.Errorf("expected total=50, got %d (some events not bucketed)", total)
	}
}

// earliest/latest Aggregation Tests

func TestComplex_EarliestLatest_ByGroup(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats earliest(level) as first_level, latest(level) as last_level by service | sort service`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (one per service), got %d", len(rows))
	}

	for i, row := range rows {
		if _, ok := row["first_level"]; !ok {
			t.Errorf("row %d missing first_level", i)
		}
		if _, ok := row["last_level"]; !ok {
			t.Errorf("row %d missing last_level", i)
		}
		if _, ok := row["service"]; !ok {
			t.Errorf("row %d missing service", i)
		}
	}
}

// Large Input Stress Tests

func TestComplex_LargeInput_5000Events_DeepPipeline(t *testing.T) {
	input := generateBackendEvents(5000)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where level != "INFO" | stats count() as count, avg(duration_ms) as avg_dur, max(duration_ms) as max_dur by level, service | sort - count | head 20`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from 5000-event pipeline")
	}

	total := 0
	for i, row := range rows {
		curr, ok := toFloat(row["count"])
		if !ok {
			t.Fatalf("row %d: count is not numeric: %v", i, row["count"])
		}
		total += int(curr)
		if i > 0 && toFloatOr(rows[i-1]["count"], 0) < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, rows[i-1]["count"], curr)
		}
	}
	if total != 2000 {
		t.Errorf("groups add up to %d, want 2000 non-INFO events", total)
	}
}

// String Function Tests

func TestComplex_EvalStringFunctions_Chained(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval msg_upper = upper(message) | eval msg_len = length(msg_upper) | eval msg_short = substring(msg_upper, 1, 10) | fields msg_short, msg_len | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	for i, row := range rows {
		short, _ := row["msg_short"].(string)
		if len(short) != 10 {
			t.Errorf("row %d: substring produced %d chars, expected 10: %q", i, len(short), short)
		}
		if !strings.HasPrefix(short, "REQUEST-") {
			t.Errorf("row %d: msg_short %q is not the upper-cased message prefix", i, short)
		}
		msgLen := toFloatOr(row["msg_len"], 0)
		if msgLen < 10 {
			t.Errorf("row %d: msg_len (%v) < 10 (substring length)", i, msgLen)
		}
	}
}

func TestComplex_EvalStrftime_HourExtraction(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval hour = strftime(_time, "%H") | stats count() by hour | sort hour`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from strftime")
	}

	for i, row := range rows {
		hour, ok := row["hour"].(string)
		if !ok {
			t.Errorf("row %d: hour is not string: %v", i, row["hour"])
			continue
		}
		if len(hour) != 2 {
			t.Errorf("row %d: hour %q is not 2 digits", i, hour)
		}
	}
}

func TestComplex_EvalCIDRMatch(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval is_internal = if(cidrmatch(client_ip, "10.0.0.0/8"), "yes", "no") | stats count() by is_internal`)

	if r.ExitCode != 0 {
		t.Fatalf("cidrmatch: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from cidrmatch")
	}

	// Every generated IP is 10.x.x.x.
	for _, row := range rows {
		if row["is_internal"] != "yes" {
			t.Errorf("expected all IPs to be internal (10.x.x.x), got row %v", row)
		}
	}
}

func TestComplex_EvalTypeof(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval dur_type = typeof(duration_ms), status_type = typeof(status) | stats count() by dur_type, status_type`)

	if r.ExitCode != 0 {
		t.Fatalf("typeof: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from typeof")
	}
	for _, row := range rows {
		durType, _ := row["dur_type"].(string)
		if durType != "int" && durType != "float" {
			t.Errorf("duration_ms should be numeric type, got dur_type=%q", durType)
		}
	}
}

func TestComplex_EvalTypeof_MessageIsString(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval msg_type = typeof(message) | stats count() by msg_type | sort msg_type`)

	if r.ExitCode != 0 {
		t.Fatalf("typeof for message: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 1 || rows[0]["msg_type"] != "string" {
		t.Errorf("expected one msg_type=string group, got %v", rows)
	}
}

func TestComplex_EvalConcat(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | eval label = concat(level, ":", service) | fields label | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("concat: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from concat")
	}

	for i, row := range rows {
		label, ok := row["label"].(string)
		if !ok || label == "" {
			t.Errorf("row %d: expected non-empty label, got %v", i, row["label"])
		}
		if !strings.Contains(label, ":") {
			t.Errorf("row %d: concat label %q doesn't contain ':' separator", i, label)
		}
	}
}

// Aggregation Shapes

func TestComplex_SlowestServices(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats max(duration_ms) as max_duration_ms by service | sort - max_duration_ms | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (one per service), got %d", len(rows))
	}
	for i := 1; i < len(rows); i++ {
		if toFloatOr(rows[i-1]["max_duration_ms"], 0) < toFloatOr(rows[i]["max_duration_ms"], 0) {
			t.Errorf("not sorted descending at row %d: %v", i, rows)
		}
	}
}

func TestComplex_ErrorsByService(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | where lower(level) in ("error", "fatal") | stats count() as count by service`)

	if r.ExitCode != 0 {
		t.Fatalf("exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	total := 0
	for _, row := range mustParseJSON(t, r.Stdout) {
		total += int(toFloatOr(row["count"], 0))
	}
	if total != 10 {
		t.Errorf("errors add up to %d, want 10", total)
	}
}

func TestComplex_Describe(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json", `describe main`)

	if r.ExitCode != 0 {
		t.Fatalf("describe: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	found := false
	for _, row := range mustParseJSON(t, r.Stdout) {
		if row["COLUMN_NAME"] == "duration_ms" {
			found = true
		}
	}
	if !found {
		t.Errorf("describe output has no duration_ms column: %s", r.Stdout)
	}
}

func TestComplex_XYseries_AllCellsPresent(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`source=main | stats count() as c by level, service | xyseries level service in ("api-service", "auth-service", "gateway-service", "payment-service") c | sort level`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (one per level), got %d", len(rows))
	}

	// Each row has the level and one column per listed service.
	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level' column from xyseries", i)
		}
		if len(row) != 5 {
			t.Errorf("row %d has %d keys, expected level + 4 service columns: %v", i, len(row), row)
		}
	}
}

// Server-Mode Cross-Index Tests

func TestComplex_Server_CrossIndexCount_SumMatches(t *testing.T) {
	srv := startServer(t)

	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("frontend_console.log"), "idx_frontend")
	ingestFileWithIndex(t, srv, testdataLog("nginx_access.log"), "idx_nginx")

	backendCount := serverQueryCount(t, srv, "source=idx_backend | stats count() as count")
	frontendCount := serverQueryCount(t, srv, "source=idx_frontend | stats count() as count")
	nginxCount := serverQueryCount(t, srv, "source=idx_nginx | stats count() as count")

	allCount := serverQueryCount(t, srv, "source=* | stats count() as count")

	expected := backendCount + frontendCount + nginxCount
	if allCount != expected {
		t.Errorf("source=* count (%d) != sum of indexes (%d+%d+%d=%d)",
			allCount, backendCount, frontendCount, nginxCount, expected)
	}
}

func TestComplex_Server_CrossIndexAppend(t *testing.T) {
	srv := startServer(t)

	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("frontend_console.log"), "idx_frontend")
	ingestFileWithIndex(t, srv, testdataLog("nginx_access.log"), "idx_nginx")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=idx_backend | stats count() as backend_events | append [ source=idx_frontend | stats count() as frontend_events ] | append [ source=idx_nginx | stats count() as nginx_events ]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 append results, got %d. stdout: %s", len(rows), r.Stdout)
	}

	for i, row := range rows {
		nonZero := 0
		for _, v := range row {
			if f, ok := v.(float64); ok && f > 0 {
				nonZero++
			}
		}
		if nonZero != 1 {
			t.Errorf("row %d: expected exactly 1 non-zero count, got %d -- row: %v", i, nonZero, row)
		}
	}
}

func TestComplex_Server_CrossIndexUnion(t *testing.T) {
	// backend_server.log = 26 events, audit_security.log = 27.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("audit_security.log"), "idx_audit")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`union [ search source=idx_backend | stats count() as n ] [ search source=idx_audit | stats count() as n ]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 2 {
		t.Fatalf("cross-index union rows = %d, want 2. stdout: %s", len(rows), r.Stdout)
	}
	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["n"], 0))
	}
	if total != 26+27 {
		t.Errorf("cross-index union total = %d, want %d (26 backend + 27 audit)", total, 26+27)
	}
}

func TestComplex_Server_ScalarSubquery(t *testing.T) {
	// backend_server.log has 6 events slower than its average duration.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=idx_backend | where duration_ms > [ source=idx_backend | stats avg(duration_ms) ] | stats count() as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 6 {
		t.Errorf("scalar subquery count = %d, want 6", got)
	}
}

func TestComplex_Server_Join_CrossIndex(t *testing.T) {
	srv := startServer(t)

	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("audit_security.log"), "idx_audit")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=idx_backend | join type=inner service [ source=idx_audit | stats count() as audit_events by service ] | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("cross-index join: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

// Server-Mode Query Patterns

func TestComplex_Server_NestedAggregation(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | stats count() as events, avg(duration_ms) as avg_dur by service | eval is_slow = if(avg_dur > 500, "slow", "fast") | stats count() as svc_count by is_slow`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from nested aggregation")
	}
}

func TestComplex_Server_Percentiles(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | stats p50(duration_ms) as p50, p90(duration_ms) as p90, p99(duration_ms) as p99`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	p50 := jsonFieldFloat(t, r.Stdout, "p50")
	p90 := jsonFieldFloat(t, r.Stdout, "p90")
	p99 := jsonFieldFloat(t, r.Stdout, "p99")

	if p50 > p90 {
		t.Errorf("p50 (%.1f) > p90 (%.1f)", p50, p90)
	}
	if p90 > p99 {
		t.Errorf("p90 (%.1f) > p99 (%.1f)", p90, p99)
	}
}

func TestComplex_Server_DeepPipeline(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("access.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | where level != "INFO" | eval host_prefix = lower(substring(host, 1, 4)) | stats count() as n, avg(response_time) as avg_rt by level, host_prefix | sort - n | eval quality = concat(level, "-", host_prefix) | fields quality, n, avg_rt`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from deep pipeline")
	}

	for i := 1; i < len(rows); i++ {
		prev := toFloatOr(rows[i-1]["n"], 0)
		curr := toFloatOr(rows[i]["n"], 0)
		if prev < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prev, curr)
		}
	}
}

func TestComplex_Server_EvalReplace(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | eval clean = replace(message, "[aeiou]", "*") | eval has_star = if(like(clean, "%*%"), "yes", "no") | stats count() by has_star`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}
}

func TestComplex_Server_SearchAND(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`search source=main payment failed | stats count() as count`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got == 0 {
		t.Error("expected some results for search payment AND failed")
	}
}

func TestComplex_Server_SearchNOT(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`search source=main payment NOT success | stats count() as count`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got == 0 {
		t.Error("expected some results for search payment NOT success")
	}
}

func TestComplex_Server_WhereBetween(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | where duration_ms between 50 and 6000 | stats min(duration_ms) as min_dur, max(duration_ms) as max_dur, count() as count`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	row := rows[0]
	count := toFloatOr(row["count"], 0)
	if count == 0 {
		t.Fatal("expected at least 1 event in range 50-6000ms")
	}

	minV := row["min_dur"]
	maxV := row["max_dur"]

	minDur, ok1 := toFloat(minV)
	maxDur, ok2 := toFloat(maxV)
	if !ok1 || !ok2 {
		t.Fatalf("min/max are not numeric: min=%v, max=%v", minV, maxV)
	}

	if minDur < 50 {
		t.Errorf("min_dur (%.1f) below lower bound (50)", minDur)
	}
	if maxDur > 6000 {
		t.Errorf("max_dur (%.1f) above upper bound (6000)", maxDur)
	}
}

func TestComplex_Server_Patterns(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`source=main | patterns message | fields message, patterns_field | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("patterns command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from patterns")
	}
	for i, row := range rows {
		if p, _ := row["patterns_field"].(string); p == "" {
			t.Errorf("row %d has no patterns_field: %v", i, row)
		}
	}
}

// Helpers

// serverQueryCount runs a server query and returns the count.
func serverQueryCount(t *testing.T, srv *Server, query string) int {
	t.Helper()

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json", query)
	if r.ExitCode != 0 {
		t.Fatalf("server query %q failed (exit %d): %s", query, r.ExitCode, r.Stderr)
	}

	return jsonCount(t, r.Stdout)
}

// toFloat converts an interface{} to float64. Returns (value, true) on success.
func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case int64:
		return float64(val), true
	case json.Number:
		if f, err := val.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}

// toFloatOr converts to float64 with a default.
func toFloatOr(v interface{}, def float64) float64 {
	f, ok := toFloat(v)
	if !ok {
		return def
	}
	return f
}
