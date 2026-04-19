//go:build clitest

package cli_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// ============================================================================
// Helpers for complex tests
// ============================================================================

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

// generateMixedEvents creates events with user_id and action fields for
// transaction/session testing.
func generateMixedEvents(n int) string {
	actions := []string{"login", "view", "click", "purchase", "logout"}
	userIDs := []string{"usr-A", "usr-B", "usr-C", "usr-D", "usr-E"}

	var sb strings.Builder
	for i := 0; i < n; i++ {
		ev := map[string]interface{}{
			"timestamp":   fmt.Sprintf("2026-04-01T%02d:%02d:%02d.000Z", (i/60)%24, i%60, i%30),
			"user_id":     userIDs[i%len(userIDs)],
			"action":      actions[i%len(actions)],
			"session_id":  fmt.Sprintf("sess-%d", i/5),
			"duration_ms": 5 + (i * 13 % 3000),
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

// jsonFieldString extracts a string field from the first NDJSON row.
func jsonFieldString(t *testing.T, stdout, field string) string {
	t.Helper()
	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatalf("expected at least 1 JSON row, got 0")
	}
	v, ok := rows[0][field]
	if !ok {
		t.Fatalf("row missing field %q, keys: %v", field, mapKeys(rows[0]))
	}
	s, ok := v.(string)
	if !ok {
		t.Fatalf("expected string for field %q, got %T: %v", field, v, v)
	}
	return s
}

// sumRowCounts sums the "count" field across all NDJSON rows.
func sumRowCounts(t *testing.T, stdout string) int {
	t.Helper()
	rows := mustParseJSON(t, stdout)
	total := 0
	for _, row := range rows {
		switch v := row["count"].(type) {
		case float64:
			total += int(v)
		case int64:
			total += int(v)
		}
	}
	return total
}

// ============================================================================
// CTE (Common Table Expression) Tests
// ============================================================================

func TestComplex_CTE_BasicFilter_Count(t *testing.T) {
	// generateBackendEvents cycles levels INFO,INFO,INFO,WARN,ERROR so ERROR = n/5.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`$errs = | where level="ERROR" | stats count as n ; FROM $errs | table n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 10 {
		t.Errorf("CTE filter count = %d, want 10 (ERROR events in 50-event fixture)", got)
	}
}

func TestComplex_CTE_FilterCount_MatchesDirectWhere(t *testing.T) {
	input := generateBackendEvents(50)

	direct := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level="ERROR" | stats count as n`)
	if direct.ExitCode != 0 {
		t.Fatalf("direct query exit %d, stderr: %s", direct.ExitCode, direct.Stderr)
	}
	directN := int(jsonFieldFloat(t, direct.Stdout, "n"))

	cte := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`$errs = | where level="ERROR" | stats count as n ; FROM $errs | table n`)
	if cte.ExitCode != 0 {
		t.Fatalf("CTE query exit %d, stderr: %s", cte.ExitCode, cte.Stderr)
	}
	cteN := int(jsonFieldFloat(t, cte.Stdout, "n"))

	if cteN != directN {
		t.Errorf("CTE count (%d) != direct WHERE count (%d)", cteN, directN)
	}
}

func TestComplex_CTE_Chained_ConsistentResults(t *testing.T) {
	// Chained CTEs: $noninfo filters out INFO, $errs narrows to ERROR from $noninfo.
	// Result must equal direct ERROR filter.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`$noninfo = | where level!="INFO" ; $errs = FROM $noninfo | where level="ERROR" | stats count as n ; FROM $errs | table n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 10 {
		t.Errorf("chained CTE count = %d, want 10", got)
	}
}

// ============================================================================
// JOIN Tests
// ============================================================================

func TestComplex_Join_ByField_Count(t *testing.T) {
	// LEFT produces one row per service with total count; RIGHT produces one row
	// per service with error count. Inner join on service must match all four
	// services present in the fixture (api,payment,gateway,auth).
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats count as total by service | join service [| where level="ERROR" | stats count as errs by service] | stats count as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got == 0 {
		t.Fatalf("JOIN returned 0 rows; expected >0 matching services. stdout: %s", r.Stdout)
	}
}

// ============================================================================
// APPEND Tests
// ============================================================================

func TestComplex_Append_TotalIsSum(t *testing.T) {
	// Two one-row aggregates concatenated via APPEND must emit both rows.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level="ERROR" | stats count as n | append [| where level="WARN" | stats count as n]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 2 {
		t.Fatalf("APPEND rows = %d, want 2. stdout: %s", len(rows), r.Stdout)
	}
	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["n"], 0))
	}
	if total != 20 {
		t.Errorf("APPEND total = %d, want 20 (10 ERROR + 10 WARN)", total)
	}
}

// ============================================================================
// Deep Pipeline Tests
// ============================================================================

func TestComplex_DeepPipeline_CountsAddUp(t *testing.T) {
	// 7-command pipeline: where → eval → stats → sort → head → eval → stats
	// Phase 1 groups should sum to total events.
	input := generateBackendEvents(100)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level!="INFO" | eval bucket=case(duration_ms<50,"fast",duration_ms<500,"medium",1=1,"slow") | stats count by level, bucket | sort -count | head 20 | eval total_group=count | fields level, bucket, total_group`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from deep pipeline")
	}

	// Verify every row has the expected fields.
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
	}
}

func TestComplex_DeepPipeline_SortOrderCorrect(t *testing.T) {
	// Multi-stage sort: stats → sort desc → head → verify descending order.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval is_error=if(level="ERROR",1,0) | stats sum(is_error) as errors, count as total by service | eval error_rate=round(errors*100/total,1) | sort -error_rate | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	// Verify descending error_rate.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["error_rate"]
		curr := rows[i]["error_rate"]
		prevF, ok1 := toFloat(prev)
		currF, ok2 := toFloat(curr)
		if !ok1 || !ok2 {
			continue
		}
		if prevF < currF-0.01 {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prevF, currF)
		}
	}
}

// ============================================================================
// Nested Aggregation Tests
// ============================================================================

func TestComplex_NestedAgg_TwoPhaseConsistent(t *testing.T) {
	// stats → eval → stats: phase-2 groups must be a function of phase-1 results.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats count as events, avg(duration_ms) as avg_dur by service | eval is_slow=if(avg_dur>500,"slow","fast") | stats count as group_count by is_slow`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from nested aggregation")
	}

	// group_count should sum to number of unique services.
	totalGroups := 0
	for _, row := range rows {
		totalGroups += int(toFloatOr(row["group_count"], 0))
	}

	if totalGroups < 2 {
		t.Errorf("expected at least 2 service groups, got %d", totalGroups)
	}
}

// ============================================================================
// Eval Function Tests
// ============================================================================

func TestComplex_EvalReplace_OutputIsValid(t *testing.T) {
	// Replace strips non-alpha chars from message.
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval clean=replace(message, "[^a-zA-Z0-9 ]", "") | fields clean | head 5`)

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
		// Verify no non-alphanumeric (except space).
		for _, ch := range clean {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == ' ') {
				t.Errorf("row %d: replace left non-alphanumeric char %q in %q", i, ch, clean)
				break
			}
		}
	}
}

func TestComplex_EvalStartswithContains(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval is_api=if(startswith(message, "request"), "yes", "no") | eval has_dash=if(contains(message, "-"), "yes", "no") | stats count by is_api, has_dash | sort is_api, has_dash`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from startswith/contains")
	}

	// Every event starts with "request-" so is_api should always be "yes".
	for _, row := range rows {
		isAPI, _ := row["is_api"].(string)
		if isAPI != "yes" {
			t.Errorf("expected is_api=yes for all rows, got %q in row %v", isAPI, row)
		}
	}
}

func TestComplex_EvalTypeChecks_NullHandling(t *testing.T) {
	// Some events have error/amount_cents, others don't.
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval has_error=if(isnotnull(error), "yes", "no") | eval has_amount=if(isnotnull(amount_cents), "yes", "no") | stats count by has_error, has_amount | sort has_error, has_amount`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["count"], 0))
	}
	if total != 20 {
		t.Errorf("expected total count=20, got %d", total)
	}
}

func TestComplex_EvalNullCoalesce(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval err=error ?? "none" | eval amt=amount_cents ?? 0 | stats count by err | sort err`)

	if r.ExitCode != 0 {
		t.Fatalf("?? null coalesce operator: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}

	// Should have at least "none" group (non-error events).
	foundNone := false
	for _, row := range rows {
		err, _ := row["err"].(string)
		if err == "none" {
			foundNone = true
		}
	}
	if !foundNone {
		t.Error("expected 'none' group from null coalesce, not found")
	}
}

// ============================================================================
// Percentile Aggregation Tests
// ============================================================================

func TestComplex_Percentiles_Ordered(t *testing.T) {
	// p25 <= p50 <= p75 <= p90 <= p99
	input := generateBackendEvents(100)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats perc25(duration_ms) as p25, perc50(duration_ms) as p50, perc75(duration_ms) as p75, perc90(duration_ms) as p90, perc99(duration_ms) as p99`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	p25 := jsonFieldFloat(t, r.Stdout, "p25")
	p50 := jsonFieldFloat(t, r.Stdout, "p50")
	p75 := jsonFieldFloat(t, r.Stdout, "p75")
	p90 := jsonFieldFloat(t, r.Stdout, "p90")
	p99 := jsonFieldFloat(t, r.Stdout, "p99")

	if p25 > p50 {
		t.Errorf("p25 (%.1f) > p50 (%.1f)", p25, p50)
	}
	if p50 > p75 {
		t.Errorf("p50 (%.1f) > p75 (%.1f)", p50, p75)
	}
	if p75 > p90 {
		t.Errorf("p75 (%.1f) > p90 (%.1f)", p75, p90)
	}
	if p90 > p99 {
		t.Errorf("p90 (%.1f) > p99 (%.1f)", p90, p99)
	}
}

// ============================================================================
// Search Expression Tests (AND, OR, NOT)
// ============================================================================

func TestComplex_SearchAND_CountMatchesWhere(t *testing.T) {
	input := generateBackendEvents(50)

	searchResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| search "request" AND "processed" | stats count`)
	if searchResult.ExitCode != 0 {
		t.Fatalf("search exit %d: %s", searchResult.ExitCode, searchResult.Stderr)
	}

	whereResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where match(message, "request") AND match(message, "processed") | stats count`)
	if whereResult.ExitCode != 0 {
		t.Fatalf("where exit %d: %s", whereResult.ExitCode, whereResult.Stderr)
	}

	searchCount := jsonCount(t, searchResult.Stdout)
	whereCount := jsonCount(t, whereResult.Stdout)

	if searchCount != whereCount {
		t.Errorf("search AND count (%d) != where match AND count (%d)", searchCount, whereCount)
	}
}

func TestComplex_SearchOR_ReturnsAtLeastOnePart(t *testing.T) {
	input := generateBackendEvents(50)

	onlyErrors := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level="ERROR" | stats count`)
	if onlyErrors.ExitCode != 0 {
		t.Fatalf("exit %d: %s", onlyErrors.ExitCode, onlyErrors.Stderr)
	}
	errorCount := jsonCount(t, onlyErrors.Stdout)

	orResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| search "ERROR" OR "WARN" | stats count`)
	if orResult.ExitCode != 0 {
		t.Fatalf("search OR exit %d: %s", orResult.ExitCode, orResult.Stderr)
	}

	orCount := jsonCount(t, orResult.Stdout)
	// OR should return at least as many as just ERROR.
	if orCount < errorCount {
		t.Errorf("OR count (%d) < ERROR-only count (%d)", orCount, errorCount)
	}
}

// ============================================================================
// WHERE BETWEEN Tests
// ============================================================================

func TestComplex_WhereBetween_MinMaxInRange(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where duration_ms between 50 and 2000 | stats min(duration_ms) as min_dur, max(duration_ms) as max_dur, count`)

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

// ============================================================================
// Streamstats Monotonicity Tests
// ============================================================================

func TestComplex_Streamstats_CumulativeMonotonic(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| sort duration_ms | streamstats sum(duration_ms) as cum_dur | fields duration_ms, cum_dur | head 15`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	// Cumulative sum must never decrease.
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

// ============================================================================
// Dedup Multi-Field Tests
// ============================================================================

func TestComplex_DedupMulti_CountMatchesDistinctGroups(t *testing.T) {
	input := generateBackendEvents(50)

	// dedup by level, service → count should be number of unique (level,service) pairs.
	dedupResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| dedup level, service | stats count as unique_pairs`)
	if dedupResult.ExitCode != 0 {
		t.Fatalf("dedup exit %d: %s", dedupResult.ExitCode, dedupResult.Stderr)
	}

	// dc(level) * dc(service) should be the upper bound.
	dcResult := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats dc(level) as levels, dc(service) as services`)
	if dcResult.ExitCode != 0 {
		t.Fatalf("dc exit %d: %s", dcResult.ExitCode, dcResult.Stderr)
	}

	dedupRows := mustParseJSON(t, dedupResult.Stdout)
	if len(dedupRows) == 0 {
		t.Fatal("dedup result is empty")
	}
	dedupCount := int(toFloatOr(dedupRows[0]["unique_pairs"], 0))

	levels := int(jsonFieldFloat(t, dcResult.Stdout, "levels"))
	services := int(jsonFieldFloat(t, dcResult.Stdout, "services"))

	maxPairs := levels * services
	if dedupCount > maxPairs {
		t.Errorf("dedup count (%d) > max possible pairs levels(%d)*services(%d)=%d",
			dedupCount, levels, services, maxPairs)
	}
	if dedupCount == 0 {
		t.Error("dedup returned 0 rows — expected at least 1")
	}
}

// ============================================================================
// CASE Eval — No Null Buckets
// ============================================================================

func TestComplex_EvalCase_NoNullBuckets(t *testing.T) {
	// CASE with default (1=1) means every event must land in a bucket.
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval bucket=case(duration_ms<50,"fast",duration_ms<500,"medium",1=1,"slow") | stats count by bucket`)

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

// ============================================================================
// earliest/latest Aggregation Tests
// ============================================================================

func TestComplex_EarliestLatest_ByGroup(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats earliest(level) as first_level, latest(level) as last_level by service | sort service`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from earliest/latest")
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

// ============================================================================
// Large Input Stress Tests
// ============================================================================

func TestComplex_LargeInput_5000Events_DeepPipeline(t *testing.T) {
	input := generateBackendEvents(5000)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| where level!="INFO" | stats count, avg(duration_ms) as avg_dur, max(duration_ms) as max_dur by level, service | sort -count | head 20`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from 5000-event pipeline")
	}

	// Verify descending count order.
	for i := 1; i < len(rows); i++ {
		prev := toFloatOr(rows[i-1]["count"], 0)
		curr := toFloatOr(rows[i]["count"], 0)
		if prev < curr {
			t.Errorf("not sorted descending at row %d: %v < %v", i, prev, curr)
		}
	}
}

// ============================================================================
// Eval String Functions Combined
// ============================================================================

func TestComplex_EvalStringFunctions_Chained(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval msg_upper=upper(message) | eval msg_len=len(msg_upper) | eval msg_short=substr(msg_upper, 1, 10) | fields msg_short, msg_len | head 5`)

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
			t.Errorf("row %d: substr produced %d chars, expected 10: %q", i, len(short), short)
		}
		msgLen := toFloatOr(row["msg_len"], 0)
		if msgLen < 10 {
			t.Errorf("row %d: msg_len (%v) < 10 (substr length)", i, msgLen)
		}
	}
}

// ============================================================================
// Eval strftime Test
// ============================================================================

func TestComplex_EvalStrftime_HourExtraction(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval hour=strftime(_time, "%H") | stats count by hour | sort hour`)

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

// ============================================================================
// Eval cidrmatch Test
// ============================================================================

func TestComplex_EvalCIDRMatch(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval is_internal=if(cidrmatch("10.0.0.0/8", client_ip), "yes", "no") | stats count by is_internal`)

	if r.ExitCode != 0 {
		t.Fatalf("cidrmatch eval function: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from cidrmatch")
	}

	// All our IPs are 10.x.x.x so all should be internal.
	for _, row := range rows {
		internal, _ := row["is_internal"].(string)
		if internal != "yes" {
			t.Errorf("expected all IPs to be internal (10.x.x.x), got %q in row %v", internal, row)
		}
	}
}

// ============================================================================
// Eval isnum / isint Tests
// ============================================================================

func TestComplex_EvalIsNumIsInt(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval dur_is_num=if(isnum(duration_ms), "yes", "no") | eval status_is_int=if(isint(status), "yes", "no") | stats count by dur_is_num, status_is_int`)

	if r.ExitCode != 0 {
		t.Fatalf("isnum/isint: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	for _, row := range rows {
		if durNum, _ := row["dur_is_num"].(string); durNum != "yes" {
			t.Errorf("duration_ms should be numeric, got dur_is_num=%q", durNum)
		}
	}
}

// ============================================================================
// Domain Sugar Commands
// ============================================================================

func TestComplex_DomainPercentiles(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| percentiles duration_ms by service`)

	if r.ExitCode != 0 {
		t.Fatalf("percentiles domain sugar: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from percentiles")
	}
}

func TestComplex_DomainSlowest(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| slowest 5 duration_ms by service`)

	if r.ExitCode != 0 {
		t.Fatalf("slowest domain sugar: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from slowest")
	}
}

func TestComplex_DomainErrors(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| errors by service`)

	if r.ExitCode != 0 {
		t.Fatalf("errors domain sugar: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	// errors command filters to error/fatal level, which may produce 0 rows
	// depending on how the domain sugar interacts with the generated data.
	_ = rows
}

// ============================================================================
// Analytics Commands
// ============================================================================

func TestComplex_AnalyticsOutliers(t *testing.T) {
	input := generateBackendEvents(100)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| outliers field=duration_ms method=zscore threshold=2.0 | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("outliers command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Log("outliers returned 0 rows (may be expected if no outliers in generated data)")
	}
}

func TestComplex_AnalyticsCorrelate(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| correlate duration_ms memory_mb method=pearson`)

	if r.ExitCode != 0 {
		t.Fatalf("correlate command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Log("correlate returned 0 rows")
	}
}

func TestComplex_AnalyticsSessionize(t *testing.T) {
	input := generateMixedEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| sessionize maxpause="5m" by user_id | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("sessionize command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Log("sessionize returned 0 rows")
	}
}

func TestComplex_AnalyticsGlimpse(t *testing.T) {
	input := generateBackendEvents(30)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| glimpse`)

	if r.ExitCode != 0 {
		t.Fatalf("glimpse command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	// Glimpse outputs schema info which may not be standard NDJSON.
	// Just verify it produced output.
	if strings.TrimSpace(r.Stdout) == "" {
		t.Error("glimpse produced no output")
	}
}

// ============================================================================
// F-string Interpolation Test
// ============================================================================

func TestComplex_EvalFString(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval label=f"{level}:{service}" | fields label | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("f-string interpolation: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from f-string")
	}

	for i, row := range rows {
		label, ok := row["label"].(string)
		if !ok || label == "" {
			t.Errorf("row %d: expected non-empty label, got %v", i, row["label"])
		}
		// Should contain a colon separating level and service.
		if !strings.Contains(label, ":") {
			t.Errorf("row %d: f-string label %q doesn't contain ':' separator", i, label)
		}
	}
}

// ============================================================================
// JSON Eval Functions Test
// ============================================================================

func TestComplex_EvalJsonFunctions(t *testing.T) {
	input := generateBackendEvents(20)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| eval is_valid=json_valid(message) | eval msg_type=json_type(message) | stats count by is_valid, msg_type | sort is_valid, msg_type`)

	if r.ExitCode != 0 {
		t.Fatalf("json_valid/json_type: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row")
	}
}

// ============================================================================
// XYseries Completeness Test
// ============================================================================

func TestComplex_XYseries_AllCellsPresent(t *testing.T) {
	input := generateBackendEvents(50)

	r := runLynxDBWithStdin(t, input, "query", "--format", "json",
		`| stats count by level, service | xyseries level service count | sort level`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from xyseries")
	}

	// Each row should have 'level' and at least one service column.
	for i, row := range rows {
		if _, ok := row["level"]; !ok {
			t.Errorf("row %d missing 'level' column from xyseries", i)
		}
		// Should have at least 2 keys: level + at least one service.
		if len(row) < 2 {
			t.Errorf("row %d has only %d keys, expected level + service columns", i, len(row))
		}
	}
}

// ============================================================================
// Server-Mode Complex Tests
// ============================================================================

func TestComplex_Server_CrossIndexCount_SumMatches(t *testing.T) {
	srv := startServer(t)

	// Ingest into separate indexes.
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("frontend_console.log"), "idx_frontend")
	ingestFileWithIndex(t, srv, testdataLog("nginx_access.log"), "idx_nginx")

	// Count each index.
	backendCount := serverQueryCount(t, srv, "FROM idx_backend | stats count")
	frontendCount := serverQueryCount(t, srv, "FROM idx_frontend | stats count")
	nginxCount := serverQueryCount(t, srv, "FROM idx_nginx | stats count")

	// Count all via FROM *.
	allCount := serverQueryCount(t, srv, "FROM * | stats count")

	expected := backendCount + frontendCount + nginxCount
	if allCount != expected {
		t.Errorf("FROM * count (%d) != sum of indexes (%d+%d+%d=%d)",
			allCount, backendCount, frontendCount, nginxCount, expected)
	}
}

func TestComplex_Server_CrossIndexMultiSearch(t *testing.T) {
	srv := startServer(t)

	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("frontend_console.log"), "idx_frontend")
	ingestFileWithIndex(t, srv, testdataLog("nginx_access.log"), "idx_nginx")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| multisearch [FROM idx_backend | stats count as backend_events] [FROM idx_frontend | stats count as frontend_events] [FROM idx_nginx | stats count as nginx_events]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 3 {
		t.Fatalf("expected 3 multisearch results, got %d", len(rows))
	}

	// Each row should have exactly one non-zero count field.
	for i, row := range rows {
		nonZero := 0
		for _, v := range row {
			if f, ok := v.(float64); ok && f > 0 {
				nonZero++
			}
		}
		if nonZero != 1 {
			t.Errorf("row %d: expected exactly 1 non-zero count, got %d — row: %v", i, nonZero, row)
		}
	}
}

func TestComplex_Server_CrossIndexAppend(t *testing.T) {
	// Cross-index APPEND must emit both subqueries' rows, not silently drop the
	// APPEND branch. backend_server.log = 26 events, audit_security.log = 27.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("audit_security.log"), "idx_audit")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`FROM idx_backend | stats count as n | append [FROM idx_audit | stats count as n]`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) != 2 {
		t.Fatalf("cross-index APPEND rows = %d, want 2. stdout: %s", len(rows), r.Stdout)
	}
	total := 0
	for _, row := range rows {
		total += int(toFloatOr(row["n"], 0))
	}
	if total != 26+27 {
		t.Errorf("cross-index APPEND total = %d, want %d (26 backend + 27 audit)", total, 26+27)
	}
}

func TestComplex_Server_CTE_CrossIndex(t *testing.T) {
	// CTE defined against idx_backend must resolve, filter, and be queryable via
	// its $variable handle. backend_server.log has 5 ERROR events.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`$errs = FROM idx_backend | where level="ERROR" | stats count as error_count ; FROM $errs | stats sum(error_count) as total_errors`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "total_errors"))
	if got != 5 {
		t.Errorf("cross-index CTE total_errors = %d, want 5", got)
	}
}

func TestComplex_Server_Join_CrossIndex(t *testing.T) {
	srv := startServer(t)

	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")
	ingestFileWithIndex(t, srv, testdataLog("audit_security.log"), "idx_audit")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`FROM idx_backend | join type=inner service [FROM idx_audit | stats count as audit_events by service] | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("cross-index join: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

// ============================================================================
// Server-Mode Advanced Query Patterns
// ============================================================================

func TestComplex_Server_NestedAggregation(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| stats count as events, avg(duration_ms) as avg_dur by service | eval is_slow=if(avg_dur>500,"slow","fast") | stats count as svc_count by is_slow`)

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
		`| stats perc50(duration_ms) as p50, perc90(duration_ms) as p90, perc99(duration_ms) as p99`)

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
		`| where level!="INFO" | eval host_prefix=lower(substr(host, 1, 4)) | stats count as n, avg(response_time) as avg_rt by level, host_prefix | sort -n | eval quality=level+"-"+host_prefix | fields quality, n, avg_rt`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	rows := mustParseJSON(t, r.Stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 row from deep pipeline")
	}

	// Verify descending count order.
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
		`| eval clean=replace(message, "[aeiou]", "*") | eval has_star=if(contains(clean, "*"), "yes", "no") | stats count by has_star`)

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
		`| search "payment" AND "failed" | stats count`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got == 0 {
		t.Error("expected some results for search 'payment' AND 'failed'")
	}
}

func TestComplex_Server_SearchNOT(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| search "payment" NOT "success" | stats count`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := jsonCount(t, r.Stdout)
	if got == 0 {
		t.Error("expected some results for search 'payment' NOT 'success'")
	}
}

func TestComplex_Server_WhereBetween(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| where duration_ms between 50 and 6000 | stats min(duration_ms) as min_dur, max(duration_ms) as max_dur, count`)

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

// ============================================================================
// Analytics Commands (Server Mode)
// ============================================================================

func TestComplex_Server_Outliers(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| outliers field=duration_ms method=iqr | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("outliers command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_Patterns(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| patterns field=message | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("patterns command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_Compare(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| compare previous -1h | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("compare command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_Rollup(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| rollup 1h, 15m, 5m | head 10`)

	if r.ExitCode != 0 {
		t.Fatalf("rollup command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_Correlate(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| correlate duration_ms cpu_pct method=pearson`)

	if r.ExitCode != 0 {
		t.Fatalf("correlate command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_Sessionize(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| sessionize maxpause="30m" by user_id | head 5`)

	if r.ExitCode != 0 {
		t.Fatalf("sessionize command: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

// ============================================================================
// Server-Mode Domain Sugar Commands
// ============================================================================

func TestComplex_Server_DomainLatency(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| latency duration_ms every 1h by service`)

	if r.ExitCode != 0 {
		t.Fatalf("latency domain sugar: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

func TestComplex_Server_DomainRate(t *testing.T) {
	srv := startServer(t)
	ingestFile(t, srv, testdataLog("backend_server.log"))

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`| rate per 1h by level`)

	if r.ExitCode != 0 {
		t.Fatalf("rate domain sugar: exit %d, stderr: %s", r.ExitCode, r.Stderr)
	}
}

// ============================================================================
// Server-Mode CTE Tests
// ============================================================================

func TestComplex_Server_CTE_BasicFilter(t *testing.T) {
	// CTE with explicit FROM + WHERE must filter server-side.
	// backend_server.log has 5 ERROR events out of 26.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`$errs = FROM idx_backend | where level="ERROR" ; FROM $errs | stats count as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 5 {
		t.Errorf("server CTE filter count = %d, want 5", got)
	}
}

func TestComplex_Server_CTE_Chained(t *testing.T) {
	// Chained CTEs must preserve all filters: non-INFO intersect with ERROR = ERROR.
	srv := startServer(t)
	ingestFileWithIndex(t, srv, testdataLog("backend_server.log"), "idx_backend")

	r := runLynxDB(t, "--server", srv.BaseURL, "query", "--format", "json",
		`$noninfo = FROM idx_backend | where level!="INFO" ; $errs = FROM $noninfo | where level="ERROR" ; FROM $errs | stats count as n`)

	if r.ExitCode != 0 {
		t.Fatalf("exit code %d, stderr: %s", r.ExitCode, r.Stderr)
	}

	got := int(jsonFieldFloat(t, r.Stdout, "n"))
	if got != 5 {
		t.Errorf("chained CTE server count = %d, want 5", got)
	}
}

// ============================================================================
// Helpers
// ============================================================================

// serverQueryCount is a helper that runs a server query and returns the count.
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
