package views

import (
	"strings"
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

func TestAnalyzeQuery_ProjectionView(t *testing.T) {
	// No aggregation — all commands are streaming.
	analysis, err := AnalyzeQuery(`FROM main | where level="error" | eval sev=upper(level)`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if analysis.SourceIndex != "main" {
		t.Errorf("SourceIndex: got %q, want %q", analysis.SourceIndex, "main")
	}
	if analysis.IsAggregation {
		t.Error("expected IsAggregation=false for projection view")
	}
	if analysis.AggSpec != nil {
		t.Error("expected nil AggSpec for projection view")
	}
	if len(analysis.StreamingCmds) != 2 {
		t.Errorf("StreamingCmds: got %d, want 2", len(analysis.StreamingCmds))
	}
	if len(analysis.QueryCmds) != 0 {
		t.Errorf("QueryCmds: got %d, want 0", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_StatsCount(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM main | stats count by host`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if analysis.SourceIndex != "main" {
		t.Errorf("SourceIndex: got %q, want %q", analysis.SourceIndex, "main")
	}
	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	if analysis.AggSpec == nil {
		t.Fatal("expected non-nil AggSpec")
	}
	if len(analysis.AggSpec.Funcs) != 1 {
		t.Fatalf("Funcs: got %d, want 1", len(analysis.AggSpec.Funcs))
	}
	if analysis.AggSpec.Funcs[0].Name != "count" {
		t.Errorf("Func name: got %q, want %q", analysis.AggSpec.Funcs[0].Name, "count")
	}
	if len(analysis.AggSpec.GroupBy) != 1 || analysis.AggSpec.GroupBy[0] != "host" {
		t.Errorf("GroupBy: got %v, want [host]", analysis.AggSpec.GroupBy)
	}
	if len(analysis.StreamingCmds) != 0 {
		t.Errorf("StreamingCmds: got %d, want 0", len(analysis.StreamingCmds))
	}
}

func TestAnalyzeQuery_StatsWithStreamingCmds(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM nginx | where status>=500 | stats count, avg(duration) by uri`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	if len(analysis.StreamingCmds) != 1 {
		t.Errorf("StreamingCmds: got %d, want 1 (where)", len(analysis.StreamingCmds))
	}
	if analysis.AggSpec == nil {
		t.Fatal("expected non-nil AggSpec")
	}
	if len(analysis.AggSpec.Funcs) != 2 {
		t.Fatalf("Funcs: got %d, want 2", len(analysis.AggSpec.Funcs))
	}
	if analysis.AggSpec.Funcs[0].Name != "count" {
		t.Errorf("Func[0].Name: got %q, want %q", analysis.AggSpec.Funcs[0].Name, "count")
	}
	if analysis.AggSpec.Funcs[1].Name != "avg" {
		t.Errorf("Func[1].Name: got %q, want %q", analysis.AggSpec.Funcs[1].Name, "avg")
	}
	if analysis.AggSpec.Funcs[1].Field != "duration" {
		t.Errorf("Func[1].Field: got %q, want %q", analysis.AggSpec.Funcs[1].Field, "duration")
	}
}

func TestAnalyzeQuery_StatsWithPostCmds(t *testing.T) {
	// Commands after stats are query-time.
	analysis, err := AnalyzeQuery(`FROM main | stats count by host | sort -count | head 10`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	if len(analysis.QueryCmds) != 2 {
		t.Errorf("QueryCmds: got %d, want 2 (sort + head)", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_TopCommand(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM main | top 5 uri`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	if analysis.AggSpec == nil {
		t.Fatal("expected non-nil AggSpec")
	}
	// Top decomposes to: stats count by uri → sort -count → head 5.
	if len(analysis.AggSpec.Funcs) != 1 || analysis.AggSpec.Funcs[0].Name != "count" {
		t.Errorf("expected count func, got %v", analysis.AggSpec.Funcs)
	}
	if len(analysis.AggSpec.GroupBy) != 1 || analysis.AggSpec.GroupBy[0] != "uri" {
		t.Errorf("GroupBy: got %v, want [uri]", analysis.AggSpec.GroupBy)
	}
	if len(analysis.QueryCmds) != 2 {
		t.Errorf("QueryCmds: got %d, want 2 (sort + head)", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_RareCommand(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM main | rare 3 uri`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	// Rare decomposes to: stats count by uri → sort +count → head 3.
	if len(analysis.QueryCmds) != 2 {
		t.Errorf("QueryCmds: got %d, want 2 (sort + head)", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_TimechartCommand(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM main | timechart count`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
	if analysis.AggSpec == nil {
		t.Fatal("expected non-nil AggSpec")
	}
	// Timechart groups by _time.
	if len(analysis.AggSpec.GroupBy) == 0 || analysis.AggSpec.GroupBy[0] != "_time" {
		t.Errorf("GroupBy[0]: got %v, want _time", analysis.AggSpec.GroupBy)
	}
	// Implicit sort by _time at query time.
	if len(analysis.QueryCmds) != 1 {
		t.Errorf("QueryCmds: got %d, want 1 (sort _time)", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_SourceIndex(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM nginx | stats count`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if analysis.SourceIndex != "nginx" {
		t.Errorf("SourceIndex: got %q, want %q", analysis.SourceIndex, "nginx")
	}
}

func TestAnalyzeQuery_NoSource(t *testing.T) {
	// Query without explicit FROM clause — NormalizeQuery adds default "FROM main".
	analysis, err := AnalyzeQuery(`level=error | stats count`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// NormalizeQuery inserts "FROM main" for queries without an explicit source.
	if analysis.SourceIndex != "main" {
		t.Errorf("SourceIndex: got %q, want %q (default from NormalizeQuery)", analysis.SourceIndex, "main")
	}
	if !analysis.IsAggregation {
		t.Error("expected IsAggregation=true")
	}
}

// Validation: unsupported patterns should return descriptive errors

func TestAnalyzeQuery_RejectEventstats(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | eventstats avg(x) by host`)
	if err == nil {
		t.Fatal("expected error for eventstats")
	}
	if !strings.Contains(err.Error(), "eventstats") {
		t.Errorf("error should mention eventstats: %v", err)
	}
}

func TestAnalyzeQuery_RejectStreamstats(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | streamstats sum(x) by host`)
	if err == nil {
		t.Fatal("expected error for streamstats")
	}
	if !strings.Contains(err.Error(), "streamstats") {
		t.Errorf("error should mention streamstats: %v", err)
	}
}

func TestAnalyzeQuery_RejectTransaction(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | transaction session_id`)
	if err == nil {
		t.Fatal("expected error for transaction")
	}
	if !strings.Contains(err.Error(), "transaction") {
		t.Errorf("error should mention transaction: %v", err)
	}
}

func TestAnalyzeQuery_RejectJoin(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | join type=inner host [FROM other]`)
	if err == nil {
		t.Fatal("expected error for join")
	}
	if !strings.Contains(err.Error(), "join") {
		t.Errorf("error should mention join: %v", err)
	}
}

func TestAnalyzeQuery_RejectValues(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | stats values(host)`)
	if err == nil {
		t.Fatal("expected error for values()")
	}
	if !strings.Contains(err.Error(), "values") {
		t.Errorf("error should mention values: %v", err)
	}
}

func TestAnalyzeQuery_RejectStdev(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | stats stdev(duration)`)
	if err == nil {
		t.Fatal("expected error for stdev()")
	}
	if !strings.Contains(err.Error(), "stdev") {
		t.Errorf("error should mention stdev: %v", err)
	}
}

func TestAnalyzeQuery_RejectPercentile(t *testing.T) {
	_, err := AnalyzeQuery(`FROM main | stats perc95(duration)`)
	if err == nil {
		t.Fatal("expected error for perc95()")
	}
	if !strings.Contains(err.Error(), "percentile") {
		t.Errorf("error should mention percentile: %v", err)
	}
}

func TestAnalyzeQuery_SupportedAggFunctions(t *testing.T) {
	// All supported functions should work without error.
	queries := []string{
		`FROM main | stats count by host`,
		`FROM main | stats sum(bytes) by host`,
		`FROM main | stats avg(duration) by host`,
		`FROM main | stats min(latency) by host`,
		`FROM main | stats max(latency) by host`,
		`FROM main | stats dc(user) by host`,
	}

	for _, q := range queries {
		_, err := AnalyzeQuery(q)
		if err != nil {
			t.Errorf("query %q should succeed, got: %v", q, err)
		}
	}
}

func TestAnalyzeQuery_MultipleAggFunctions(t *testing.T) {
	analysis, err := AnalyzeQuery(`FROM main | stats count, sum(bytes), avg(duration), min(latency), max(latency) by host`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(analysis.AggSpec.Funcs) != 5 {
		t.Fatalf("Funcs: got %d, want 5", len(analysis.AggSpec.Funcs))
	}

	expected := []struct{ name, field string }{
		{"count", ""},
		{"sum", "bytes"},
		{"avg", "duration"},
		{"min", "latency"},
		{"max", "latency"},
	}
	for i, exp := range expected {
		if analysis.AggSpec.Funcs[i].Name != exp.name {
			t.Errorf("Func[%d].Name: got %q, want %q", i, analysis.AggSpec.Funcs[i].Name, exp.name)
		}
		if analysis.AggSpec.Funcs[i].Field != exp.field {
			t.Errorf("Func[%d].Field: got %q, want %q", i, analysis.AggSpec.Funcs[i].Field, exp.field)
		}
	}
}

func TestAnalyzeQuery_RejectGlobSource(t *testing.T) {
	_, err := AnalyzeQuery(`FROM idx_* | stats count`)
	if err == nil {
		t.Fatal("expected error for glob source")
	}
	if !strings.Contains(err.Error(), "glob") {
		t.Errorf("error should mention glob: %v", err)
	}
}

func TestAnalyzeQuery_RejectDedup(t *testing.T) {
	// Dedup before stats is semantically incorrect for MV: per-batch dedup
	// only deduplicates within each insert batch, not across all events.
	_, err := AnalyzeQuery(`FROM main | dedup host | stats count`)
	if err == nil {
		t.Fatal("expected error for dedup before stats")
	}
	if !strings.Contains(err.Error(), "dedup") {
		t.Errorf("error should mention dedup: %v", err)
	}
	if !strings.Contains(err.Error(), "per-batch") {
		t.Errorf("error should explain per-batch semantics: %v", err)
	}
}

func TestAnalyzeQuery_TimechartHasBinInStreaming(t *testing.T) {
	// Timechart must inject a BinCommand into StreamingCmds so that _time
	// is bucketed before partial aggregation groups by it. Without this,
	// each event's nanosecond-precision _time creates its own group.
	analysis, err := AnalyzeQuery(`FROM main | timechart span=5m count`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !analysis.IsAggregation {
		t.Fatal("expected IsAggregation=true")
	}

	// StreamingCmds should contain exactly one BinCommand.
	if len(analysis.StreamingCmds) != 1 {
		t.Fatalf("StreamingCmds: got %d, want 1 (BinCommand)", len(analysis.StreamingCmds))
	}
	bin, ok := analysis.StreamingCmds[0].(*spl2.BinCommand)
	if !ok {
		t.Fatalf("StreamingCmds[0]: got %T, want *spl2.BinCommand", analysis.StreamingCmds[0])
	}
	if bin.Field != "_time" {
		t.Errorf("BinCommand.Field: got %q, want %q", bin.Field, "_time")
	}
	if bin.Span != "5m" {
		t.Errorf("BinCommand.Span: got %q, want %q", bin.Span, "5m")
	}

	// GroupBy should include _time.
	if len(analysis.GroupBy) == 0 || analysis.GroupBy[0] != "_time" {
		t.Errorf("GroupBy: got %v, want [_time, ...]", analysis.GroupBy)
	}

	// QueryCmds should have the implicit sort by _time.
	if len(analysis.QueryCmds) != 1 {
		t.Errorf("QueryCmds: got %d, want 1 (sort _time)", len(analysis.QueryCmds))
	}
}

func TestAnalyzeQuery_TimechartWithStreamingCmds(t *testing.T) {
	// Pre-existing streaming commands + timechart's injected BinCommand.
	analysis, err := AnalyzeQuery(`FROM main | where level="error" | timechart span=1h count`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// StreamingCmds: [where, bin _time span=1h].
	if len(analysis.StreamingCmds) != 2 {
		t.Fatalf("StreamingCmds: got %d, want 2 (where + BinCommand)", len(analysis.StreamingCmds))
	}
	if _, ok := analysis.StreamingCmds[1].(*spl2.BinCommand); !ok {
		t.Errorf("StreamingCmds[1]: got %T, want *spl2.BinCommand", analysis.StreamingCmds[1])
	}
}
