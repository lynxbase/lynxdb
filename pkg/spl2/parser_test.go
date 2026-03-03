package spl2

import (
	"strings"
	"testing"
)

func TestParse_FromSearchStatsSort(t *testing.T) {
	input := `FROM main WHERE host="web-*" | search "error" | stats count() by host | sort -count | head 20`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if q.Source == nil || q.Source.Index != "main" {
		t.Errorf("Source: got %v, want main", q.Source)
	}

	if len(q.Commands) != 5 {
		t.Fatalf("Commands: got %d, want 5", len(q.Commands))
	}

	// where
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("where expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Left.(*FieldExpr).Name != "host" {
		t.Errorf("where left: got %s, want host", cmp.Left)
	}
	if cmp.Op != "=" {
		t.Errorf("where op: got %q, want =", cmp.Op)
	}

	// search
	search, ok := q.Commands[1].(*SearchCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected SearchCommand, got %T", q.Commands[1])
	}
	if search.Term != "error" {
		t.Errorf("search term: got %q, want \"error\"", search.Term)
	}

	// stats
	stats, ok := q.Commands[2].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[2]: expected StatsCommand, got %T", q.Commands[2])
	}
	if len(stats.Aggregations) != 1 || stats.Aggregations[0].Func != "count" {
		t.Errorf("stats aggs: got %v", stats.Aggregations)
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "host" {
		t.Errorf("stats groupby: got %v", stats.GroupBy)
	}

	// sort
	sortCmd, ok := q.Commands[3].(*SortCommand)
	if !ok {
		t.Fatalf("cmd[3]: expected SortCommand, got %T", q.Commands[3])
	}
	if len(sortCmd.Fields) != 1 || sortCmd.Fields[0].Name != "count" || !sortCmd.Fields[0].Desc {
		t.Errorf("sort: got %v", sortCmd.Fields)
	}

	// head
	head, ok := q.Commands[4].(*HeadCommand)
	if !ok {
		t.Fatalf("cmd[4]: expected HeadCommand, got %T", q.Commands[4])
	}
	if head.Count != 20 {
		t.Errorf("head count: got %d, want 20", head.Count)
	}
}

func TestParse_TimechartByIP(t *testing.T) {
	input := `FROM security | search "auth failed" | timechart span=5m count() by src_ip`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if q.Source.Index != "security" {
		t.Errorf("Source: got %q", q.Source.Index)
	}

	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}

	tc, ok := q.Commands[1].(*TimechartCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected TimechartCommand, got %T", q.Commands[1])
	}
	if tc.Span != "5m" {
		t.Errorf("span: got %q, want 5m", tc.Span)
	}
	if len(tc.Aggregations) != 1 || tc.Aggregations[0].Func != "count" {
		t.Errorf("aggs: got %v", tc.Aggregations)
	}
	if len(tc.GroupBy) != 1 || tc.GroupBy[0] != "src_ip" {
		t.Errorf("groupby: got %v, want [src_ip]", tc.GroupBy)
	}
}

func TestParse_WhereStatsAvg(t *testing.T) {
	input := `FROM nginx | where status >= 500 | stats avg(response_time) as avg_rt by uri`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}

	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	cmp := where.Expr.(*CompareExpr)
	if cmp.Op != ">=" {
		t.Errorf("where op: got %q, want >=", cmp.Op)
	}

	stats, ok := q.Commands[1].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected StatsCommand, got %T", q.Commands[1])
	}
	if len(stats.Aggregations) != 1 {
		t.Fatalf("aggs: got %d, want 1", len(stats.Aggregations))
	}
	if stats.Aggregations[0].Func != "avg" {
		t.Errorf("agg func: got %q, want avg", stats.Aggregations[0].Func)
	}
	if stats.Aggregations[0].Alias != "avg_rt" {
		t.Errorf("agg alias: got %q, want avg_rt", stats.Aggregations[0].Alias)
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "uri" {
		t.Errorf("groupby: got %v, want [uri]", stats.GroupBy)
	}
}

func TestParse_EvalCommand(t *testing.T) {
	input := `FROM main | eval duration = response_time`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	eval, ok := q.Commands[0].(*EvalCommand)
	if !ok {
		t.Fatalf("expected EvalCommand, got %T", q.Commands[0])
	}
	if eval.Field != "duration" {
		t.Errorf("field: got %q, want duration", eval.Field)
	}
}

func TestParse_RexCommand(t *testing.T) {
	input := `FROM main | rex field=_raw "status=(?P<status>\d+)"`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	rex, ok := q.Commands[0].(*RexCommand)
	if !ok {
		t.Fatalf("expected RexCommand, got %T", q.Commands[0])
	}
	if rex.Field != "_raw" {
		t.Errorf("field: got %q, want _raw", rex.Field)
	}
	if rex.Pattern != `status=(?P<status>\d+)` {
		t.Errorf("pattern: got %q", rex.Pattern)
	}
}

func TestParse_FieldsCommand(t *testing.T) {
	input := `FROM main | fields host, status, _time`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	fields, ok := q.Commands[0].(*FieldsCommand)
	if !ok {
		t.Fatalf("expected FieldsCommand, got %T", q.Commands[0])
	}
	if len(fields.Fields) != 3 {
		t.Fatalf("fields count: got %d, want 3", len(fields.Fields))
	}
	expectedFields := []string{"host", "status", "_time"}
	for i, name := range expectedFields {
		if fields.Fields[i] != name {
			t.Errorf("fields[%d]: got %q, want %q", i, fields.Fields[i], name)
		}
	}
}

func TestParse_TableCommand(t *testing.T) {
	input := `FROM main | table _time, host, _raw`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	table, ok := q.Commands[0].(*TableCommand)
	if !ok {
		t.Fatalf("expected TableCommand, got %T", q.Commands[0])
	}
	if len(table.Fields) != 3 {
		t.Fatalf("table fields count: got %d, want 3", len(table.Fields))
	}
	expectedFields := []string{"_time", "host", "_raw"}
	for i, name := range expectedFields {
		if table.Fields[i] != name {
			t.Errorf("table.Fields[%d]: got %q, want %q", i, table.Fields[i], name)
		}
	}
}

func TestParse_DedupCommand(t *testing.T) {
	input := `FROM main | dedup host`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	dedup, ok := q.Commands[0].(*DedupCommand)
	if !ok {
		t.Fatalf("expected DedupCommand, got %T", q.Commands[0])
	}
	if len(dedup.Fields) != 1 || dedup.Fields[0] != "host" {
		t.Errorf("dedup fields: got %v", dedup.Fields)
	}
}

func TestParse_TailCommand(t *testing.T) {
	input := `FROM main | tail 50`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	tail, ok := q.Commands[0].(*TailCommand)
	if !ok {
		t.Fatalf("expected TailCommand, got %T", q.Commands[0])
	}
	if tail.Count != 50 {
		t.Errorf("tail count: got %d, want 50", tail.Count)
	}
}

func TestParse_HeadDefault(t *testing.T) {
	input := `FROM main | head`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	head, ok := q.Commands[0].(*HeadCommand)
	if !ok {
		t.Fatalf("expected HeadCommand, got %T", q.Commands[0])
	}
	if head.Count != 10 {
		t.Errorf("head count: got %d, want 10 (default)", head.Count)
	}
}

func TestParse_BooleanExpr(t *testing.T) {
	input := `FROM main | where host = "web-01" and status >= 500 or level = "ERROR"`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("expected WhereCommand, got %T", q.Commands[0])
	}
	// Should parse as: (host = "web-01" AND status >= 500) OR (level = "ERROR")
	binExpr, ok := where.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", where.Expr)
	}
	if binExpr.Op != "or" {
		t.Errorf("top-level op: got %q, want or", binExpr.Op)
	}
}

func TestParse_NotExpr(t *testing.T) {
	input := `FROM main | where not status = 200`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	where := q.Commands[0].(*WhereCommand)
	notExpr, ok := where.Expr.(*NotExpr)
	if !ok {
		t.Fatalf("expected NotExpr, got %T", where.Expr)
	}
	inner, ok := notExpr.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expected CompareExpr inside NotExpr, got %T", notExpr.Expr)
	}
	if inner.Left.(*FieldExpr).Name != "status" {
		t.Errorf("inner left: got %q, want %q", inner.Left.(*FieldExpr).Name, "status")
	}
	if inner.Op != "=" {
		t.Errorf("inner op: got %q, want %q", inner.Op, "=")
	}
	lit, ok := inner.Right.(*LiteralExpr)
	if !ok {
		t.Fatalf("expected LiteralExpr for right side, got %T", inner.Right)
	}
	if lit.Value != "200" {
		t.Errorf("inner right: got %q, want %q", lit.Value, "200")
	}
}

func TestParse_MultipleAggs(t *testing.T) {
	input := `FROM main | stats count() as cnt, avg(latency) as avg_lat, max(latency) as max_lat by host`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.Aggregations) != 3 {
		t.Fatalf("aggs: got %d, want 3", len(stats.Aggregations))
	}
	if stats.Aggregations[0].Func != "count" || stats.Aggregations[0].Alias != "cnt" {
		t.Errorf("agg[0]: %+v", stats.Aggregations[0])
	}
	if stats.Aggregations[1].Func != "avg" || stats.Aggregations[1].Alias != "avg_lat" {
		t.Errorf("agg[1]: %+v", stats.Aggregations[1])
	}
}

func TestParse_Errors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantSub string // if non-empty, err.Error() must contain this substring
	}{
		{"unterminated string", `FROM main | search "unterminated`, "unterminated"},
		{"missing FROM index", `FROM | search "test"`, ""},
		{"empty sort", `FROM main | sort`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			if err == nil {
				t.Fatal("expected parse error")
			}
			if tt.wantSub != "" && !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}

func TestParse_DigitPrefixedSourceName(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantIndex string
	}{
		{"FROM 2xlog", "FROM 2xlog | stats count", "2xlog"},
		{"FROM 123abc", "FROM 123abc | head 10", "123abc"},
		{"FROM bare number", "FROM 42 | stats count", "42"},
		{"FROM normal ident", "FROM main | stats count", "main"},
		{"FROM quoted name", `FROM "my-logs" | stats count`, "my-logs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.input, err)
			}
			if q.Source == nil {
				t.Fatal("Source is nil")
			}
			if q.Source.Index != tt.wantIndex {
				t.Errorf("Source.Index = %q, want %q", q.Source.Index, tt.wantIndex)
			}
		})
	}
}

func TestParse_SearchWithGlob(t *testing.T) {
	input := `FROM main | search web-*`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	if search.Term != "web-*" {
		t.Errorf("term: got %q, want web-*", search.Term)
	}
}

func TestParse_FromStar(t *testing.T) {
	input := `FROM * | stats count`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if !q.Source.IsGlob {
		t.Error("expected IsGlob=true for FROM *")
	}
	if q.Source.Index != "*" {
		t.Errorf("Index: got %q, want *", q.Source.Index)
	}
	if !q.Source.IsAllSources() {
		t.Error("expected IsAllSources()=true for FROM *")
	}
}

func TestParse_FromGlob(t *testing.T) {
	input := `FROM logs* | stats count by source`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if !q.Source.IsGlob {
		t.Error("expected IsGlob=true for FROM logs*")
	}
	if q.Source.Index != "logs*" {
		t.Errorf("Index: got %q, want logs*", q.Source.Index)
	}
}

func TestParse_FromMulti(t *testing.T) {
	input := `FROM nginx, postgres, redis | stats count by source`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if q.Source.IsGlob {
		t.Error("expected IsGlob=false for comma list")
	}
	expected := []string{"nginx", "postgres", "redis"}
	if len(q.Source.Indices) != len(expected) {
		t.Fatalf("Indices: got %d, want %d", len(q.Source.Indices), len(expected))
	}
	for i, want := range expected {
		if q.Source.Indices[i] != want {
			t.Errorf("Indices[%d]: got %q, want %q", i, q.Source.Indices[i], want)
		}
	}
}

func TestParse_FromQuoted(t *testing.T) {
	input := `FROM "my-logs", "web-access" | stats count`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	expected := []string{"my-logs", "web-access"}
	if len(q.Source.Indices) != len(expected) {
		t.Fatalf("Indices: got %v, want %v", q.Source.Indices, expected)
	}
	for i, want := range expected {
		if q.Source.Indices[i] != want {
			t.Errorf("Indices[%d]: got %q, want %q", i, q.Source.Indices[i], want)
		}
	}
}

func TestParse_FromMVPriority(t *testing.T) {
	// Single name starting with mv_ — not treated as multi-source.
	input := `FROM mv_errors_5m | stats count`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if q.Source.Index != "mv_errors_5m" {
		t.Errorf("Index: got %q, want mv_errors_5m", q.Source.Index)
	}
	if q.Source.IsGlob {
		t.Error("expected IsGlob=false for MV name")
	}
	if len(q.Source.Indices) != 0 {
		t.Errorf("expected empty Indices for single MV, got %v", q.Source.Indices)
	}
}

func TestParse_FromSingleUnchanged(t *testing.T) {
	// Existing behavior: single source name works as before.
	input := `FROM main | stats count`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if q.Source.Index != "main" {
		t.Errorf("Index: got %q, want main", q.Source.Index)
	}
	if q.Source.IsGlob {
		t.Error("expected IsGlob=false")
	}
	if !q.Source.IsSingleSource() {
		t.Error("expected IsSingleSource()=true")
	}
}

func TestParse_SearchIndexEquals(t *testing.T) {
	// search index=nginx should still work via the existing code path.
	input := `FROM main | search index=nginx level=error`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	if search.Index != "nginx" {
		t.Errorf("search.Index: got %q, want nginx", search.Index)
	}
}

func TestParse_SearchSourceFieldComparison(t *testing.T) {
	// source=nginx in search expression should produce a SearchCompareExpr.
	input := `FROM main | search source=nginx level=error`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	if search.Expression == nil {
		t.Fatal("expected search expression")
	}

	// Should be an AND of two comparisons.
	and, ok := search.Expression.(*SearchAndExpr)
	if !ok {
		t.Fatalf("expected SearchAndExpr, got %T", search.Expression)
	}

	left, ok := and.Left.(*SearchCompareExpr)
	if !ok {
		t.Fatalf("expected SearchCompareExpr for left, got %T", and.Left)
	}
	if left.Field != "source" || left.Value != "nginx" {
		t.Errorf("left: got field=%q value=%q, want source=nginx", left.Field, left.Value)
	}
}

func TestParse_SearchIndexFieldWildcard(t *testing.T) {
	// index=logs* in search expression — should be parsed as SearchCompareExpr
	// with HasWildcard=true.
	input := `FROM main | search index=logs* level=error`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	// When index= is detected at the start of search, it sets SearchCommand.Index.
	// The "index=logs*" path goes through the special code path in parseSearch().
	if search.Index != "logs*" {
		t.Errorf("search.Index: got %q, want logs*", search.Index)
	}
}

func TestParse_SearchFieldIn(t *testing.T) {
	input := `FROM main | search source IN ("nginx", "postgres", "redis")`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("expected SearchCommand, got %T", q.Commands[0])
	}
	if search.Expression == nil {
		t.Fatal("expected search expression")
	}

	inExpr, ok := search.Expression.(*SearchInExpr)
	if !ok {
		t.Fatalf("expected SearchInExpr, got %T", search.Expression)
	}
	if inExpr.Field != "source" {
		t.Errorf("field: got %q, want source", inExpr.Field)
	}
	if len(inExpr.Values) != 3 {
		t.Fatalf("values: got %d, want 3", len(inExpr.Values))
	}
	expected := []string{"nginx", "postgres", "redis"}
	for i, want := range expected {
		if inExpr.Values[i].Value != want {
			t.Errorf("values[%d]: got %q, want %q", i, inExpr.Values[i].Value, want)
		}
	}
}
