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

func TestParse_UnpackJSON_Default(t *testing.T) {
	q, err := Parse(`| unpack_json`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("commands: got %d, want 1", len(q.Commands))
	}
	cmd, ok := q.Commands[0].(*UnpackCommand)
	if !ok {
		t.Fatalf("cmd type: got %T, want *UnpackCommand", q.Commands[0])
	}
	if cmd.Format != "json" {
		t.Errorf("format: got %q, want %q", cmd.Format, "json")
	}
	if cmd.SourceField != "_raw" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "_raw")
	}
	if cmd.Fields != nil {
		t.Errorf("fields: got %v, want nil", cmd.Fields)
	}
	if cmd.Prefix != "" {
		t.Errorf("prefix: got %q, want empty", cmd.Prefix)
	}
	if cmd.KeepOriginal {
		t.Error("keepOriginal: got true, want false")
	}
}

func TestParse_UnpackJSON_FromField(t *testing.T) {
	q, err := Parse(`| unpack_json from message`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if cmd.SourceField != "message" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "message")
	}
}

func TestParse_UnpackJSON_FieldsList(t *testing.T) {
	q, err := Parse(`| unpack_json fields (level, service)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if len(cmd.Fields) != 2 || cmd.Fields[0] != "level" || cmd.Fields[1] != "service" {
		t.Errorf("fields: got %v, want [level service]", cmd.Fields)
	}
}

func TestParse_UnpackJSON_PrefixAndKeepOriginal(t *testing.T) {
	q, err := Parse(`| unpack_json from payload prefix "app_" keep_original`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if cmd.SourceField != "payload" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "payload")
	}
	if cmd.Prefix != "app_" {
		t.Errorf("prefix: got %q, want %q", cmd.Prefix, "app_")
	}
	if !cmd.KeepOriginal {
		t.Error("keepOriginal: got false, want true")
	}
}

func TestParse_UnpackLogfmt(t *testing.T) {
	q, err := Parse(`| unpack_logfmt from message`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if cmd.Format != "logfmt" {
		t.Errorf("format: got %q, want %q", cmd.Format, "logfmt")
	}
	if cmd.SourceField != "message" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "message")
	}
}

func TestParse_UnpackSyslog(t *testing.T) {
	q, err := Parse(`| unpack_syslog`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if cmd.Format != "syslog" {
		t.Errorf("format: got %q, want %q", cmd.Format, "syslog")
	}
}

func TestParse_UnpackCombined(t *testing.T) {
	q, err := Parse(`| unpack_combined`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnpackCommand)
	if cmd.Format != "combined" {
		t.Errorf("format: got %q, want %q", cmd.Format, "combined")
	}
}

func TestParse_JsonCmd_Default(t *testing.T) {
	q, err := Parse(`| json`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*JsonCommand)
	if !ok {
		t.Fatalf("cmd type: got %T, want *JsonCommand", q.Commands[0])
	}
	if cmd.SourceField != "_raw" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "_raw")
	}
	if cmd.Paths != nil {
		t.Errorf("paths: got %v, want nil", cmd.Paths)
	}
}

func TestParse_JsonCmd_FieldAndPaths(t *testing.T) {
	q, err := Parse(`| json field=payload paths="user.id, request.method"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if cmd.SourceField != "payload" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "payload")
	}
	if len(cmd.Paths) != 2 || cmd.Paths[0].Path != "user.id" || cmd.Paths[1].Path != "request.method" {
		t.Errorf("paths: got %v, want [user.id request.method]", cmd.Paths)
	}
	// No aliases specified.
	if cmd.Paths[0].Alias != "" || cmd.Paths[1].Alias != "" {
		t.Errorf("aliases should be empty: got %q, %q", cmd.Paths[0].Alias, cmd.Paths[1].Alias)
	}
}

func TestParse_JsonCmd_PathsWithAS(t *testing.T) {
	q, err := Parse(`| json paths="user.id AS uid, request.method AS method"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 2 {
		t.Fatalf("paths count: got %d, want 2", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "user.id" || cmd.Paths[0].Alias != "uid" {
		t.Errorf("paths[0]: got {%q, %q}, want {user.id, uid}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
	if cmd.Paths[1].Path != "request.method" || cmd.Paths[1].Alias != "method" {
		t.Errorf("paths[1]: got {%q, %q}, want {request.method, method}", cmd.Paths[1].Path, cmd.Paths[1].Alias)
	}
}

func TestParse_JsonCmd_PathsMixedAS(t *testing.T) {
	q, err := Parse(`| json paths="user.id AS uid, action"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 2 {
		t.Fatalf("paths count: got %d, want 2", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "user.id" || cmd.Paths[0].Alias != "uid" {
		t.Errorf("paths[0]: got {%q, %q}, want {user.id, uid}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
	if cmd.Paths[1].Path != "action" || cmd.Paths[1].Alias != "" {
		t.Errorf("paths[1]: got {%q, %q}, want {action, \"\"}", cmd.Paths[1].Path, cmd.Paths[1].Alias)
	}
}

func TestParse_JsonCmd_SingularPathWithAS(t *testing.T) {
	q, err := Parse(`| json path="items[0].name" AS first_item`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 1 {
		t.Fatalf("paths count: got %d, want 1", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "items[0].name" || cmd.Paths[0].Alias != "first_item" {
		t.Errorf("paths[0]: got {%q, %q}, want {items[0].name, first_item}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
}

func TestParse_JsonCmd_SingularPathNoAS(t *testing.T) {
	q, err := Parse(`| json path="user.id"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 1 {
		t.Fatalf("paths count: got %d, want 1", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "user.id" || cmd.Paths[0].Alias != "" {
		t.Errorf("paths[0]: got {%q, %q}, want {user.id, \"\"}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
}

func TestParse_JsonCmd_BarePathsWithAS(t *testing.T) {
	q, err := Parse(`| json items[*].price AS price, items[*].product AS product`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 2 {
		t.Fatalf("paths count: got %d, want 2", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "items[*].price" || cmd.Paths[0].Alias != "price" {
		t.Errorf("paths[0]: got {%q, %q}, want {items[*].price, price}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
	if cmd.Paths[1].Path != "items[*].product" || cmd.Paths[1].Alias != "product" {
		t.Errorf("paths[1]: got {%q, %q}, want {items[*].product, product}", cmd.Paths[1].Path, cmd.Paths[1].Alias)
	}
}

func TestParse_JsonCmd_BareSimplePath(t *testing.T) {
	q, err := Parse(`| json user.name AS name`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 1 {
		t.Fatalf("paths count: got %d, want 1", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "user.name" || cmd.Paths[0].Alias != "name" {
		t.Errorf("paths[0]: got {%q, %q}, want {user.name, name}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
}

func TestParse_JsonCmd_BarePathNoBracket(t *testing.T) {
	q, err := Parse(`| json action`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if len(cmd.Paths) != 1 {
		t.Fatalf("paths count: got %d, want 1", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "action" || cmd.Paths[0].Alias != "" {
		t.Errorf("paths[0]: got {%q, %q}, want {action, \"\"}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
}

func TestParse_JsonCmd_BarePaths_PipelineContinues(t *testing.T) {
	q, err := Parse(`| json items[*].price AS price | explode price`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("commands: got %d, want 2", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*JsonCommand); !ok {
		t.Errorf("cmd[0]: got %T, want *JsonCommand", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*UnrollCommand); !ok {
		t.Errorf("cmd[1]: got %T, want *UnrollCommand", q.Commands[1])
	}
}

func TestParse_JsonCmd_BareAndKeywordMixed(t *testing.T) {
	q, err := Parse(`| json field=payload items[*].id AS id`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*JsonCommand)
	if cmd.SourceField != "payload" {
		t.Errorf("sourceField: got %q, want %q", cmd.SourceField, "payload")
	}
	if len(cmd.Paths) != 1 {
		t.Fatalf("paths count: got %d, want 1", len(cmd.Paths))
	}
	if cmd.Paths[0].Path != "items[*].id" || cmd.Paths[0].Alias != "id" {
		t.Errorf("paths[0]: got {%q, %q}, want {items[*].id, id}", cmd.Paths[0].Path, cmd.Paths[0].Alias)
	}
}

func TestParse_UnpackInPipeline(t *testing.T) {
	q, err := Parse(`| unpack_json | where level="error" | stats count by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("commands: got %d, want 3", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*UnpackCommand); !ok {
		t.Errorf("cmd[0]: got %T, want *UnpackCommand", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*WhereCommand); !ok {
		t.Errorf("cmd[1]: got %T, want *WhereCommand", q.Commands[1])
	}
	if _, ok := q.Commands[2].(*StatsCommand); !ok {
		t.Errorf("cmd[2]: got %T, want *StatsCommand", q.Commands[2])
	}
}

func TestParse_PackJson_WithFields(t *testing.T) {
	input := `FROM main | pack_json level, service into output`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("commands: got %d, want 1", len(q.Commands))
	}
	pj, ok := q.Commands[0].(*PackJsonCommand)
	if !ok {
		t.Fatalf("cmd[0]: got %T, want *PackJsonCommand", q.Commands[0])
	}
	if len(pj.Fields) != 2 || pj.Fields[0] != "level" || pj.Fields[1] != "service" {
		t.Errorf("fields: got %v, want [level service]", pj.Fields)
	}
	if pj.Target != "output" {
		t.Errorf("target: got %q, want %q", pj.Target, "output")
	}
}

func TestParse_PackJson_AllFields(t *testing.T) {
	input := `FROM main | pack_json into output_json`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("commands: got %d, want 1", len(q.Commands))
	}
	pj, ok := q.Commands[0].(*PackJsonCommand)
	if !ok {
		t.Fatalf("cmd[0]: got %T, want *PackJsonCommand", q.Commands[0])
	}
	if pj.Fields != nil {
		t.Errorf("fields: got %v, want nil", pj.Fields)
	}
	if pj.Target != "output_json" {
		t.Errorf("target: got %q, want %q", pj.Target, "output_json")
	}
}

func TestParse_WhereNotIn(t *testing.T) {
	q, err := Parse(`| where status NOT IN (200, 301, 302)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("commands: got %d, want 1", len(q.Commands))
	}
	w, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	in, ok := w.Expr.(*InExpr)
	if !ok {
		t.Fatalf("expr: expected InExpr, got %T", w.Expr)
	}
	if !in.Negated {
		t.Error("expected Negated=true")
	}
	if len(in.Values) != 3 {
		t.Errorf("values: got %d, want 3", len(in.Values))
	}
	// Check the String() output includes "not in"
	s := in.String()
	if !strings.Contains(s, "not in") {
		t.Errorf("String(): got %q, want to contain 'not in'", s)
	}
}

func TestParse_WhereNotLike(t *testing.T) {
	q, err := Parse(`| where host NOT LIKE "web%"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	cmp, ok := w.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", w.Expr)
	}
	if cmp.Op != "not like" {
		t.Errorf("op: got %q, want 'not like'", cmp.Op)
	}
}

func TestParse_WhereBetween(t *testing.T) {
	q, err := Parse(`| where status BETWEEN 200 AND 299`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	bin, ok := w.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expr: expected BinaryExpr (AND), got %T", w.Expr)
	}
	if bin.Op != "and" {
		t.Errorf("op: got %q, want 'and'", bin.Op)
	}
	// Left: status >= 200
	left, ok := bin.Left.(*CompareExpr)
	if !ok {
		t.Fatalf("left: expected CompareExpr, got %T", bin.Left)
	}
	if left.Op != ">=" {
		t.Errorf("left op: got %q, want '>='", left.Op)
	}
	// Right: status <= 299
	right, ok := bin.Right.(*CompareExpr)
	if !ok {
		t.Fatalf("right: expected CompareExpr, got %T", bin.Right)
	}
	if right.Op != "<=" {
		t.Errorf("right op: got %q, want '<='", right.Op)
	}
}

func TestParse_WhereRegexMatch(t *testing.T) {
	q, err := Parse(`| where message =~ "^error"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	cmp, ok := w.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", w.Expr)
	}
	if cmp.Op != "=~" {
		t.Errorf("op: got %q, want '=~'", cmp.Op)
	}
}

func TestParse_WhereRegexNotMatch(t *testing.T) {
	q, err := Parse(`| where message !~ "^debug"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	cmp, ok := w.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", w.Expr)
	}
	if cmp.Op != "!~" {
		t.Errorf("op: got %q, want '!~'", cmp.Op)
	}
}

func TestParse_WhereIsNull(t *testing.T) {
	q, err := Parse(`| where host IS NULL`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	fn, ok := w.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr, got %T", w.Expr)
	}
	if fn.Name != "isnull" {
		t.Errorf("name: got %q, want 'isnull'", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("args: got %d, want 1", len(fn.Args))
	}
}

func TestParse_WhereIsNotNull(t *testing.T) {
	q, err := Parse(`| where host IS NOT NULL`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	fn, ok := w.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr, got %T", w.Expr)
	}
	if fn.Name != "isnotnull" {
		t.Errorf("name: got %q, want 'isnotnull'", fn.Name)
	}
}

func TestParse_BetweenWithExpressions(t *testing.T) {
	// BETWEEN should work with arithmetic expressions
	q, err := Parse(`| where duration BETWEEN 1.5 AND 10.0`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	bin, ok := w.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expr: expected BinaryExpr, got %T", w.Expr)
	}
	if bin.Op != "and" {
		t.Errorf("op: got %q, want 'and'", bin.Op)
	}
}

func TestParse_NotNotAmbiguity(t *testing.T) {
	// NOT followed by something other than IN/LIKE should be handled
	// as a boolean NOT in the caller (parseNot), not in parseComparison.
	q, err := Parse(`| where NOT status = 200`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	w := q.Commands[0].(*WhereCommand)
	not, ok := w.Expr.(*NotExpr)
	if !ok {
		t.Fatalf("expr: expected NotExpr, got %T", w.Expr)
	}
	cmp, ok := not.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("inner: expected CompareExpr, got %T", not.Expr)
	}
	if cmp.Op != "=" {
		t.Errorf("op: got %q, want '='", cmp.Op)
	}
}

// --- Implicit WHERE for function calls ---

func TestParse_ImplicitWhereFuncCall(t *testing.T) {
	// `| isnotnull(field)` should parse as WhereCommand with FuncCallExpr.
	tests := []struct {
		name     string
		input    string
		wantFunc string
	}{
		{"isnotnull", `FROM main | isnotnull(pg.duration_ms)`, "isnotnull"},
		{"isnull", `FROM main | isnull(pg.duration_ms)`, "isnull"},
		{"len", `FROM main | len(message) > 0`, ""},  // len() > 0 is a compare expr, not bare func
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.input, err)
			}
			if len(q.Commands) == 0 {
				t.Fatal("no commands parsed")
			}
			w, ok := q.Commands[len(q.Commands)-1].(*WhereCommand)
			if !ok {
				t.Fatalf("last command: expected WhereCommand, got %T", q.Commands[len(q.Commands)-1])
			}
			if tt.wantFunc != "" {
				fc, ok := w.Expr.(*FuncCallExpr)
				if !ok {
					t.Fatalf("where expr: expected FuncCallExpr, got %T", w.Expr)
				}
				if fc.Name != tt.wantFunc {
					t.Errorf("func name: got %q, want %q", fc.Name, tt.wantFunc)
				}
			}
		})
	}
}

// --- INDEX as alias for FROM ---

func TestParse_IndexBare(t *testing.T) {
	// index nginx === from nginx
	tests := []struct {
		name      string
		input     string
		wantIndex string
	}{
		{"index nginx", "index nginx | stats count", "nginx"},
		{"INDEX nginx", "INDEX nginx | stats count", "nginx"},
		{"index quoted", `index "my-logs" | stats count`, "my-logs"},
		{"index mv name", "index mv_errors_5m | stats count", "mv_errors_5m"},
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

func TestParse_IndexEqualsName(t *testing.T) {
	// index="nginx" === from nginx (SPL1 compat)
	tests := []struct {
		name      string
		input     string
		wantIndex string
		wantGlob  bool
	}{
		{"index=nginx", `index=nginx | stats count`, "nginx", false},
		{"index=\"nginx\"", `index="nginx" | stats count`, "nginx", false},
		{"index=logs*", `index=logs* | stats count`, "logs*", true},
		{"index=*", `index=* | stats count`, "*", true},
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
			if q.Source.IsGlob != tt.wantGlob {
				t.Errorf("Source.IsGlob = %v, want %v", q.Source.IsGlob, tt.wantGlob)
			}
		})
	}
}

func TestParse_IndexEqualsDesugarsSearch(t *testing.T) {
	// index="nginx" status>=500 desugars to: from nginx | search status>=500
	q, err := Parse(`index="nginx" status>=500`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Fatalf("Source: got %v, want nginx", q.Source)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1 (desugared search)", len(q.Commands))
	}
	search, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected SearchCommand, got %T", q.Commands[0])
	}
	if search.Expression == nil {
		t.Fatal("search.Expression is nil — expected desugared search expression")
	}
}

func TestParse_IndexEqualsDesugarsComplex(t *testing.T) {
	// index=nginx status>=500 method="POST" desugars with implicit search
	q, err := Parse(`index=nginx status>=500 method="POST"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Fatalf("Source: got %v, want nginx", q.Source)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	_, ok := q.Commands[0].(*SearchCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected SearchCommand, got %T", q.Commands[0])
	}
}

func TestParse_IndexEqualsThenPipe(t *testing.T) {
	// index=nginx | where status>=500 — no implicit search, just source + pipe
	q, err := Parse(`index=nginx | where status>=500`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Fatalf("Source: got %v, want nginx", q.Source)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	_, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
}

func TestParse_IndexEqualsNoSearch(t *testing.T) {
	// index=nginx alone — just source, no commands
	q, err := Parse(`index=nginx`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Fatalf("Source: got %v, want nginx", q.Source)
	}
	if len(q.Commands) != 0 {
		t.Fatalf("Commands: got %d, want 0", len(q.Commands))
	}
}

func TestParse_IndexEquivalentToFrom(t *testing.T) {
	// Verify that index nginx and from nginx produce identical ASTs.
	tests := []struct {
		indexQuery string
		fromQuery  string
	}{
		{"index nginx | stats count", "FROM nginx | stats count"},
		{`index="nginx" | stats count`, `FROM nginx | stats count`},
		{"index nginx, api_gw | stats count", "FROM nginx, api_gw | stats count"},
		{"index logs* | stats count", "FROM logs* | stats count"},
		{"index * | stats count", "FROM * | stats count"},
	}

	for _, tt := range tests {
		t.Run(tt.indexQuery, func(t *testing.T) {
			iq, err := Parse(tt.indexQuery)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.indexQuery, err)
			}
			fq, err := Parse(tt.fromQuery)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.fromQuery, err)
			}

			// Compare source.
			if iq.Source == nil || fq.Source == nil {
				t.Fatal("one of the sources is nil")
			}
			if iq.Source.Index != fq.Source.Index {
				t.Errorf("Index: index=%q vs from=%q", iq.Source.Index, fq.Source.Index)
			}
			if iq.Source.IsGlob != fq.Source.IsGlob {
				t.Errorf("IsGlob: index=%v vs from=%v", iq.Source.IsGlob, fq.Source.IsGlob)
			}
			if len(iq.Source.Indices) != len(fq.Source.Indices) {
				t.Errorf("Indices len: index=%d vs from=%d", len(iq.Source.Indices), len(fq.Source.Indices))
			}
		})
	}
}

func TestParse_FromEqualsError(t *testing.T) {
	// from="nginx" should produce a helpful error (not silently succeed).
	_, err := Parse(`from="nginx"`)
	if err == nil {
		t.Fatal("expected error for from=\"nginx\"")
	}
	errStr := err.Error()
	if !strings.Contains(errStr, "unexpected '='") {
		t.Errorf("error should mention unexpected '=', got: %s", errStr)
	}
	if !strings.Contains(errStr, "from nginx") {
		t.Errorf("error should suggest 'from nginx', got: %s", errStr)
	}
	if !strings.Contains(errStr, `index="nginx"`) {
		t.Errorf("error should suggest index=\"nginx\", got: %s", errStr)
	}
}

func TestParse_IndexMulti(t *testing.T) {
	// index nginx, api_gw === from nginx, api_gw
	q, err := Parse(`index nginx, api_gw | stats count by source`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("Source is nil")
	}
	expected := []string{"nginx", "api_gw"}
	if len(q.Source.Indices) != len(expected) {
		t.Fatalf("Indices: got %v, want %v", q.Source.Indices, expected)
	}
	for i, want := range expected {
		if q.Source.Indices[i] != want {
			t.Errorf("Indices[%d]: got %q, want %q", i, q.Source.Indices[i], want)
		}
	}
}

func TestParse_IndexGlob(t *testing.T) {
	// index logs* === from logs*
	q, err := Parse(`index logs* | stats count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("Source is nil")
	}
	if !q.Source.IsGlob {
		t.Error("expected IsGlob=true")
	}
	if q.Source.Index != "logs*" {
		t.Errorf("Index: got %q, want logs*", q.Source.Index)
	}
}

func TestParse_IndexStar(t *testing.T) {
	// index * === from *
	q, err := Parse(`index * | stats count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("Source is nil")
	}
	if !q.Source.IsGlob || q.Source.Index != "*" {
		t.Errorf("expected glob '*', got Index=%q IsGlob=%v", q.Source.Index, q.Source.IsGlob)
	}
	if !q.Source.IsAllSources() {
		t.Error("expected IsAllSources()=true")
	}
}

func TestParse_IndexVariable(t *testing.T) {
	// index $threats === from $threats
	q, err := Parse(`index $threats | stats count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("Source is nil")
	}
	if !q.Source.IsVariable {
		t.Error("expected IsVariable=true")
	}
	if q.Source.Index != "threats" {
		t.Errorf("Index: got %q, want threats", q.Source.Index)
	}
}

func TestParse_IndexWithWhere(t *testing.T) {
	// index nginx WHERE status>=500 — WHERE after INDEX (same as FROM ... WHERE)
	q, err := Parse(`index nginx WHERE status>=500`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Fatalf("Source: got %v, want nginx", q.Source)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	_, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
}

func TestParse_StatsPercentileAliases(t *testing.T) {
	// Verify that p50..p99 shorthand aliases are normalized to perc50..perc99
	// at parse time so downstream code only sees the canonical names.
	tests := []struct {
		input    string
		wantFunc string
		wantAlias string
	}{
		{`| stats p99(duration) by host`, "perc99", ""},
		{`| stats p50(latency) as median`, "perc50", "median"},
		{`| stats p75(response_time) as rt_p75`, "perc75", "rt_p75"},
		{`| stats p90(bytes) as p90_bytes`, "perc90", "p90_bytes"},
		{`| stats p95(dur)`, "perc95", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.input, err)
			}
			if len(q.Commands) != 1 {
				t.Fatalf("Commands: got %d, want 1", len(q.Commands))
			}
			stats, ok := q.Commands[0].(*StatsCommand)
			if !ok {
				t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
			}
			if len(stats.Aggregations) != 1 {
				t.Fatalf("aggs: got %d, want 1", len(stats.Aggregations))
			}
			agg := stats.Aggregations[0]
			if agg.Func != tt.wantFunc {
				t.Errorf("Func: got %q, want %q", agg.Func, tt.wantFunc)
			}
			if agg.Alias != tt.wantAlias {
				t.Errorf("Alias: got %q, want %q", agg.Alias, tt.wantAlias)
			}
		})
	}
}

func TestParse_StatsMultiplePercentileAliases(t *testing.T) {
	// Verify multiple percentile aliases in a single stats command.
	q, err := Parse(`| stats p50(dur), p99(dur), count by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.Aggregations) != 3 {
		t.Fatalf("aggs: got %d, want 3", len(stats.Aggregations))
	}
	wantFuncs := []string{"perc50", "perc99", "count"}
	for i, want := range wantFuncs {
		if stats.Aggregations[i].Func != want {
			t.Errorf("agg[%d].Func: got %q, want %q", i, stats.Aggregations[i].Func, want)
		}
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v, want [service]", stats.GroupBy)
	}
}

func TestParse_FieldsGlobPattern(t *testing.T) {
	q, err := Parse(`FROM main | fields pg.*, status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc, ok := q.Commands[0].(*FieldsCommand)
	if !ok {
		t.Fatalf("expected FieldsCommand, got %T", q.Commands[0])
	}
	if len(fc.Fields) != 2 {
		t.Fatalf("fields count: got %d, want 2", len(fc.Fields))
	}
	if fc.Fields[0] != "pg.*" {
		t.Errorf("fields[0]: got %q, want %q", fc.Fields[0], "pg.*")
	}
	if fc.Fields[1] != "status" {
		t.Errorf("fields[1]: got %q, want %q", fc.Fields[1], "status")
	}
	if fc.Remove {
		t.Error("expected Remove=false")
	}
}

func TestParse_FieldsRemoveGlobPattern(t *testing.T) {
	q, err := Parse(`FROM main | fields - pg.*`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc, ok := q.Commands[0].(*FieldsCommand)
	if !ok {
		t.Fatalf("expected FieldsCommand, got %T", q.Commands[0])
	}
	if len(fc.Fields) != 1 || fc.Fields[0] != "pg.*" {
		t.Errorf("Fields: got %v, want [pg.*]", fc.Fields)
	}
	if !fc.Remove {
		t.Error("expected Remove=true")
	}
}

func TestParse_FieldsStar(t *testing.T) {
	q, err := Parse(`FROM main | fields *`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc, ok := q.Commands[0].(*FieldsCommand)
	if !ok {
		t.Fatalf("expected FieldsCommand, got %T", q.Commands[0])
	}
	if len(fc.Fields) != 1 || fc.Fields[0] != "*" {
		t.Errorf("Fields: got %v, want [*]", fc.Fields)
	}
}

func TestParse_TableGlobPattern(t *testing.T) {
	q, err := Parse(`FROM main | table pg.*, http.*`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc, ok := q.Commands[0].(*TableCommand)
	if !ok {
		t.Fatalf("expected TableCommand, got %T", q.Commands[0])
	}
	if len(tc.Fields) != 2 {
		t.Fatalf("fields count: got %d, want 2", len(tc.Fields))
	}
	if tc.Fields[0] != "pg.*" {
		t.Errorf("fields[0]: got %q, want %q", tc.Fields[0], "pg.*")
	}
	if tc.Fields[1] != "http.*" {
		t.Errorf("fields[1]: got %q, want %q", tc.Fields[1], "http.*")
	}
}
