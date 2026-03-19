package spl2

import (
	"strings"
	"testing"
)

// =============================================================================
// Spec 01 — Source (FROM / INDEX aliases)
// =============================================================================

func TestLynxFlow_FromBasic(t *testing.T) {
	q, err := Parse(`from nginx | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Errorf("Source: got %v, want nginx", q.Source)
	}
}

func TestLynxFlow_FromMultiple(t *testing.T) {
	q, err := Parse(`from nginx, api_gw | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Source.Indices) != 2 {
		t.Fatalf("Indices: got %v, want [nginx api_gw]", q.Source.Indices)
	}
	if q.Source.Indices[0] != "nginx" || q.Source.Indices[1] != "api_gw" {
		t.Errorf("Indices: got %v", q.Source.Indices)
	}
}

func TestLynxFlow_FromGlob(t *testing.T) {
	q, err := Parse(`from logs_* | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !q.Source.IsGlob {
		t.Error("expected IsGlob=true")
	}
}

func TestLynxFlow_FromAll(t *testing.T) {
	q, err := Parse(`from * | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !q.Source.IsAllSources() {
		t.Error("expected IsAllSources()=true")
	}
}

func TestLynxFlow_FromVariable(t *testing.T) {
	q, err := Parse(`from $threats | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !q.Source.IsVariable {
		t.Error("expected IsVariable=true")
	}
	if q.Source.Index != "threats" {
		t.Errorf("Index: got %q, want threats", q.Source.Index)
	}
}

func TestLynxFlow_FromView(t *testing.T) {
	q, err := Parse(`from view.mv_errors_5m | stats count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.Index != "view.mv_errors_5m" {
		t.Errorf("Source: got %v, want view.mv_errors_5m", q.Source)
	}
}

// =============================================================================
// Spec 02 — Parsing (parse command, explode)
// =============================================================================

func TestLynxFlow_ParseJSON(t *testing.T) {
	q, err := Parse(`from nginx | parse json(_raw)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	unpack, ok := q.Commands[0].(*UnpackCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected UnpackCommand, got %T", q.Commands[0])
	}
	if unpack.Format != "json" {
		t.Errorf("Format: got %q, want json", unpack.Format)
	}
	if unpack.SourceField != "_raw" {
		t.Errorf("SourceField: got %q, want _raw", unpack.SourceField)
	}
}

func TestLynxFlow_ParseJSONWithNamespace(t *testing.T) {
	q, err := Parse(`from nginx | parse json(_raw) as req`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unpack := q.Commands[0].(*UnpackCommand)
	if unpack.Prefix != "req." {
		t.Errorf("Prefix: got %q, want \"req.\"", unpack.Prefix)
	}
}

func TestLynxFlow_ParseCombinedWithExtract(t *testing.T) {
	q, err := Parse(`from nginx | parse combined(_raw) extract (status, uri)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unpack := q.Commands[0].(*UnpackCommand)
	if unpack.Format != "combined" {
		t.Errorf("Format: got %q, want combined", unpack.Format)
	}
	if len(unpack.Fields) != 2 || unpack.Fields[0] != "status" || unpack.Fields[1] != "uri" {
		t.Errorf("Fields: got %v, want [status uri]", unpack.Fields)
	}
}

func TestLynxFlow_ParseSyslogAllModifiers(t *testing.T) {
	q, err := Parse(`from app | parse syslog(_raw) as env extract (severity) if_missing`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unpack := q.Commands[0].(*UnpackCommand)
	if unpack.Format != "syslog" {
		t.Errorf("Format: got %q", unpack.Format)
	}
	if unpack.Prefix != "env." {
		t.Errorf("Prefix: got %q", unpack.Prefix)
	}
	if len(unpack.Fields) != 1 || unpack.Fields[0] != "severity" {
		t.Errorf("Fields: got %v", unpack.Fields)
	}
	if !unpack.KeepOriginal {
		t.Error("expected KeepOriginal=true")
	}
}

func TestLynxFlow_ParseRegex(t *testing.T) {
	q, err := Parse(`from app | parse regex(_raw, "host=(?P<host>\\S+)")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rex, ok := q.Commands[0].(*RexCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected RexCommand, got %T", q.Commands[0])
	}
	if rex.Field != "_raw" {
		t.Errorf("Field: got %q", rex.Field)
	}
	if !strings.Contains(rex.Pattern, "host=") {
		t.Errorf("Pattern: got %q", rex.Pattern)
	}
}

func TestLynxFlow_ParsePattern(t *testing.T) {
	q, err := Parse(`from app | parse pattern(_raw, "%{ip} - %{user}")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unpack, ok := q.Commands[0].(*UnpackCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected UnpackCommand, got %T", q.Commands[0])
	}
	if unpack.Format != "pattern" {
		t.Errorf("Format: got %q", unpack.Format)
	}
	if unpack.Pattern == "" {
		t.Error("Pattern is empty")
	}
}

func TestLynxFlow_ParseLogfmt(t *testing.T) {
	q, err := Parse(`from app | parse logfmt(_raw)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unpack := q.Commands[0].(*UnpackCommand)
	if unpack.Format != "logfmt" {
		t.Errorf("Format: got %q, want logfmt", unpack.Format)
	}
}

func TestLynxFlow_Explode(t *testing.T) {
	q, err := Parse(`from app | explode tags`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll, ok := q.Commands[0].(*UnrollCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected UnrollCommand, got %T", q.Commands[0])
	}
	if unroll.Field != "tags" {
		t.Errorf("Field: got %q, want tags", unroll.Field)
	}
	if unroll.Alias != "" {
		t.Errorf("Alias: got %q, want empty", unroll.Alias)
	}
}

func TestLynxFlow_ExplodeWithAlias(t *testing.T) {
	q, err := Parse(`from app | explode tags as tag`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll := q.Commands[0].(*UnrollCommand)
	if unroll.Field != "tags" || unroll.Alias != "tag" {
		t.Errorf("Field=%q, Alias=%q, want tags/tag", unroll.Field, unroll.Alias)
	}
}

// =============================================================================
// Spec 03 — Derivation (let)
// =============================================================================

func TestLynxFlow_LetSingle(t *testing.T) {
	q, err := Parse(`from app | let x = y + 1`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval, ok := q.Commands[0].(*EvalCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected EvalCommand, got %T", q.Commands[0])
	}
	if eval.Field != "x" {
		t.Errorf("Field: got %q, want x", eval.Field)
	}
}

func TestLynxFlow_LetMulti(t *testing.T) {
	q, err := Parse(`from app | let a = 1 + 2, b = 3 * 4`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	if len(eval.Assignments) != 2 {
		t.Fatalf("Assignments: got %d, want 2", len(eval.Assignments))
	}
	if eval.Assignments[0].Field != "a" {
		t.Errorf("Assignments[0].Field: got %q", eval.Assignments[0].Field)
	}
	if eval.Assignments[1].Field != "b" {
		t.Errorf("Assignments[1].Field: got %q", eval.Assignments[1].Field)
	}
}

func TestLynxFlow_LetWithCoalesce(t *testing.T) {
	q, err := Parse(`from app | let x = a ?? b ?? c`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	if eval.Field != "x" {
		t.Errorf("Field: got %q", eval.Field)
	}
	// The expression should be coalesce(coalesce(a, b), c)
	fn, ok := eval.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expected FuncCallExpr, got %T", eval.Expr)
	}
	if fn.Name != "coalesce" {
		t.Errorf("expected coalesce, got %q", fn.Name)
	}
}

// =============================================================================
// Spec 04 — Filtering (where)
// =============================================================================

func TestLynxFlow_WhereBasic(t *testing.T) {
	q, err := Parse(`from app | where status >= 500`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Op != ">=" {
		t.Errorf("Op: got %q, want >=", cmp.Op)
	}
}

func TestLynxFlow_WhereExistence(t *testing.T) {
	q, err := Parse(`from app | where trace_id?`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	fn, ok := where.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr, got %T", where.Expr)
	}
	if fn.Name != "isnotnull" {
		t.Errorf("Name: got %q, want isnotnull", fn.Name)
	}
	if len(fn.Args) != 1 {
		t.Fatalf("Args: got %d, want 1", len(fn.Args))
	}
	field, ok := fn.Args[0].(*FieldExpr)
	if !ok {
		t.Fatalf("arg: expected FieldExpr, got %T", fn.Args[0])
	}
	if field.Name != "trace_id" {
		t.Errorf("field: got %q, want trace_id", field.Name)
	}
}

func TestLynxFlow_WhereIn(t *testing.T) {
	q, err := Parse(`from app | where level in ("error", "fatal")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	in, ok := where.Expr.(*InExpr)
	if !ok {
		t.Fatalf("expr: expected InExpr, got %T", where.Expr)
	}
	if len(in.Values) != 2 {
		t.Errorf("Values: got %d, want 2", len(in.Values))
	}
}

func TestLynxFlow_WhereLike(t *testing.T) {
	q, err := Parse(`from app | where uri like "/api/%"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Op != "like" {
		t.Errorf("Op: got %q, want like", cmp.Op)
	}
}

func TestLynxFlow_WhereIsNull(t *testing.T) {
	q, err := Parse(`from app | where trace_id is null`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	fn, ok := where.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr (isnull), got %T", where.Expr)
	}
	if fn.Name != "isnull" {
		t.Errorf("Name: got %q, want isnull", fn.Name)
	}
}

func TestLynxFlow_WhereIsNotNull(t *testing.T) {
	q, err := Parse(`from app | where trace_id is not null`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	fn, ok := where.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr (isnotnull), got %T", where.Expr)
	}
	if fn.Name != "isnotnull" {
		t.Errorf("Name: got %q, want isnotnull", fn.Name)
	}
}

func TestLynxFlow_WhereBetween(t *testing.T) {
	q, err := Parse(`from app | where dur between 100 and 5000`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	// BETWEEN desugars to: (dur >= 100 AND dur <= 5000)
	bin, ok := where.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expr: expected BinaryExpr (AND), got %T", where.Expr)
	}
	if bin.Op != "and" {
		t.Errorf("Op: got %q, want and", bin.Op)
	}
	left, ok := bin.Left.(*CompareExpr)
	if !ok {
		t.Fatalf("left: expected CompareExpr, got %T", bin.Left)
	}
	if left.Op != ">=" {
		t.Errorf("left op: got %q, want >=", left.Op)
	}
	right, ok := bin.Right.(*CompareExpr)
	if !ok {
		t.Fatalf("right: expected CompareExpr, got %T", bin.Right)
	}
	if right.Op != "<=" {
		t.Errorf("right op: got %q, want <=", right.Op)
	}
}

// =============================================================================
// Spec 05 — Field Shaping (keep, omit, select, rename)
// =============================================================================

func TestLynxFlow_Keep(t *testing.T) {
	q, err := Parse(`from app | keep f1, f2, f3`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc, ok := q.Commands[0].(*FieldsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected FieldsCommand, got %T", q.Commands[0])
	}
	if fc.Remove {
		t.Error("expected Remove=false for keep")
	}
	if len(fc.Fields) != 3 || fc.Fields[0] != "f1" || fc.Fields[1] != "f2" || fc.Fields[2] != "f3" {
		t.Errorf("Fields: got %v", fc.Fields)
	}
}

func TestLynxFlow_Omit(t *testing.T) {
	q, err := Parse(`from app | omit f1, f2`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc := q.Commands[0].(*FieldsCommand)
	if !fc.Remove {
		t.Error("expected Remove=true for omit")
	}
	if len(fc.Fields) != 2 {
		t.Errorf("Fields: got %v", fc.Fields)
	}
}

func TestLynxFlow_Select(t *testing.T) {
	q, err := Parse(`from app | select _timestamp as time, uri as path, status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel, ok := q.Commands[0].(*SelectCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected SelectCommand, got %T", q.Commands[0])
	}
	if len(sel.Columns) != 3 {
		t.Fatalf("Columns: got %d, want 3", len(sel.Columns))
	}
	if sel.Columns[0].Name != "_timestamp" || sel.Columns[0].Alias != "time" {
		t.Errorf("col[0]: got %+v", sel.Columns[0])
	}
	if sel.Columns[1].Name != "uri" || sel.Columns[1].Alias != "path" {
		t.Errorf("col[1]: got %+v", sel.Columns[1])
	}
	if sel.Columns[2].Name != "status" || sel.Columns[2].Alias != "" {
		t.Errorf("col[2]: got %+v", sel.Columns[2])
	}
}

func TestLynxFlow_SelectStar(t *testing.T) {
	q, err := Parse(`from app | select *`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel := q.Commands[0].(*SelectCommand)
	if len(sel.Columns) != 1 || sel.Columns[0].Name != "*" {
		t.Errorf("expected select *, got %v", sel.Columns)
	}
}

func TestLynxFlow_Rename(t *testing.T) {
	q, err := Parse(`from app | rename old as new`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ren, ok := q.Commands[0].(*RenameCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected RenameCommand, got %T", q.Commands[0])
	}
	if len(ren.Renames) != 1 || ren.Renames[0].Old != "old" || ren.Renames[0].New != "new" {
		t.Errorf("Renames: got %v", ren.Renames)
	}
}

// =============================================================================
// Spec 06 — Aggregation (group, every, bucket)
// =============================================================================

func TestLynxFlow_GroupByCompute(t *testing.T) {
	q, err := Parse(`from app | group by service compute count() as total`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
	if len(stats.Aggregations) != 1 {
		t.Fatalf("Aggregations: got %d", len(stats.Aggregations))
	}
	if stats.Aggregations[0].Func != "count" {
		t.Errorf("Func: got %q", stats.Aggregations[0].Func)
	}
	if stats.Aggregations[0].Alias != "total" {
		t.Errorf("Alias: got %q, want total", stats.Aggregations[0].Alias)
	}
}

func TestLynxFlow_GroupNoBy(t *testing.T) {
	q, err := Parse(`from app | group compute count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 0 {
		t.Errorf("GroupBy: got %v, want empty", stats.GroupBy)
	}
	if len(stats.Aggregations) != 1 || stats.Aggregations[0].Func != "count" {
		t.Errorf("Aggregations: got %v", stats.Aggregations)
	}
}

func TestLynxFlow_GroupMultiByMultiCompute(t *testing.T) {
	q, err := Parse(`from app | group by service, region compute count() as n, avg(dur) as avg_dur`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 2 {
		t.Fatalf("GroupBy: got %v, want [service region]", stats.GroupBy)
	}
	if len(stats.Aggregations) != 2 {
		t.Fatalf("Aggregations: got %d, want 2", len(stats.Aggregations))
	}
}

func TestLynxFlow_Every(t *testing.T) {
	q, err := Parse(`from app | every 5m compute count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc, ok := q.Commands[0].(*TimechartCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TimechartCommand, got %T", q.Commands[0])
	}
	if tc.Span != "5m" {
		t.Errorf("Span: got %q, want 5m", tc.Span)
	}
	if len(tc.Aggregations) != 1 || tc.Aggregations[0].Func != "count" {
		t.Errorf("Aggregations: got %v", tc.Aggregations)
	}
}

func TestLynxFlow_EveryByCompute(t *testing.T) {
	q, err := Parse(`from app | every 5m by service compute count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc := q.Commands[0].(*TimechartCommand)
	if tc.Span != "5m" {
		t.Errorf("Span: got %q", tc.Span)
	}
	if len(tc.GroupBy) != 1 || tc.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", tc.GroupBy)
	}
}

func TestLynxFlow_Bucket(t *testing.T) {
	q, err := Parse(`from app | bucket _timestamp span=1h as hour`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	bin, ok := q.Commands[0].(*BinCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected BinCommand, got %T", q.Commands[0])
	}
	if bin.Field != "_timestamp" {
		t.Errorf("Field: got %q", bin.Field)
	}
	if bin.Span != "1h" {
		t.Errorf("Span: got %q", bin.Span)
	}
	if bin.Alias != "hour" {
		t.Errorf("Alias: got %q", bin.Alias)
	}
}

// =============================================================================
// Spec 07 — Ranking & Order (order by, take, rank, top, topby, bottom, bottomby, rare, dedup)
// =============================================================================

func TestLynxFlow_OrderBy(t *testing.T) {
	q, err := Parse(`from app | order by dur desc`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort, ok := q.Commands[0].(*SortCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected SortCommand, got %T", q.Commands[0])
	}
	if len(sort.Fields) != 1 {
		t.Fatalf("Fields: got %d, want 1", len(sort.Fields))
	}
	if sort.Fields[0].Name != "dur" || !sort.Fields[0].Desc {
		t.Errorf("Fields[0]: got %+v, want dur desc", sort.Fields[0])
	}
}

func TestLynxFlow_OrderByMulti(t *testing.T) {
	q, err := Parse(`from app | order by status desc, uri asc`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if len(sort.Fields) != 2 {
		t.Fatalf("Fields: got %d, want 2", len(sort.Fields))
	}
	if sort.Fields[0].Name != "status" || !sort.Fields[0].Desc {
		t.Errorf("Fields[0]: got %+v", sort.Fields[0])
	}
	if sort.Fields[1].Name != "uri" || sort.Fields[1].Desc {
		t.Errorf("Fields[1]: got %+v", sort.Fields[1])
	}
}

func TestLynxFlow_SortByDesc(t *testing.T) {
	// "sort by" delegates to order-by-style parsing.
	q, err := Parse(`from app | sort by dur desc`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if len(sort.Fields) != 1 || sort.Fields[0].Name != "dur" || !sort.Fields[0].Desc {
		t.Errorf("sort by dur desc: got %+v", sort.Fields)
	}
}

func TestLynxFlow_Take(t *testing.T) {
	q, err := Parse(`from app | take 10`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	head, ok := q.Commands[0].(*HeadCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected HeadCommand, got %T", q.Commands[0])
	}
	if head.Count != 10 {
		t.Errorf("Count: got %d, want 10", head.Count)
	}
}

func TestLynxFlow_TakeDefault(t *testing.T) {
	q, err := Parse(`from app | take`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	head := q.Commands[0].(*HeadCommand)
	if head.Count != 10 {
		t.Errorf("Count: got %d, want 10 (default)", head.Count)
	}
}

func TestLynxFlow_RankTop(t *testing.T) {
	q, err := Parse(`from app | rank top 10 by dur`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	sort, ok := q.Commands[0].(*SortCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected SortCommand, got %T", q.Commands[0])
	}
	if !sort.Fields[0].Desc || sort.Fields[0].Name != "dur" {
		t.Errorf("sort: got %+v", sort.Fields[0])
	}
	head, ok := q.Commands[1].(*HeadCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected HeadCommand, got %T", q.Commands[1])
	}
	if head.Count != 10 {
		t.Errorf("Count: got %d, want 10", head.Count)
	}
}

func TestLynxFlow_RankBottom(t *testing.T) {
	q, err := Parse(`from app | rank bottom 5 by lat`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if sort.Fields[0].Desc {
		t.Error("expected Desc=false for bottom")
	}
	head := q.Commands[1].(*HeadCommand)
	if head.Count != 5 {
		t.Errorf("Count: got %d, want 5", head.Count)
	}
}

func TestLynxFlow_TopFrequency(t *testing.T) {
	q, err := Parse(`from app | top 10 status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	top, ok := q.Commands[0].(*TopCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TopCommand, got %T", q.Commands[0])
	}
	if top.N != 10 || top.Field != "status" {
		t.Errorf("Top: got N=%d Field=%q", top.N, top.Field)
	}
}

func TestLynxFlow_TopFrequencyByGroup(t *testing.T) {
	q, err := Parse(`from app | top 5 uri by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	top := q.Commands[0].(*TopCommand)
	if top.N != 5 || top.Field != "uri" || top.ByField != "service" {
		t.Errorf("Top: got N=%d Field=%q ByField=%q", top.N, top.Field, top.ByField)
	}
}

func TestLynxFlow_TopByAgg(t *testing.T) {
	// top N field by agg() → metric ranking (topby desugaring)
	q, err := Parse(`from app | top 5 uri by avg(dur)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3 (stats+sort+head)", len(q.Commands))
	}
	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "uri" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
	_, ok = q.Commands[1].(*SortCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected SortCommand, got %T", q.Commands[1])
	}
	head := q.Commands[2].(*HeadCommand)
	if head.Count != 5 {
		t.Errorf("Head count: got %d", head.Count)
	}
}

func TestLynxFlow_Topby(t *testing.T) {
	q, err := Parse(`from app | topby 20 sku using avg(dur) compute count() as n`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3", len(q.Commands))
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "sku" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
	if len(stats.Aggregations) != 2 {
		t.Fatalf("Aggregations: got %d, want 2", len(stats.Aggregations))
	}
	if stats.Aggregations[0].Func != "avg" {
		t.Errorf("Agg[0].Func: got %q, want avg", stats.Aggregations[0].Func)
	}
	if stats.Aggregations[1].Func != "count" || stats.Aggregations[1].Alias != "n" {
		t.Errorf("Agg[1]: got %+v", stats.Aggregations[1])
	}
	sort := q.Commands[1].(*SortCommand)
	if !sort.Fields[0].Desc {
		t.Error("expected desc sort for topby")
	}
	head := q.Commands[2].(*HeadCommand)
	if head.Count != 20 {
		t.Errorf("Head count: got %d, want 20", head.Count)
	}
}

func TestLynxFlow_Bottomby(t *testing.T) {
	q, err := Parse(`from app | bottomby 5 svc using count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3", len(q.Commands))
	}
	sort := q.Commands[1].(*SortCommand)
	if sort.Fields[0].Desc {
		t.Error("expected asc sort for bottomby")
	}
	head := q.Commands[2].(*HeadCommand)
	if head.Count != 5 {
		t.Errorf("Head count: got %d", head.Count)
	}
}

func TestLynxFlow_BottomFrequency(t *testing.T) {
	q, err := Parse(`from app | bottom 10 uri`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rare, ok := q.Commands[0].(*RareCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected RareCommand, got %T", q.Commands[0])
	}
	if rare.N != 10 || rare.Field != "uri" {
		t.Errorf("Rare: N=%d Field=%q", rare.N, rare.Field)
	}
}

func TestLynxFlow_Rare(t *testing.T) {
	q, err := Parse(`from app | rare 10 status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	rare := q.Commands[0].(*RareCommand)
	if rare.N != 10 || rare.Field != "status" {
		t.Errorf("Rare: N=%d Field=%q", rare.N, rare.Field)
	}
}

func TestLynxFlow_Dedup(t *testing.T) {
	q, err := Parse(`from app | dedup user_id`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dd, ok := q.Commands[0].(*DedupCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected DedupCommand, got %T", q.Commands[0])
	}
	if len(dd.Fields) != 1 || dd.Fields[0] != "user_id" {
		t.Errorf("Fields: got %v", dd.Fields)
	}
}

func TestLynxFlow_DedupN(t *testing.T) {
	q, err := Parse(`from app | dedup 3 service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dd := q.Commands[0].(*DedupCommand)
	if dd.Limit != 3 {
		t.Errorf("Limit: got %d, want 3", dd.Limit)
	}
}

// =============================================================================
// Spec 08 — Combining (join, lookup, append, multisearch, transaction)
// =============================================================================

func TestLynxFlow_JoinInner(t *testing.T) {
	q, err := Parse(`from app | join type=inner host [from logs | stats count() by host]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	join, ok := q.Commands[0].(*JoinCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected JoinCommand, got %T", q.Commands[0])
	}
	if join.JoinType != "inner" {
		t.Errorf("JoinType: got %q", join.JoinType)
	}
	if join.Field != "host" {
		t.Errorf("Field: got %q", join.Field)
	}
}

func TestLynxFlow_Lookup(t *testing.T) {
	q, err := Parse(`from app | lookup geo_db on client_ip`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	join, ok := q.Commands[0].(*JoinCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected JoinCommand, got %T", q.Commands[0])
	}
	if join.JoinType != "left" {
		t.Errorf("JoinType: got %q, want left", join.JoinType)
	}
	if join.Field != "client_ip" {
		t.Errorf("Field: got %q", join.Field)
	}
	if join.Subquery == nil || join.Subquery.Source == nil || join.Subquery.Source.Index != "geo_db" {
		t.Errorf("Subquery source: got %v", join.Subquery)
	}
}

func TestLynxFlow_Append(t *testing.T) {
	q, err := Parse(`from app | append [from logs | stats count()]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	app, ok := q.Commands[0].(*AppendCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected AppendCommand, got %T", q.Commands[0])
	}
	if app.Subquery == nil {
		t.Error("Subquery is nil")
	}
}

func TestLynxFlow_Transaction(t *testing.T) {
	q, err := Parse(`from app | transaction session_id maxspan=30m`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tx, ok := q.Commands[0].(*TransactionCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TransactionCommand, got %T", q.Commands[0])
	}
	if tx.Field != "session_id" {
		t.Errorf("Field: got %q", tx.Field)
	}
	if tx.MaxSpan != "30m" {
		t.Errorf("MaxSpan: got %q", tx.MaxSpan)
	}
}

// =============================================================================
// Spec 09 — Window Ops (running, enrich)
// =============================================================================

func TestLynxFlow_Running(t *testing.T) {
	q, err := Parse(`from app | running count() as n`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ss, ok := q.Commands[0].(*StreamstatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StreamstatsCommand, got %T", q.Commands[0])
	}
	if len(ss.Aggregations) != 1 || ss.Aggregations[0].Func != "count" || ss.Aggregations[0].Alias != "n" {
		t.Errorf("Aggregations: got %v", ss.Aggregations)
	}
}

func TestLynxFlow_RunningWindow(t *testing.T) {
	q, err := Parse(`from app | running window=10 avg(x) as ra`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ss := q.Commands[0].(*StreamstatsCommand)
	if ss.Window != 10 {
		t.Errorf("Window: got %d, want 10", ss.Window)
	}
	if len(ss.Aggregations) != 1 || ss.Aggregations[0].Alias != "ra" {
		t.Errorf("Aggregations: got %v", ss.Aggregations)
	}
}

func TestLynxFlow_Enrich(t *testing.T) {
	q, err := Parse(`from app | enrich avg(x) as ga`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	es, ok := q.Commands[0].(*EventstatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected EventstatsCommand, got %T", q.Commands[0])
	}
	if len(es.Aggregations) != 1 || es.Aggregations[0].Alias != "ga" {
		t.Errorf("Aggregations: got %v", es.Aggregations)
	}
}

func TestLynxFlow_EnrichByGroup(t *testing.T) {
	q, err := Parse(`from app | enrich avg(x) as sa by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	es := q.Commands[0].(*EventstatsCommand)
	if len(es.GroupBy) != 1 || es.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", es.GroupBy)
	}
}

// =============================================================================
// Spec 10 — Null Handling (fillnull, ??, ?)
// =============================================================================

func TestLynxFlow_Fillnull(t *testing.T) {
	q, err := Parse(`from app | fillnull value="N/A" f1, f2`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fn, ok := q.Commands[0].(*FillnullCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected FillnullCommand, got %T", q.Commands[0])
	}
	if fn.Value != "N/A" {
		t.Errorf("Value: got %q", fn.Value)
	}
	if len(fn.Fields) != 2 {
		t.Errorf("Fields: got %v", fn.Fields)
	}
}

func TestLynxFlow_NullCoalesce(t *testing.T) {
	q, err := Parse(`from app | let x = a ?? b ?? c`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	// a ?? b ?? c → coalesce(coalesce(a, b), c) (left-associative)
	outer, ok := eval.Expr.(*FuncCallExpr)
	if !ok || outer.Name != "coalesce" {
		t.Fatalf("expr: expected coalesce, got %T / %v", eval.Expr, eval.Expr)
	}
	if len(outer.Args) != 2 {
		t.Fatalf("outer coalesce args: got %d, want 2", len(outer.Args))
	}
	inner, ok := outer.Args[0].(*FuncCallExpr)
	if !ok || inner.Name != "coalesce" {
		t.Fatalf("inner: expected coalesce, got %T", outer.Args[0])
	}
	aField, ok := inner.Args[0].(*FieldExpr)
	if !ok || aField.Name != "a" {
		t.Errorf("inner arg0: got %v", inner.Args[0])
	}
}

func TestLynxFlow_ExistenceOperator(t *testing.T) {
	q, err := Parse(`from app | where trace_id?`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	fn := where.Expr.(*FuncCallExpr)
	if fn.Name != "isnotnull" {
		t.Errorf("Name: got %q", fn.Name)
	}
}

// =============================================================================
// Spec 11 — Presentation (table, xyseries, pack)
// =============================================================================

func TestLynxFlow_Table(t *testing.T) {
	q, err := Parse(`from app | table f1, f2`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tbl, ok := q.Commands[0].(*TableCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TableCommand, got %T", q.Commands[0])
	}
	if len(tbl.Fields) != 2 {
		t.Errorf("Fields: got %v", tbl.Fields)
	}
}

func TestLynxFlow_XYSeries(t *testing.T) {
	q, err := Parse(`from app | xyseries x y v`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	xy, ok := q.Commands[0].(*XYSeriesCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected XYSeriesCommand, got %T", q.Commands[0])
	}
	if xy.XField != "x" || xy.YField != "y" || xy.ValueField != "v" {
		t.Errorf("Fields: got %q %q %q", xy.XField, xy.YField, xy.ValueField)
	}
}

func TestLynxFlow_PackFields(t *testing.T) {
	q, err := Parse(`from app | pack f1, f2 into target`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pk, ok := q.Commands[0].(*PackJsonCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected PackJsonCommand, got %T", q.Commands[0])
	}
	if len(pk.Fields) != 2 || pk.Fields[0] != "f1" || pk.Fields[1] != "f2" {
		t.Errorf("Fields: got %v", pk.Fields)
	}
	if pk.Target != "target" {
		t.Errorf("Target: got %q", pk.Target)
	}
}

func TestLynxFlow_PackInto(t *testing.T) {
	q, err := Parse(`from app | pack into target`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	pk := q.Commands[0].(*PackJsonCommand)
	if len(pk.Fields) != 0 {
		t.Errorf("Fields: got %v, want empty", pk.Fields)
	}
	if pk.Target != "target" {
		t.Errorf("Target: got %q", pk.Target)
	}
}

// =============================================================================
// Spec 12 — Domain Sugar (latency, errors, rate, percentiles, slowest)
// =============================================================================

func TestLynxFlow_Latency(t *testing.T) {
	q, err := Parse(`from app | latency dur every 5m by svc`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc, ok := q.Commands[0].(*TimechartCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TimechartCommand, got %T", q.Commands[0])
	}
	if tc.Span != "5m" {
		t.Errorf("Span: got %q", tc.Span)
	}
	if len(tc.GroupBy) != 1 || tc.GroupBy[0] != "svc" {
		t.Errorf("GroupBy: got %v", tc.GroupBy)
	}
	// Default: perc50, perc95, perc99, count.
	if len(tc.Aggregations) != 4 {
		t.Fatalf("Aggregations: got %d, want 4", len(tc.Aggregations))
	}
	if tc.Aggregations[0].Func != "perc50" {
		t.Errorf("Agg[0].Func: got %q, want perc50", tc.Aggregations[0].Func)
	}
	if tc.Aggregations[1].Func != "perc95" {
		t.Errorf("Agg[1].Func: got %q, want perc95", tc.Aggregations[1].Func)
	}
	if tc.Aggregations[2].Func != "perc99" {
		t.Errorf("Agg[2].Func: got %q, want perc99", tc.Aggregations[2].Func)
	}
	if tc.Aggregations[3].Func != "count" {
		t.Errorf("Agg[3].Func: got %q, want count", tc.Aggregations[3].Func)
	}
}

func TestLynxFlow_LatencyCustomCompute(t *testing.T) {
	q, err := Parse(`from app | latency dur every 1m compute p50, p99, avg`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc := q.Commands[0].(*TimechartCommand)
	if len(tc.Aggregations) != 3 {
		t.Fatalf("Aggregations: got %d, want 3", len(tc.Aggregations))
	}
	if tc.Aggregations[0].Func != "perc50" {
		t.Errorf("Agg[0].Func: got %q, want perc50", tc.Aggregations[0].Func)
	}
	if tc.Aggregations[1].Func != "perc99" {
		t.Errorf("Agg[1].Func: got %q, want perc99", tc.Aggregations[1].Func)
	}
	if tc.Aggregations[2].Func != "avg" {
		t.Errorf("Agg[2].Func: got %q, want avg", tc.Aggregations[2].Func)
	}
}

func TestLynxFlow_Errors(t *testing.T) {
	q, err := Parse(`from app | errors by service compute count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2 (where + stats)", len(q.Commands))
	}
	// Where: level in ("error", "fatal")
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	in, ok := where.Expr.(*InExpr)
	if !ok {
		t.Fatalf("where expr: expected InExpr, got %T", where.Expr)
	}
	if len(in.Values) != 2 {
		t.Errorf("Values: got %d", len(in.Values))
	}
	// Stats: count() by service
	stats, ok := q.Commands[1].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected StatsCommand, got %T", q.Commands[1])
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
}

func TestLynxFlow_ErrorsDefault(t *testing.T) {
	// errors without explicit compute → default count()
	q, err := Parse(`from app | errors`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	stats := q.Commands[1].(*StatsCommand)
	if len(stats.Aggregations) != 1 || stats.Aggregations[0].Func != "count" {
		t.Errorf("Aggregations: got %v", stats.Aggregations)
	}
}

func TestLynxFlow_Rate(t *testing.T) {
	q, err := Parse(`from app | rate per 5m by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc, ok := q.Commands[0].(*TimechartCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TimechartCommand, got %T", q.Commands[0])
	}
	if tc.Span != "5m" {
		t.Errorf("Span: got %q, want 5m", tc.Span)
	}
	if len(tc.Aggregations) != 1 || tc.Aggregations[0].Func != "count" || tc.Aggregations[0].Alias != "rate" {
		t.Errorf("Aggregations: got %v", tc.Aggregations)
	}
	if len(tc.GroupBy) != 1 || tc.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", tc.GroupBy)
	}
}

func TestLynxFlow_RateDefault(t *testing.T) {
	q, err := Parse(`from app | rate`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc := q.Commands[0].(*TimechartCommand)
	if tc.Span != "1m" {
		t.Errorf("Span: got %q, want 1m (default)", tc.Span)
	}
}

func TestLynxFlow_Percentiles(t *testing.T) {
	q, err := Parse(`from app | percentiles dur by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.Aggregations) != 5 {
		t.Fatalf("Aggregations: got %d, want 5 (p50/p75/p90/p95/p99)", len(stats.Aggregations))
	}
	expected := []string{"perc50", "perc75", "perc90", "perc95", "perc99"}
	for i, want := range expected {
		if stats.Aggregations[i].Func != want {
			t.Errorf("Agg[%d].Func: got %q, want %q", i, stats.Aggregations[i].Func, want)
		}
	}
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
}

func TestLynxFlow_SlowestRow(t *testing.T) {
	q, err := Parse(`from app | slowest 10 by duration_ms`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2 (sort + head)", len(q.Commands))
	}
	sort := q.Commands[0].(*SortCommand)
	if !sort.Fields[0].Desc || sort.Fields[0].Name != "duration_ms" {
		t.Errorf("sort: got %+v", sort.Fields[0])
	}
	head := q.Commands[1].(*HeadCommand)
	if head.Count != 10 {
		t.Errorf("head count: got %d", head.Count)
	}
}

func TestLynxFlow_SlowestGroup(t *testing.T) {
	q, err := Parse(`from app | slowest 10 uri by duration_ms`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3 (stats + sort + head)", len(q.Commands))
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "uri" {
		t.Errorf("GroupBy: got %v", stats.GroupBy)
	}
	if len(stats.Aggregations) != 1 || stats.Aggregations[0].Func != "max" {
		t.Errorf("Aggregations: got %v", stats.Aggregations)
	}
}

func TestLynxFlow_SlowestDefault(t *testing.T) {
	// slowest with no count → default 10
	q, err := Parse(`from app | slowest`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	sort := q.Commands[0].(*SortCommand)
	if sort.Fields[0].Name != "duration_ms" {
		t.Errorf("default dur field: got %q, want duration_ms", sort.Fields[0].Name)
	}
	head := q.Commands[1].(*HeadCommand)
	if head.Count != 10 {
		t.Errorf("default count: got %d, want 10", head.Count)
	}
}

// =============================================================================
// Spec 13 — Views & CTEs
// =============================================================================

func TestLynxFlow_Materialize(t *testing.T) {
	q, err := Parse(`from app | stats count() by service | materialize "mv_errors" retention=90d`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) < 2 {
		t.Fatalf("Commands: got %d", len(q.Commands))
	}
	mat, ok := q.Commands[len(q.Commands)-1].(*MaterializeCommand)
	if !ok {
		t.Fatalf("last cmd: expected MaterializeCommand, got %T", q.Commands[len(q.Commands)-1])
	}
	if mat.Name != "mv_errors" {
		t.Errorf("Name: got %q", mat.Name)
	}
	if mat.Retention != "90d" {
		t.Errorf("Retention: got %q", mat.Retention)
	}
}

func TestLynxFlow_Views(t *testing.T) {
	q, err := Parse(`| views`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d", len(q.Commands))
	}
	_, ok := q.Commands[0].(*ViewsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected ViewsCommand, got %T", q.Commands[0])
	}
}

func TestLynxFlow_Dropview(t *testing.T) {
	q, err := Parse(`| dropview "mv_errors"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	dv, ok := q.Commands[0].(*DropviewCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected DropviewCommand, got %T", q.Commands[0])
	}
	if dv.Name != "mv_errors" {
		t.Errorf("Name: got %q", dv.Name)
	}
}

func TestLynxFlow_CTE(t *testing.T) {
	input := `$x = from app | where level="error"; from $x | stats count()`
	prog, err := ParseProgram(input)
	if err != nil {
		t.Fatalf("ParseProgram: %v", err)
	}
	if len(prog.Datasets) != 1 {
		t.Fatalf("Datasets: got %d, want 1", len(prog.Datasets))
	}
	if prog.Datasets[0].Name != "x" {
		t.Errorf("Dataset name: got %q, want x", prog.Datasets[0].Name)
	}
	if prog.Main.Source == nil || !prog.Main.Source.IsVariable {
		t.Error("expected main query to reference $x variable")
	}
}

// =============================================================================
// Spec 14 — Expression Extensions (%, ??, ?, between, is null)
// =============================================================================

func TestLynxFlow_ModuloOperator(t *testing.T) {
	q, err := Parse(`from app | let x = y % 10`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	arith, ok := eval.Expr.(*ArithExpr)
	if !ok {
		t.Fatalf("expr: expected ArithExpr, got %T", eval.Expr)
	}
	if arith.Op != "%" {
		t.Errorf("Op: got %q, want %%", arith.Op)
	}
}

func TestLynxFlow_ModuloPrecedence(t *testing.T) {
	// % has same precedence as * and /: a + b % c → a + (b % c)
	q, err := Parse(`from app | let x = a + b % c`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	// Top-level should be addition.
	add, ok := eval.Expr.(*ArithExpr)
	if !ok {
		t.Fatalf("expr: expected ArithExpr, got %T", eval.Expr)
	}
	if add.Op != "+" {
		t.Errorf("top-level op: got %q, want +", add.Op)
	}
	// Right side should be modulo.
	mod, ok := add.Right.(*ArithExpr)
	if !ok {
		t.Fatalf("right: expected ArithExpr, got %T", add.Right)
	}
	if mod.Op != "%" {
		t.Errorf("right op: got %q, want %%", mod.Op)
	}
}

func TestLynxFlow_NullCoalescePrecedence(t *testing.T) {
	// ?? has lower precedence than OR: a OR b ?? c → (a OR b) ?? c
	q, err := Parse(`from app | where a or b ?? c`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	// Top-level should be coalesce.
	fn, ok := where.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr (coalesce), got %T", where.Expr)
	}
	if fn.Name != "coalesce" {
		t.Errorf("Name: got %q, want coalesce", fn.Name)
	}
	// Left arg should be the OR expression.
	_, ok = fn.Args[0].(*BinaryExpr)
	if !ok {
		t.Fatalf("arg[0]: expected BinaryExpr (or), got %T", fn.Args[0])
	}
}

func TestLynxFlow_ExistenceNotConsumeDoubleQuestion(t *testing.T) {
	// field ?? default — the ?? should NOT be consumed by the ? postfix.
	q, err := Parse(`from app | let x = field ?? "default"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	fn, ok := eval.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: expected FuncCallExpr (coalesce), got %T", eval.Expr)
	}
	if fn.Name != "coalesce" {
		t.Errorf("Name: got %q, want coalesce", fn.Name)
	}
}

// =============================================================================
// Lexer Tests
// =============================================================================

func TestLynxFlow_LexerDoubleQuestion(t *testing.T) {
	tokens, err := NewLexer("a ?? b").Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	// a, ??, b, EOF
	if len(tokens) != 4 {
		t.Fatalf("tokens: got %d, want 4", len(tokens))
	}
	if tokens[1].Type != TokenDoubleQuestion {
		t.Errorf("token[1]: got %s, want DOUBLE_QUESTION", tokens[1].Type)
	}
}

func TestLynxFlow_LexerSingleQuestion(t *testing.T) {
	tokens, err := NewLexer("field?").Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	// field, ?, EOF
	if len(tokens) != 3 {
		t.Fatalf("tokens: got %d, want 3", len(tokens))
	}
	if tokens[0].Type != TokenIdent || tokens[0].Literal != "field" {
		t.Errorf("token[0]: got %v", tokens[0])
	}
	if tokens[1].Type != TokenQuestionMark {
		t.Errorf("token[1]: got %s, want QUESTION", tokens[1].Type)
	}
}

func TestLynxFlow_LexerPercent(t *testing.T) {
	tokens, err := NewLexer("a % b").Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	if len(tokens) != 4 {
		t.Fatalf("tokens: got %d, want 4", len(tokens))
	}
	if tokens[1].Type != TokenPercent {
		t.Errorf("token[1]: got %s, want PERCENT", tokens[1].Type)
	}
}

func TestLynxFlow_LexerLineComment(t *testing.T) {
	tokens, err := NewLexer("a -- this is a comment\nb").Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	// a, b, EOF (comment skipped)
	if len(tokens) != 3 {
		t.Fatalf("tokens: got %d, want 3", len(tokens))
	}
	if tokens[0].Literal != "a" || tokens[1].Literal != "b" {
		t.Errorf("tokens: got %v %v", tokens[0], tokens[1])
	}
}

func TestLynxFlow_LexerCommentDoesNotBreakMinus(t *testing.T) {
	// Single dash should still work as minus operator.
	tokens, err := NewLexer("a - b").Tokenize()
	if err != nil {
		t.Fatalf("Tokenize: %v", err)
	}
	if len(tokens) != 4 {
		t.Fatalf("tokens: got %d, want 4", len(tokens))
	}
	if tokens[1].Type != TokenMinus {
		t.Errorf("token[1]: got %s, want MINUS", tokens[1].Type)
	}
}

func TestLynxFlow_LexerSortDashField(t *testing.T) {
	// "sort -count" should still parse correctly (not treated as comment).
	q, err := Parse(`from app | sort -count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if len(sort.Fields) != 1 || sort.Fields[0].Name != "count" || !sort.Fields[0].Desc {
		t.Errorf("sort: got %+v", sort.Fields)
	}
}

func TestLynxFlow_LexerKeywordsAsTokens(t *testing.T) {
	// "group", "order", "select" should lex as keyword tokens, not idents.
	tests := []struct {
		word     string
		expected TokenType
	}{
		{"let", TokenLet},
		{"keep", TokenKeep},
		{"omit", TokenOmit},
		{"select", TokenSelect},
		{"group", TokenGroup},
		{"every", TokenEvery},
		{"bucket", TokenBucket},
		{"order", TokenOrder},
		{"take", TokenTake},
		{"rank", TokenRank},
		{"topby", TokenTopby},
		{"bottomby", TokenBottomby},
		{"running", TokenRunning},
		{"enrich", TokenEnrich},
		{"explode", TokenExplode},
		{"pack", TokenPack},
		{"lookup", TokenLookup},
		{"using", TokenUsing},
		{"extract", TokenExtract},
		{"if_missing", TokenIfMissing},
		{"per", TokenPer},
		{"on", TokenOn},
		{"into", TokenInto},
		{"asc", TokenAsc},
		{"desc", TokenDesc},
		{"latency", TokenLatency},
		{"errors", TokenErrors},
		{"rate", TokenRate},
		{"percentiles", TokenPercentiles},
		{"slowest", TokenSlowest},
	}
	for _, tt := range tests {
		tokens, err := NewLexer(tt.word).Tokenize()
		if err != nil {
			t.Errorf("Tokenize(%q): %v", tt.word, err)
			continue
		}
		if tokens[0].Type != tt.expected {
			t.Errorf("Tokenize(%q): got %s, want %s", tt.word, tokens[0].Type, tt.expected)
		}
	}
}

// =============================================================================
// Equivalence Tests — Lynx Flow and SPL2 produce identical AST structure
// =============================================================================

func TestLynxFlow_Equiv_KeepIsFields(t *testing.T) {
	lf, err := Parse(`from app | keep f1, f2`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | fields f1, f2`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfCmd := lf.Commands[0].(*FieldsCommand)
	splCmd := spl.Commands[0].(*FieldsCommand)
	if lfCmd.Remove != splCmd.Remove {
		t.Errorf("Remove: LF=%v SPL=%v", lfCmd.Remove, splCmd.Remove)
	}
	if len(lfCmd.Fields) != len(splCmd.Fields) {
		t.Errorf("Fields len: LF=%d SPL=%d", len(lfCmd.Fields), len(splCmd.Fields))
	}
}

func TestLynxFlow_Equiv_OmitIsFieldsMinus(t *testing.T) {
	lf, err := Parse(`from app | omit f1`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | fields - f1`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfCmd := lf.Commands[0].(*FieldsCommand)
	splCmd := spl.Commands[0].(*FieldsCommand)
	if lfCmd.Remove != splCmd.Remove {
		t.Errorf("Remove: LF=%v SPL=%v", lfCmd.Remove, splCmd.Remove)
	}
}

func TestLynxFlow_Equiv_LetIsEval(t *testing.T) {
	lf, err := Parse(`from app | let x = y + 1`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | eval x = y + 1`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfCmd := lf.Commands[0].(*EvalCommand)
	splCmd := spl.Commands[0].(*EvalCommand)
	if lfCmd.Field != splCmd.Field {
		t.Errorf("Field: LF=%q SPL=%q", lfCmd.Field, splCmd.Field)
	}
}

func TestLynxFlow_Equiv_TakeIsHead(t *testing.T) {
	lf, err := Parse(`from app | take 20`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | head 20`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfCmd := lf.Commands[0].(*HeadCommand)
	splCmd := spl.Commands[0].(*HeadCommand)
	if lfCmd.Count != splCmd.Count {
		t.Errorf("Count: LF=%d SPL=%d", lfCmd.Count, splCmd.Count)
	}
}

func TestLynxFlow_Equiv_OrderByIsSort(t *testing.T) {
	lf, err := Parse(`from app | order by dur desc`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | sort -dur`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfSort := lf.Commands[0].(*SortCommand)
	splSort := spl.Commands[0].(*SortCommand)
	if lfSort.Fields[0].Name != splSort.Fields[0].Name {
		t.Errorf("Name: LF=%q SPL=%q", lfSort.Fields[0].Name, splSort.Fields[0].Name)
	}
	if lfSort.Fields[0].Desc != splSort.Fields[0].Desc {
		t.Errorf("Desc: LF=%v SPL=%v", lfSort.Fields[0].Desc, splSort.Fields[0].Desc)
	}
}

func TestLynxFlow_Equiv_GroupIsStats(t *testing.T) {
	lf, err := Parse(`from app | group by host compute count() as n`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | stats count() as n by host`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfStats := lf.Commands[0].(*StatsCommand)
	splStats := spl.Commands[0].(*StatsCommand)
	if len(lfStats.GroupBy) != len(splStats.GroupBy) || lfStats.GroupBy[0] != splStats.GroupBy[0] {
		t.Errorf("GroupBy: LF=%v SPL=%v", lfStats.GroupBy, splStats.GroupBy)
	}
	if len(lfStats.Aggregations) != len(splStats.Aggregations) {
		t.Errorf("Aggs: LF=%d SPL=%d", len(lfStats.Aggregations), len(splStats.Aggregations))
	}
}

func TestLynxFlow_Equiv_RunningIsStreamstats(t *testing.T) {
	lf, err := Parse(`from app | running count() as n`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | streamstats count() as n`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfSS := lf.Commands[0].(*StreamstatsCommand)
	splSS := spl.Commands[0].(*StreamstatsCommand)
	if lfSS.Aggregations[0].Func != splSS.Aggregations[0].Func {
		t.Errorf("Func: LF=%q SPL=%q", lfSS.Aggregations[0].Func, splSS.Aggregations[0].Func)
	}
}

func TestLynxFlow_Equiv_EnrichIsEventstats(t *testing.T) {
	lf, err := Parse(`from app | enrich avg(x) as ga`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | eventstats avg(x) as ga`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfES := lf.Commands[0].(*EventstatsCommand)
	splES := spl.Commands[0].(*EventstatsCommand)
	if lfES.Aggregations[0].Func != splES.Aggregations[0].Func {
		t.Errorf("Func: LF=%q SPL=%q", lfES.Aggregations[0].Func, splES.Aggregations[0].Func)
	}
}

func TestLynxFlow_Equiv_ExplodeIsUnroll(t *testing.T) {
	lf, err := Parse(`from app | explode tags`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | unroll field=tags`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfCmd := lf.Commands[0].(*UnrollCommand)
	splCmd := spl.Commands[0].(*UnrollCommand)
	if lfCmd.Field != splCmd.Field {
		t.Errorf("Field: LF=%q SPL=%q", lfCmd.Field, splCmd.Field)
	}
}

// =============================================================================
// Error Tests
// =============================================================================

func TestLynxFlow_Error_ParseUnknownFormat(t *testing.T) {
	_, err := Parse(`from app | parse foobar(_raw)`)
	// Should still parse (format=foobar), the execution engine validates formats.
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestLynxFlow_Error_GroupMissingCompute(t *testing.T) {
	_, err := Parse(`from app | group by host`)
	if err == nil {
		t.Fatal("expected error for missing 'compute'")
	}
	if !strings.Contains(err.Error(), "compute") {
		t.Errorf("expected 'compute' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_OrderMissingBy(t *testing.T) {
	_, err := Parse(`from app | order dur`)
	if err == nil {
		t.Fatal("expected error for missing 'by' after 'order'")
	}
	if !strings.Contains(err.Error(), "by") {
		t.Errorf("expected 'by' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_EveryMissingCompute(t *testing.T) {
	_, err := Parse(`from app | every 5m`)
	if err == nil {
		t.Fatal("expected error for missing 'compute'")
	}
	if !strings.Contains(err.Error(), "compute") {
		t.Errorf("expected 'compute' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_RankMissingDirection(t *testing.T) {
	_, err := Parse(`from app | rank 10 by dur`)
	if err == nil {
		t.Fatal("expected error for missing 'top' or 'bottom'")
	}
}

func TestLynxFlow_Error_LookupMissingOn(t *testing.T) {
	_, err := Parse(`from app | lookup geo_db`)
	if err == nil {
		t.Fatal("expected error for missing 'on'")
	}
	if !strings.Contains(err.Error(), "on") {
		t.Errorf("expected 'on' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_TopbyMissingUsing(t *testing.T) {
	_, err := Parse(`from app | topby 10 uri`)
	if err == nil {
		t.Fatal("expected error for missing 'using'")
	}
	if !strings.Contains(err.Error(), "using") {
		t.Errorf("expected 'using' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_LatencyMissingEvery(t *testing.T) {
	_, err := Parse(`from app | latency dur`)
	if err == nil {
		t.Fatal("expected error for missing 'every'")
	}
	if !strings.Contains(err.Error(), "every") {
		t.Errorf("expected 'every' in error, got: %v", err)
	}
}

// =============================================================================
// Keyword-as-Field-Name Tests
// =============================================================================

func TestLynxFlow_KeywordAsFieldInWhere(t *testing.T) {
	// "order" is a keyword but should work as a field name in expressions.
	q, err := Parse(`from app | where order >= 5`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	cmp := where.Expr.(*CompareExpr)
	field := cmp.Left.(*FieldExpr)
	if field.Name != "order" {
		t.Errorf("field name: got %q, want order", field.Name)
	}
}

func TestLynxFlow_KeywordAsFieldInStats(t *testing.T) {
	q, err := Parse(`from app | stats count() by group`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "group" {
		t.Errorf("GroupBy: got %v, want [group]", stats.GroupBy)
	}
}

func TestLynxFlow_KeywordAsFieldInKeep(t *testing.T) {
	q, err := Parse(`from app | keep select, order, group`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fc := q.Commands[0].(*FieldsCommand)
	expected := []string{"select", "order", "group"}
	if len(fc.Fields) != 3 {
		t.Fatalf("Fields: got %v", fc.Fields)
	}
	for i, want := range expected {
		if fc.Fields[i] != want {
			t.Errorf("Fields[%d]: got %q, want %q", i, fc.Fields[i], want)
		}
	}
}

func TestLynxFlow_BucketAsAlias(t *testing.T) {
	// Regression: "AS bucket" must work because bucket is now a keyword.
	q, err := Parse(`FROM main | BIN _time span=1m AS bucket`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	bin := q.Commands[0].(*BinCommand)
	if bin.Alias != "bucket" {
		t.Errorf("Alias: got %q, want bucket", bin.Alias)
	}
}

// =============================================================================
// Pipeline Tests — Multi-command Lynx Flow queries
// =============================================================================

func TestLynxFlow_Pipeline_Full(t *testing.T) {
	input := `from nginx | parse json(_raw) | where status >= 500 | group by service compute count() as errors | order by errors desc | take 10`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 5 {
		t.Fatalf("Commands: got %d, want 5", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*UnpackCommand); !ok {
		t.Errorf("cmd[0]: expected UnpackCommand, got %T", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*WhereCommand); !ok {
		t.Errorf("cmd[1]: expected WhereCommand, got %T", q.Commands[1])
	}
	if _, ok := q.Commands[2].(*StatsCommand); !ok {
		t.Errorf("cmd[2]: expected StatsCommand, got %T", q.Commands[2])
	}
	if _, ok := q.Commands[3].(*SortCommand); !ok {
		t.Errorf("cmd[3]: expected SortCommand, got %T", q.Commands[3])
	}
	if _, ok := q.Commands[4].(*HeadCommand); !ok {
		t.Errorf("cmd[4]: expected HeadCommand, got %T", q.Commands[4])
	}
}

func TestLynxFlow_Pipeline_MixedSyntax(t *testing.T) {
	// Mix SPL2 and Lynx Flow commands in same pipeline.
	input := `from app | where level="error" | group by service compute count() | sort -count | head 5`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 4 {
		t.Fatalf("Commands: got %d, want 4", len(q.Commands))
	}
}

func TestLynxFlow_Pipeline_DomainSugarMultiCmd(t *testing.T) {
	// errors desugars to WHERE + STATS — verify the pipeline length is correct.
	input := `from app | errors by service | order by count desc | take 5`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// errors → [WhereCommand, StatsCommand] + SortCommand + HeadCommand = 4
	if len(q.Commands) != 4 {
		t.Fatalf("Commands: got %d, want 4", len(q.Commands))
	}
}

// =============================================================================
// Normalize Tests — Lynx Flow commands recognized by normalizer
// =============================================================================

func TestLynxFlow_NormalizeKnownCommands(t *testing.T) {
	// All Lynx Flow commands should be in the knownCommands list.
	lfCommands := []string{
		"let", "keep", "omit", "select", "group", "every", "bucket",
		"order", "take", "rank", "topby", "bottomby", "bottom",
		"running", "enrich", "parse", "explode", "pack", "lookup",
		"latency", "errors", "rate", "percentiles", "slowest",
		"views", "dropview",
	}
	for _, cmd := range lfCommands {
		if !isKnownCommand(cmd) {
			t.Errorf("command %q not in knownCommands", cmd)
		}
	}
}

// =============================================================================
// Bare Expression as Implicit Where (Spec 04 — Accepted alias)
// =============================================================================

func TestLynxFlow_BareExprSimple(t *testing.T) {
	q, err := Parse(`from nginx | status >= 500`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("Expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Op != ">=" {
		t.Errorf("Op: got %q, want >=", cmp.Op)
	}
}

func TestLynxFlow_BareExprNot(t *testing.T) {
	q, err := Parse(`from nginx | NOT status = 200`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	if _, ok := where.Expr.(*NotExpr); !ok {
		t.Errorf("Expr: expected NotExpr, got %T", where.Expr)
	}
}

func TestLynxFlow_BareExprCompound(t *testing.T) {
	q, err := Parse(`from nginx | status >= 500 AND method = "POST"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	bin, ok := where.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("Expr: expected BinaryExpr, got %T", where.Expr)
	}
	if bin.Op != "and" {
		t.Errorf("Op: got %q, want and", bin.Op)
	}
}

func TestLynxFlow_BareExprInPipeline(t *testing.T) {
	q, err := Parse(`from nginx | status >= 500 | group by uri compute count()`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*WhereCommand); !ok {
		t.Errorf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*StatsCommand); !ok {
		t.Errorf("cmd[1]: expected StatsCommand, got %T", q.Commands[1])
	}
}

func TestLynxFlow_BareExprParenthesized(t *testing.T) {
	q, err := Parse(`from nginx | (status = 404 OR status = 500)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	if _, ok := where.Expr.(*BinaryExpr); !ok {
		t.Errorf("Expr: expected BinaryExpr, got %T", where.Expr)
	}
}

// =============================================================================
// Additional Filter Tests
// =============================================================================

func TestLynxFlow_WhereRegex(t *testing.T) {
	q, err := Parse(`from nginx | where uri =~ "^/api/"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("Expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Op != "=~" {
		t.Errorf("Op: got %q, want =~", cmp.Op)
	}
}

// =============================================================================
// Additional Ranking/Order Tests
// =============================================================================

func TestLynxFlow_Tail(t *testing.T) {
	q, err := Parse(`from app | tail 5`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
	tail, ok := q.Commands[0].(*TailCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TailCommand, got %T", q.Commands[0])
	}
	if tail.Count != 5 {
		t.Errorf("Count: got %d, want 5", tail.Count)
	}
}

func TestLynxFlow_SortDashField(t *testing.T) {
	q, err := Parse(`from app | sort -count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if len(sort.Fields) != 1 {
		t.Fatalf("Fields: got %d, want 1", len(sort.Fields))
	}
	if sort.Fields[0].Name != "count" {
		t.Errorf("Name: got %q, want count", sort.Fields[0].Name)
	}
	if !sort.Fields[0].Desc {
		t.Error("expected Desc=true")
	}
}

func TestLynxFlow_BottomByAgg(t *testing.T) {
	q, err := Parse(`from app | bottom 5 uri by avg(duration_ms)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// bottom N field by agg() desugars to: stats + sort + head
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*StatsCommand); !ok {
		t.Errorf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	sort, ok := q.Commands[1].(*SortCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected SortCommand, got %T", q.Commands[1])
	}
	// bottomby → sort ascending (not desc)
	if sort.Fields[0].Desc {
		t.Error("expected sort ascending for bottomby")
	}
	head, ok := q.Commands[2].(*HeadCommand)
	if !ok {
		t.Fatalf("cmd[2]: expected HeadCommand, got %T", q.Commands[2])
	}
	if head.Count != 5 {
		t.Errorf("Count: got %d, want 5", head.Count)
	}
}

func TestLynxFlow_PercentilesNoBy(t *testing.T) {
	q, err := Parse(`from app | percentiles duration_ms`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	stats, ok := q.Commands[0].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
	}
	if len(stats.Aggregations) != 5 {
		t.Fatalf("Aggregations: got %d, want 5", len(stats.Aggregations))
	}
	if len(stats.GroupBy) != 0 {
		t.Errorf("GroupBy: got %v, want empty", stats.GroupBy)
	}
	// Verify percentile order: p50, p75, p90, p95, p99.
	expected := []string{"perc50", "perc75", "perc90", "perc95", "perc99"}
	for i, e := range expected {
		if stats.Aggregations[i].Func != e {
			t.Errorf("agg[%d]: got %q, want %q", i, stats.Aggregations[i].Func, e)
		}
	}
}

// =============================================================================
// Additional Combining Tests
// =============================================================================

func TestLynxFlow_JoinLeft(t *testing.T) {
	q, err := Parse(`from nginx | join type=left user_id [from users]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	join, ok := q.Commands[0].(*JoinCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected JoinCommand, got %T", q.Commands[0])
	}
	if join.JoinType != "left" {
		t.Errorf("JoinType: got %q, want left", join.JoinType)
	}
	if join.Field != "user_id" {
		t.Errorf("Field: got %q, want user_id", join.Field)
	}
}

func TestLynxFlow_Multisearch(t *testing.T) {
	q, err := Parse(`| multisearch [from nginx | stats count()] [from app | stats count()]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ms, ok := q.Commands[0].(*MultisearchCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected MultisearchCommand, got %T", q.Commands[0])
	}
	if len(ms.Searches) != 2 {
		t.Fatalf("Searches: got %d, want 2", len(ms.Searches))
	}
}

// =============================================================================
// Additional Window Ops Tests
// =============================================================================

func TestLynxFlow_RunningWindowCurrentBy(t *testing.T) {
	q, err := Parse(`from app | running window=5 current=false avg(x) as ra by service`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ss, ok := q.Commands[0].(*StreamstatsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected StreamstatsCommand, got %T", q.Commands[0])
	}
	if ss.Window != 5 {
		t.Errorf("Window: got %d, want 5", ss.Window)
	}
	if ss.Current != false {
		t.Error("expected Current=false")
	}
	if len(ss.GroupBy) != 1 || ss.GroupBy[0] != "service" {
		t.Errorf("GroupBy: got %v, want [service]", ss.GroupBy)
	}
	if len(ss.Aggregations) != 1 || ss.Aggregations[0].Alias != "ra" {
		t.Errorf("Aggregations: got %v", ss.Aggregations)
	}
}

// =============================================================================
// Additional Null Handling Tests
// =============================================================================

func TestLynxFlow_FillnullBare(t *testing.T) {
	q, err := Parse(`from app | fillnull`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fn, ok := q.Commands[0].(*FillnullCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected FillnullCommand, got %T", q.Commands[0])
	}
	if fn.Value != "" {
		t.Errorf("Value: got %q, want empty", fn.Value)
	}
}

func TestLynxFlow_FillnullValueZero(t *testing.T) {
	q, err := Parse(`from app | fillnull value=0`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	fn, ok := q.Commands[0].(*FillnullCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected FillnullCommand, got %T", q.Commands[0])
	}
	if fn.Value != "0" {
		t.Errorf("Value: got %q, want 0", fn.Value)
	}
}

// =============================================================================
// Additional Presentation Tests
// =============================================================================

func TestLynxFlow_TableStar(t *testing.T) {
	q, err := Parse(`from app | table *`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tbl, ok := q.Commands[0].(*TableCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected TableCommand, got %T", q.Commands[0])
	}
	if len(tbl.Fields) != 1 || tbl.Fields[0] != "*" {
		t.Errorf("Fields: got %v, want [*]", tbl.Fields)
	}
}

// =============================================================================
// Parse All Formats (Spec 02 — Remaining formats)
// =============================================================================

func TestLynxFlow_ParseAllFormats(t *testing.T) {
	formats := []string{
		"clf", "nginx_error", "cef", "kv", "docker", "redis",
		"apache_error", "postgres", "mysql_slow", "haproxy", "leef", "w3c",
	}
	for _, format := range formats {
		t.Run(format, func(t *testing.T) {
			input := "from app | parse " + format + "(_raw)"
			q, err := Parse(input)
			if err != nil {
				t.Fatalf("Parse(%s): %v", format, err)
			}
			if len(q.Commands) != 1 {
				t.Fatalf("Commands: got %d, want 1", len(q.Commands))
			}
			unpack, ok := q.Commands[0].(*UnpackCommand)
			if !ok {
				t.Fatalf("cmd[0]: expected UnpackCommand, got %T", q.Commands[0])
			}
			if unpack.Format != format {
				t.Errorf("Format: got %q, want %q", unpack.Format, format)
			}
			if unpack.SourceField != "_raw" {
				t.Errorf("SourceField: got %q, want _raw", unpack.SourceField)
			}
		})
	}
}

// =============================================================================
// Additional Field Shaping Tests
// =============================================================================

func TestLynxFlow_RenameMulti(t *testing.T) {
	q, err := Parse(`from app | rename old1 as new1, old2 as new2`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ren, ok := q.Commands[0].(*RenameCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected RenameCommand, got %T", q.Commands[0])
	}
	if len(ren.Renames) != 2 {
		t.Fatalf("Renames: got %d, want 2", len(ren.Renames))
	}
	if ren.Renames[0].Old != "old1" || ren.Renames[0].New != "new1" {
		t.Errorf("rename[0]: got %v", ren.Renames[0])
	}
	if ren.Renames[1].Old != "old2" || ren.Renames[1].New != "new2" {
		t.Errorf("rename[1]: got %v", ren.Renames[1])
	}
}

// =============================================================================
// Additional Equivalence Tests
// =============================================================================

func TestLynxFlow_Equiv_ParseJsonIsUnpackJson(t *testing.T) {
	q1, err := Parse(`from app | parse json(_raw)`)
	if err != nil {
		t.Fatalf("Parse(parse json): %v", err)
	}
	q2, err := Parse(`from app | unpack_json`)
	if err != nil {
		t.Fatalf("Parse(unpack_json): %v", err)
	}

	u1, ok := q1.Commands[0].(*UnpackCommand)
	if !ok {
		t.Fatalf("parse json: expected UnpackCommand, got %T", q1.Commands[0])
	}
	u2, ok := q2.Commands[0].(*UnpackCommand)
	if !ok {
		t.Fatalf("unpack_json: expected UnpackCommand, got %T", q2.Commands[0])
	}
	if u1.Format != u2.Format {
		t.Errorf("Format mismatch: parse json=%q, unpack_json=%q", u1.Format, u2.Format)
	}
}

func TestLynxFlow_Equiv_PackIsPackJson(t *testing.T) {
	q1, err := Parse(`from app | pack f1, f2 into payload`)
	if err != nil {
		t.Fatalf("Parse(pack): %v", err)
	}
	q2, err := Parse(`from app | pack_json f1, f2 into payload`)
	if err != nil {
		t.Fatalf("Parse(pack_json): %v", err)
	}

	p1, ok := q1.Commands[0].(*PackJsonCommand)
	if !ok {
		t.Fatalf("pack: expected PackJsonCommand, got %T", q1.Commands[0])
	}
	p2, ok := q2.Commands[0].(*PackJsonCommand)
	if !ok {
		t.Fatalf("pack_json: expected PackJsonCommand, got %T", q2.Commands[0])
	}
	if p1.Target != p2.Target {
		t.Errorf("Target mismatch: pack=%q, pack_json=%q", p1.Target, p2.Target)
	}
	if len(p1.Fields) != len(p2.Fields) {
		t.Errorf("Fields length mismatch: pack=%d, pack_json=%d", len(p1.Fields), len(p2.Fields))
	}
}

// =============================================================================
// Cross-Syntax Hint Tests (error_hints.go)
// =============================================================================

// =============================================================================
// Additional Sort Tests
// =============================================================================

func TestLynxFlow_SortMultiDashPlus(t *testing.T) {
	// SPL-style multi-field sort: sort -status, +uri
	q, err := Parse(`from app | sort -status, +uri`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sort := q.Commands[0].(*SortCommand)
	if len(sort.Fields) != 2 {
		t.Fatalf("Fields: got %d, want 2", len(sort.Fields))
	}
	if sort.Fields[0].Name != "status" || !sort.Fields[0].Desc {
		t.Errorf("Fields[0]: got %+v, want status desc", sort.Fields[0])
	}
	if sort.Fields[1].Name != "uri" || sort.Fields[1].Desc {
		t.Errorf("Fields[1]: got %+v, want uri asc", sort.Fields[1])
	}
}

// =============================================================================
// Additional Filter Tests
// =============================================================================

func TestLynxFlow_WhereNotIn(t *testing.T) {
	q, err := Parse(`from app | where status not in (200, 301)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	in, ok := where.Expr.(*InExpr)
	if !ok {
		t.Fatalf("expr: expected InExpr, got %T", where.Expr)
	}
	if !in.Negated {
		t.Error("expected Negated=true for NOT IN")
	}
	if len(in.Values) != 2 {
		t.Errorf("Values: got %d, want 2", len(in.Values))
	}
}

func TestLynxFlow_WhereNotLike(t *testing.T) {
	q, err := Parse(`from app | where uri not like "/admin/%"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expr: expected CompareExpr, got %T", where.Expr)
	}
	if cmp.Op != "not like" {
		t.Errorf("Op: got %q, want 'not like'", cmp.Op)
	}
}

// =============================================================================
// Additional Domain Sugar Tests
// =============================================================================

func TestLynxFlow_ErrorsMultiAgg(t *testing.T) {
	q, err := Parse(`from app | errors by service compute count(), dc(user_id)`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	stats := q.Commands[1].(*StatsCommand)
	if len(stats.Aggregations) != 2 {
		t.Fatalf("Aggregations: got %d, want 2", len(stats.Aggregations))
	}
	if stats.Aggregations[0].Func != "count" {
		t.Errorf("Agg[0]: got %q, want count", stats.Aggregations[0].Func)
	}
	if stats.Aggregations[1].Func != "dc" {
		t.Errorf("Agg[1]: got %q, want dc", stats.Aggregations[1].Func)
	}
}

func TestLynxFlow_RatePerOnly(t *testing.T) {
	q, err := Parse(`from app | rate per 1m`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc := q.Commands[0].(*TimechartCommand)
	if tc.Span != "1m" {
		t.Errorf("Span: got %q, want 1m", tc.Span)
	}
	if len(tc.GroupBy) != 0 {
		t.Errorf("GroupBy: got %v, want empty", tc.GroupBy)
	}
}

func TestLynxFlow_SlowestGroupDefaultDur(t *testing.T) {
	// slowest 20 uri — uses default duration_ms
	q, err := Parse(`from app | slowest 20 uri`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 3 {
		t.Fatalf("Commands: got %d, want 3 (stats + sort + head)", len(q.Commands))
	}
	stats := q.Commands[0].(*StatsCommand)
	if len(stats.GroupBy) != 1 || stats.GroupBy[0] != "uri" {
		t.Errorf("GroupBy: got %v, want [uri]", stats.GroupBy)
	}
	// Default dur field is "duration_ms".
	if len(stats.Aggregations) != 1 || stats.Aggregations[0].Func != "max" {
		t.Errorf("Aggregations: got %v", stats.Aggregations)
	}
	agg := stats.Aggregations[0]
	if len(agg.Args) == 1 {
		if f, ok := agg.Args[0].(*FieldExpr); ok {
			if f.Name != "duration_ms" {
				t.Errorf("dur field: got %q, want duration_ms", f.Name)
			}
		}
	}
	head := q.Commands[2].(*HeadCommand)
	if head.Count != 20 {
		t.Errorf("Count: got %d, want 20", head.Count)
	}
}

func TestLynxFlow_LatencyAllPercentiles(t *testing.T) {
	q, err := Parse(`from app | latency dur every 1m compute p50, p75, p90, p95, p99, avg, max`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	tc := q.Commands[0].(*TimechartCommand)
	if tc.Span != "1m" {
		t.Errorf("Span: got %q, want 1m", tc.Span)
	}
	if len(tc.Aggregations) != 7 {
		t.Fatalf("Aggregations: got %d, want 7", len(tc.Aggregations))
	}
	expectedFuncs := []string{"perc50", "perc75", "perc90", "perc95", "perc99", "avg", "max"}
	for i, want := range expectedFuncs {
		if tc.Aggregations[i].Func != want {
			t.Errorf("Agg[%d]: got %q, want %q", i, tc.Aggregations[i].Func, want)
		}
	}
}

// =============================================================================
// Additional Views Tests
// =============================================================================

func TestLynxFlow_ViewsDetail(t *testing.T) {
	q, err := Parse(`| views "mv_errors_5m"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	views, ok := q.Commands[0].(*ViewsCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected ViewsCommand, got %T", q.Commands[0])
	}
	if views.Name != "mv_errors_5m" {
		t.Errorf("Name: got %q, want mv_errors_5m", views.Name)
	}
}

func TestLynxFlow_ViewsRetention(t *testing.T) {
	q, err := Parse(`| views "mv_errors_5m" retention=180d`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	views := q.Commands[0].(*ViewsCommand)
	if views.Name != "mv_errors_5m" {
		t.Errorf("Name: got %q", views.Name)
	}
	if views.Retention != "180d" {
		t.Errorf("Retention: got %q, want 180d", views.Retention)
	}
}

// =============================================================================
// Additional Parse Modifier Tests
// =============================================================================

func TestLynxFlow_ParseRegexWithModifiers(t *testing.T) {
	// parse regex with as namespace modifier
	q, err := Parse(`from app | parse regex(_raw, "host=(?P<host>\\S+)") as net`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// regex with "as" modifier should still produce a command.
	// Note: RexCommand doesn't natively support namespace prefix — this
	// tests that the parser doesn't reject the syntax.
	if len(q.Commands) != 1 {
		t.Fatalf("Commands: got %d, want 1", len(q.Commands))
	}
}

// =============================================================================
// Additional Equivalence Tests
// =============================================================================

func TestLynxFlow_Equiv_EveryIsTimechart(t *testing.T) {
	lf, err := Parse(`from app | every 5m compute count()`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | timechart span=5m count()`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfTC := lf.Commands[0].(*TimechartCommand)
	splTC := spl.Commands[0].(*TimechartCommand)
	if lfTC.Span != splTC.Span {
		t.Errorf("Span: LF=%q SPL=%q", lfTC.Span, splTC.Span)
	}
	if len(lfTC.Aggregations) != len(splTC.Aggregations) {
		t.Errorf("Aggs: LF=%d SPL=%d", len(lfTC.Aggregations), len(splTC.Aggregations))
	}
}

func TestLynxFlow_Equiv_BucketIsBin(t *testing.T) {
	lf, err := Parse(`from app | bucket _time span=1h as hour`)
	if err != nil {
		t.Fatalf("LF Parse: %v", err)
	}
	spl, err := Parse(`from app | bin _time span=1h as hour`)
	if err != nil {
		t.Fatalf("SPL Parse: %v", err)
	}
	lfBin := lf.Commands[0].(*BinCommand)
	splBin := spl.Commands[0].(*BinCommand)
	if lfBin.Field != splBin.Field {
		t.Errorf("Field: LF=%q SPL=%q", lfBin.Field, splBin.Field)
	}
	if lfBin.Span != splBin.Span {
		t.Errorf("Span: LF=%q SPL=%q", lfBin.Span, splBin.Span)
	}
	if lfBin.Alias != splBin.Alias {
		t.Errorf("Alias: LF=%q SPL=%q", lfBin.Alias, splBin.Alias)
	}
}

// =============================================================================
// Additional Error Tests
// =============================================================================

func TestLynxFlow_Error_PackMissingInto(t *testing.T) {
	_, err := Parse(`from app | pack f1 f2`)
	if err == nil {
		t.Fatal("expected error for pack without 'into'")
	}
	if !strings.Contains(err.Error(), "into") {
		t.Errorf("expected 'into' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_EveryNoSpan(t *testing.T) {
	_, err := Parse(`from app | every compute count()`)
	if err == nil {
		t.Fatal("expected error for every without span")
	}
}

func TestLynxFlow_Error_BottombyMissingUsing(t *testing.T) {
	_, err := Parse(`from app | bottomby 10 uri`)
	if err == nil {
		t.Fatal("expected error for bottomby without 'using'")
	}
	if !strings.Contains(err.Error(), "using") {
		t.Errorf("expected 'using' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_SelectNoColumns(t *testing.T) {
	_, err := Parse(`from app | select`)
	if err == nil {
		t.Fatal("expected error for select without columns")
	}
}

// =============================================================================
// Cross-Syntax Hint Tests (error_hints.go)
// =============================================================================

func TestLynxFlow_HintComputeAsCommand(t *testing.T) {
	_, err := Parse(`from app | compute count()`)
	if err == nil {
		t.Fatal("expected error for standalone 'compute'")
	}
	hint := SuggestFix(err.Error(), nil)
	if !strings.Contains(hint, "group") {
		t.Errorf("expected hint mentioning 'group', got: %q", hint)
	}
}

func TestLynxFlow_HintUsingAsCommand(t *testing.T) {
	_, err := Parse(`from app | using avg(x)`)
	if err == nil {
		t.Fatal("expected error for standalone 'using'")
	}
	hint := SuggestFix(err.Error(), nil)
	if !strings.Contains(hint, "topby") {
		t.Errorf("expected hint mentioning 'topby', got: %q", hint)
	}
}

// =============================================================================
// Spec 01 — Source via Normalizer Round-Trip
// =============================================================================

func TestLynxFlow_NormalizerIndexSpaceSeparated(t *testing.T) {
	// "index nginx" is normalized to "FROM nginx" before parsing.
	normalized := NormalizeQuery("index nginx | stats count()")
	q, err := Parse(normalized)
	if err != nil {
		t.Fatalf("Parse(%q): %v", normalized, err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Errorf("Source: got %v, want nginx", q.Source)
	}
}

func TestLynxFlow_NormalizerIndexQuoted(t *testing.T) {
	// index="nginx" (SPL1 quoted source) is normalized to "FROM nginx".
	normalized := NormalizeQuery(`index="nginx" | stats count()`)
	q, err := Parse(normalized)
	if err != nil {
		t.Fatalf("Parse(%q): %v", normalized, err)
	}
	if q.Source == nil || q.Source.Index != "nginx" {
		t.Errorf("Source: got %v, want nginx", q.Source)
	}
}

// =============================================================================
// Error Hint Tests — Lynx Flow-specific recovery suggestions
// =============================================================================

func TestLynxFlow_HintParseFormatName(t *testing.T) {
	// Simulated error: parse without format name.
	hint := SuggestFix("spl2: parse: expected format name at position 20", nil)
	if hint == "" {
		t.Fatal("expected hint for missing parse format")
	}
	if !strings.Contains(hint, "json") || !strings.Contains(hint, "logfmt") {
		t.Errorf("expected known formats in hint, got: %q", hint)
	}
}

func TestLynxFlow_HintParseParentheses(t *testing.T) {
	// Simulated error: parse without parentheses.
	hint := SuggestFix("spl2: parse: expected '(' after format name", nil)
	if hint == "" {
		t.Fatal("expected hint for missing parentheses")
	}
	if !strings.Contains(hint, "parse") && !strings.Contains(hint, "(") {
		t.Errorf("expected parentheses suggestion, got: %q", hint)
	}
}

func TestLynxFlow_HintGroupMissingCompute(t *testing.T) {
	// The group parser produces: "spl2: group: expected 'compute' at position ..."
	hint := SuggestFix("spl2: group: expected 'compute' at position 25, got EOF \"\"", nil)
	if hint == "" {
		t.Fatal("expected hint for missing compute")
	}
	if !strings.Contains(hint, "group") || !strings.Contains(hint, "compute") {
		t.Errorf("expected 'group ... compute' suggestion, got: %q", hint)
	}
}

func TestLynxFlow_HintEveryMissingCompute(t *testing.T) {
	hint := SuggestFix("spl2: every: expected 'compute' at position 30", nil)
	if hint == "" {
		t.Fatal("expected hint for missing compute in every")
	}
	if !strings.Contains(hint, "every") || !strings.Contains(hint, "compute") {
		t.Errorf("expected 'every ... compute' suggestion, got: %q", hint)
	}
}

// =============================================================================
// Additional Error Tests — Gap completion
// =============================================================================

func TestLynxFlow_Error_GroupNeitherByNorCompute(t *testing.T) {
	// "group" alone (no by, no compute) should error.
	_, err := Parse(`from app | group`)
	if err == nil {
		t.Fatal("expected error for bare 'group' command")
	}
	if !strings.Contains(err.Error(), "compute") {
		t.Errorf("expected 'compute' in error, got: %v", err)
	}
}

func TestLynxFlow_Error_GroupByFieldNoCompute(t *testing.T) {
	// "group by host" without compute is an error.
	_, err := Parse(`from app | group by host`)
	if err == nil {
		t.Fatal("expected error for group by without compute")
	}
	if !strings.Contains(err.Error(), "compute") {
		t.Errorf("expected 'compute' in error, got: %v", err)
	}
}

// =============================================================================
// Lynx Flow Commands as Normalizer Known Commands
// =============================================================================

func TestLynxFlow_NormalizerAllLynxFlowCommands(t *testing.T) {
	// All Lynx Flow commands should be recognized by the normalizer so that
	// "let x = 1" gets normalized to "FROM main | let x = 1".
	tests := []struct {
		input string
		want  string
	}{
		{"let x = 1", "FROM main | let x = 1"},
		{"keep f1", "FROM main | keep f1"},
		{"omit f1", "FROM main | omit f1"},
		{"select f1", "FROM main | select f1"},
		{"group by f compute count()", "FROM main | group by f compute count()"},
		{"every 5m compute count()", "FROM main | every 5m compute count()"},
		{"order by f asc", "FROM main | order by f asc"},
		{"take 10", "FROM main | take 10"},
		{"views", "FROM main | views"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := NormalizeQuery(tt.input)
			if got != tt.want {
				t.Errorf("NormalizeQuery(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Spec 14 — Additional expression edge cases
// =============================================================================

func TestLynxFlow_LetStringConcat(t *testing.T) {
	// Concatenation via + on strings should parse.
	q, err := Parse(`from app | let full = first + " " + last`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval := q.Commands[0].(*EvalCommand)
	if eval.Field != "full" {
		t.Errorf("Field: got %q, want full", eval.Field)
	}
}

func TestLynxFlow_WhereComplex(t *testing.T) {
	// Complex expression: nested AND/OR with parentheses.
	q, err := Parse(`from app | where (status >= 500 or status = 0) and service = "api"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where := q.Commands[0].(*WhereCommand)
	bin, ok := where.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expr: expected BinaryExpr (and), got %T", where.Expr)
	}
	if bin.Op != "and" {
		t.Errorf("Op: got %q, want and", bin.Op)
	}
}

// =============================================================================
// Pipeline integration: domain sugar embedded in longer pipelines
// =============================================================================

func TestLynxFlow_Pipeline_LatencyInPipeline(t *testing.T) {
	input := `from nginx | where uri like "/api/%" | latency dur every 5m compute p50, p99`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// where + timechart = 2 commands
	if len(q.Commands) != 2 {
		t.Fatalf("Commands: got %d, want 2", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*WhereCommand); !ok {
		t.Errorf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	tc, ok := q.Commands[1].(*TimechartCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected TimechartCommand, got %T", q.Commands[1])
	}
	if len(tc.Aggregations) != 2 {
		t.Errorf("Aggregations: got %d, want 2", len(tc.Aggregations))
	}
}

func TestLynxFlow_Pipeline_ErrorsThenOrder(t *testing.T) {
	input := `from app | errors by service compute count() as n, dc(user_id) as uniq | order by n desc | take 3`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	// errors → [where, stats] + sort + head = 4 commands
	if len(q.Commands) != 4 {
		t.Fatalf("Commands: got %d, want 4", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*WhereCommand); !ok {
		t.Errorf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	stats, ok := q.Commands[1].(*StatsCommand)
	if !ok {
		t.Fatalf("cmd[1]: expected StatsCommand, got %T", q.Commands[1])
	}
	if len(stats.Aggregations) != 2 {
		t.Errorf("stats aggs: got %d, want 2", len(stats.Aggregations))
	}
	if _, ok := q.Commands[2].(*SortCommand); !ok {
		t.Errorf("cmd[2]: expected SortCommand, got %T", q.Commands[2])
	}
	head, ok := q.Commands[3].(*HeadCommand)
	if !ok {
		t.Fatalf("cmd[3]: expected HeadCommand, got %T", q.Commands[3])
	}
	if head.Count != 3 {
		t.Errorf("head count: got %d, want 3", head.Count)
	}
}

func TestLynxFlow_Pipeline_ParseThenGroupThenOrder(t *testing.T) {
	input := `from app | parse json(_raw) | group by service compute count() as n | order by n desc | take 10`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(q.Commands) != 4 {
		t.Fatalf("Commands: got %d, want 4", len(q.Commands))
	}
	if _, ok := q.Commands[0].(*UnpackCommand); !ok {
		t.Errorf("cmd[0]: expected UnpackCommand, got %T", q.Commands[0])
	}
	if _, ok := q.Commands[1].(*StatsCommand); !ok {
		t.Errorf("cmd[1]: expected StatsCommand, got %T", q.Commands[1])
	}
	if _, ok := q.Commands[2].(*SortCommand); !ok {
		t.Errorf("cmd[2]: expected SortCommand, got %T", q.Commands[2])
	}
	if _, ok := q.Commands[3].(*HeadCommand); !ok {
		t.Errorf("cmd[3]: expected HeadCommand, got %T", q.Commands[3])
	}
}

// =============================================================================
// Multi-field explode
// =============================================================================

func TestLynxFlow_ExplodeMultiField(t *testing.T) {
	q, err := Parse(`| explode product, price`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll, ok := q.Commands[0].(*UnrollCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected UnrollCommand, got %T", q.Commands[0])
	}
	if unroll.Field != "product" {
		t.Errorf("Field: got %q, want product", unroll.Field)
	}
	if len(unroll.ExtraFields) != 1 || unroll.ExtraFields[0] != "price" {
		t.Errorf("ExtraFields: got %v, want [price]", unroll.ExtraFields)
	}
	if unroll.Alias != "" {
		t.Errorf("Alias: got %q, want empty (multi-field disallows alias)", unroll.Alias)
	}
}

func TestLynxFlow_ExplodeMultiFieldThree(t *testing.T) {
	q, err := Parse(`| explode a, b, c`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll := q.Commands[0].(*UnrollCommand)
	if unroll.Field != "a" {
		t.Errorf("Field: got %q, want a", unroll.Field)
	}
	if len(unroll.ExtraFields) != 2 {
		t.Fatalf("ExtraFields: got %v, want [b c]", unroll.ExtraFields)
	}
	if unroll.ExtraFields[0] != "b" || unroll.ExtraFields[1] != "c" {
		t.Errorf("ExtraFields: got %v, want [b c]", unroll.ExtraFields)
	}
	all := unroll.AllFields()
	if len(all) != 3 || all[0] != "a" || all[1] != "b" || all[2] != "c" {
		t.Errorf("AllFields: got %v, want [a b c]", all)
	}
}

func TestLynxFlow_ExplodeSingleStillWorks(t *testing.T) {
	q, err := Parse(`| explode tags`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll := q.Commands[0].(*UnrollCommand)
	if unroll.Field != "tags" {
		t.Errorf("Field: got %q, want tags", unroll.Field)
	}
	if len(unroll.ExtraFields) != 0 {
		t.Errorf("ExtraFields: got %v, want empty", unroll.ExtraFields)
	}
	all := unroll.AllFields()
	if len(all) != 1 || all[0] != "tags" {
		t.Errorf("AllFields: got %v, want [tags]", all)
	}
}

func TestLynxFlow_ExplodeAliasStillWorks(t *testing.T) {
	q, err := Parse(`| explode tags as tag`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	unroll := q.Commands[0].(*UnrollCommand)
	if unroll.Field != "tags" || unroll.Alias != "tag" {
		t.Errorf("Field=%q, Alias=%q, want tags/tag", unroll.Field, unroll.Alias)
	}
	if len(unroll.ExtraFields) != 0 {
		t.Errorf("ExtraFields: got %v, want empty", unroll.ExtraFields)
	}
}

func TestLynxFlow_ExplodeMultiFieldString(t *testing.T) {
	cmd := &UnrollCommand{Field: "product", ExtraFields: []string{"price", "qty"}}
	s := cmd.String()
	if !strings.Contains(s, "product") || !strings.Contains(s, "price") || !strings.Contains(s, "qty") {
		t.Errorf("String() = %q, should contain all fields", s)
	}
}
