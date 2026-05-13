package spl2

import (
	"reflect"
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

func TestParse_UnsupportedSplunkCommands(t *testing.T) {
	tests := []string{"delete", "collect", "stash", "sendemail", "sendalert", "localop", "redistribute", "loadjob", "savedsearch", "spl1"}
	for _, cmd := range tests {
		t.Run(cmd, func(t *testing.T) {
			_, err := Parse(`FROM main | ` + cmd)
			if err == nil {
				t.Fatalf("Parse(%q): expected error", cmd)
			}
			msg := err.Error()
			for _, want := range []string{LintUnsupportedCommand, "unsupported command", cmd} {
				if !strings.Contains(msg, want) {
					t.Fatalf("error %q missing %q", msg, want)
				}
			}
		})
	}
}

func TestParse_UnsupportedCommandNameCanBeField(t *testing.T) {
	q, err := Parse(`FROM main | delete=1`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	where, ok := q.Commands[0].(*WhereCommand)
	if !ok {
		t.Fatalf("cmd[0]: expected WhereCommand, got %T", q.Commands[0])
	}
	cmp, ok := where.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("where expr: expected CompareExpr, got %T", where.Expr)
	}
	if got := cmp.Left.(*FieldExpr).Name; got != "delete" {
		t.Errorf("field: got %q, want delete", got)
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

func TestParse_DedupCompatibilityForms(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		fields []string
		limit  int
	}{
		{
			name:   "space separated fields",
			input:  `FROM main | dedup host source`,
			fields: []string{"host", "source"},
		},
		{
			name:   "leading limit with space separated fields",
			input:  `FROM main | dedup 2 host source`,
			fields: []string{"host", "source"},
			limit:  2,
		},
		{
			name:   "trailing limit",
			input:  `FROM main | dedup host source 2`,
			fields: []string{"host", "source"},
			limit:  2,
		},
		{
			name:   "canonical comma fields",
			input:  `FROM main | dedup 2 host, source`,
			fields: []string{"host", "source"},
			limit:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			dedup, ok := q.Commands[0].(*DedupCommand)
			if !ok {
				t.Fatalf("expected DedupCommand, got %T", q.Commands[0])
			}
			if dedup.Limit != tt.limit {
				t.Fatalf("Limit: got %d, want %d", dedup.Limit, tt.limit)
			}
			if len(dedup.Fields) != len(tt.fields) {
				t.Fatalf("fields: got %v, want %v", dedup.Fields, tt.fields)
			}
			for i, want := range tt.fields {
				if dedup.Fields[i] != want {
					t.Fatalf("fields[%d]: got %q, want %q", i, dedup.Fields[i], want)
				}
			}
		})
	}
}

func TestParse_DoubleQuotedLegacyFieldLists(t *testing.T) {
	tests := []struct {
		name  string
		input string
		check func(*testing.T, *Query)
	}{
		{
			name:  "fields",
			input: `FROM main | fields "user id", status`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*FieldsCommand)
				if got, want := cmd.Fields, []string{"user id", "status"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("fields: got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "table",
			input: `FROM main | table _time, "user id"`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*TableCommand)
				if got, want := cmd.Fields, []string{"_time", "user id"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("table fields: got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "dedup",
			input: `FROM main | dedup 2 "user id", host`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*DedupCommand)
				if got, want := cmd.Fields, []string{"user id", "host"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("dedup fields: got %v, want %v", got, want)
				}
				if cmd.Limit != 2 {
					t.Fatalf("dedup limit: got %d, want 2", cmd.Limit)
				}
			},
		},
		{
			name:  "stats by",
			input: `FROM main | stats count() by "user id", host`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*StatsCommand)
				if got, want := cmd.GroupBy, []string{"user id", "host"}; !reflect.DeepEqual(got, want) {
					t.Fatalf("stats group by: got %v, want %v", got, want)
				}
			},
		},
		{
			name:  "stats alias",
			input: `FROM main | stats count() as "total count"`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*StatsCommand)
				if got, want := cmd.Aggregations[0].Alias, "total count"; got != want {
					t.Fatalf("stats alias: got %q, want %q", got, want)
				}
			},
		},
		{
			name:  "eval target",
			input: `FROM main | eval "display name"=status`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*EvalCommand)
				if got, want := cmd.Field, "display name"; got != want {
					t.Fatalf("eval field: got %q, want %q", got, want)
				}
			},
		},
		{
			name:  "rename pair",
			input: `FROM main | rename "old name" as "new name"`,
			check: func(t *testing.T, q *Query) {
				cmd := q.Commands[0].(*RenameCommand)
				if len(cmd.Renames) != 1 || cmd.Renames[0].Old != "old name" || cmd.Renames[0].New != "new name" {
					t.Fatalf("rename pairs: got %+v", cmd.Renames)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			tt.check(t, q)
		})
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

func TestParse_ReverseCommand(t *testing.T) {
	input := `FROM main | reverse`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if _, ok := q.Commands[0].(*ReverseCommand); !ok {
		t.Fatalf("expected ReverseCommand, got %T", q.Commands[0])
	}
}

func TestParse_RegexCommandDefaultRaw(t *testing.T) {
	q, err := Parse(`FROM main | regex "error|fatal"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cmd, ok := q.Commands[0].(*RegexCommand)
	if !ok {
		t.Fatalf("expected RegexCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "_raw" {
		t.Errorf("field: got %q, want _raw", cmd.Field)
	}
	if cmd.Pattern != "error|fatal" {
		t.Errorf("pattern: got %q", cmd.Pattern)
	}
	if cmd.Negate {
		t.Error("negate: got true, want false")
	}
}

func TestParse_RegexCommandFieldNotMatch(t *testing.T) {
	q, err := Parse(`FROM main | regex message!="^debug"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cmd, ok := q.Commands[0].(*RegexCommand)
	if !ok {
		t.Fatalf("expected RegexCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "message" {
		t.Errorf("field: got %q, want message", cmd.Field)
	}
	if cmd.Pattern != "^debug" {
		t.Errorf("pattern: got %q", cmd.Pattern)
	}
	if !cmd.Negate {
		t.Error("negate: got false, want true")
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

func TestParse_XorExpr(t *testing.T) {
	input := `FROM main | where host = "web-01" or status >= 500 xor level = "ERROR"`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	where := q.Commands[0].(*WhereCommand)
	binExpr, ok := where.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected BinaryExpr, got %T", where.Expr)
	}
	if binExpr.Op != "xor" {
		t.Errorf("top-level op: got %q, want xor", binExpr.Op)
	}
	left, ok := binExpr.Left.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected OR on left side, got %T", binExpr.Left)
	}
	if left.Op != "or" {
		t.Errorf("left op: got %q, want or", left.Op)
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
		{"FROM single quoted name", `FROM 'my logs' | stats count`, "my logs"},
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

func TestParse_SourceTimeRangeSignedDurations(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantRelative string
		wantEnd      string
		wantSnapTo   string
	}{
		{
			name:         "positive duration",
			input:        "FROM jobs[+30m] | head 1",
			wantRelative: "+30m",
		},
		{
			name:         "signed range",
			input:        "FROM jobs[-1h..+30m] | head 1",
			wantRelative: "-1h",
			wantEnd:      "+30m",
		},
		{
			name:         "range ending now",
			input:        "FROM jobs[-5m..now] | head 1",
			wantRelative: "-5m",
			wantEnd:      "now",
		},
		{
			name:         "duration snap suffix",
			input:        "FROM jobs[-1d@d] | head 1",
			wantRelative: "-1d@d",
			wantSnapTo:   "d",
		},
		{
			name:       "week snap variant",
			input:      "FROM jobs[@w1] | head 1",
			wantSnapTo: "w1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if q.Source == nil || q.Source.TimeRange == nil {
				t.Fatalf("missing source time range")
			}
			if q.Source.TimeRange.Relative != tt.wantRelative {
				t.Fatalf("Relative: got %q, want %q", q.Source.TimeRange.Relative, tt.wantRelative)
			}
			if q.Source.TimeRange.End != tt.wantEnd {
				t.Fatalf("End: got %q, want %q", q.Source.TimeRange.End, tt.wantEnd)
			}
			if q.Source.TimeRange.SnapTo != tt.wantSnapTo {
				t.Fatalf("SnapTo: got %q, want %q", q.Source.TimeRange.SnapTo, tt.wantSnapTo)
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

func TestParse_FromExcludeGlob(t *testing.T) {
	input := `FROM nginx,logs*,!logs-debug* | stats count by source`
	q, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil {
		t.Fatal("expected source clause")
	}
	if !q.Source.IsGlob {
		t.Fatal("expected IsGlob=true")
	}
	if q.Source.Index != "nginx" {
		t.Fatalf("Index: got %q, want nginx", q.Source.Index)
	}
	if got, want := q.Source.Indices, []string{"nginx"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Indices: got %v, want %v", got, want)
	}
	if got, want := q.Source.IncludeGlobs, []string{"logs*"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("IncludeGlobs: got %v, want %v", got, want)
	}
	if got, want := q.Source.ExcludeGlobs, []string{"logs-debug*"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ExcludeGlobs: got %v, want %v", got, want)
	}
}

func TestParse_FromBareAdvancedGlobs(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`FROM logs-[ab]* | stats count`, "logs-[ab]*"},
		{`FROM {api,web} | stats count`, "{api,web}"},
		{`FROM api/** | stats count`, "api/**"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if q.Source == nil || !q.Source.IsGlob {
				t.Fatalf("expected glob source, got %#v", q.Source)
			}
			if q.Source.Index != tt.want {
				t.Fatalf("Index: got %q, want %q", q.Source.Index, tt.want)
			}
			if got, want := q.Source.IncludeGlobs, []string{tt.want}; !reflect.DeepEqual(got, want) {
				t.Fatalf("IncludeGlobs: got %v, want %v", got, want)
			}
		})
	}
}

func TestParse_FromTimeRangeAfterName(t *testing.T) {
	q, err := Parse(`FROM nginx[-1h] | stats count`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if q.Source == nil || q.Source.TimeRange == nil {
		t.Fatalf("expected source time range, got %#v", q.Source)
	}
	if q.Source.Index != "nginx" || q.Source.TimeRange.Relative != "-1h" {
		t.Fatalf("source/time range: got %#v", q.Source)
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

func TestParse_EvalReplaceFunction(t *testing.T) {
	q, err := Parse(`FROM main | eval clean=replace(source, "old-", "new-")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	eval, ok := q.Commands[0].(*EvalCommand)
	if !ok {
		t.Fatalf("cmd[0]: got %T, want *EvalCommand", q.Commands[0])
	}
	call, ok := eval.Assignments[0].Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: got %T, want *FuncCallExpr", eval.Assignments[0].Expr)
	}
	if call.Name != "replace" {
		t.Fatalf("function: got %q, want replace", call.Name)
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

func TestFuncCallExprStringFormatsArguments(t *testing.T) {
	expr := &FuncCallExpr{
		Name: "cidrmatch",
		Args: []Expr{
			&LiteralExpr{Value: `"10.0.0.0/8"`},
			&FieldExpr{Name: "SourceIP"},
		},
	}
	if got, want := expr.String(), `cidrmatch("10.0.0.0/8", SourceIP)`; got != want {
		t.Fatalf("String(): got %q, want %q", got, want)
	}

	q, err := Parse(`FROM main | search * | where ` + expr.String())
	if err != nil {
		t.Fatalf("Parse rendered function call: %v", err)
	}
	where := q.Commands[1].(*WhereCommand)
	if got := where.String(); got != `where cidrmatch("10.0.0.0/8", SourceIP)` {
		t.Fatalf("where String(): got %q", got)
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
		{"len", `FROM main | len(message) > 0`, ""}, // len() > 0 is a compare expr, not bare func
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
		input     string
		wantFunc  string
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

func TestParse_StatsAggregateAliases(t *testing.T) {
	tests := []struct {
		input    string
		wantFunc string
	}{
		{`| stats mean(duration) as mean_duration`, "avg"},
		{`| stats median(duration) as median_duration`, "perc50"},
		{`| stats distinct_count(user) as users`, "dc"},
		{`| stats estdc(user) as estimated_users`, "dc"},
		{`| stats estdc_error(user) as estimated_error`, "estdc_error"},
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
			if got := stats.Aggregations[0].Func; got != tt.wantFunc {
				t.Errorf("Func: got %q, want %q", got, tt.wantFunc)
			}
		})
	}
}

func TestParse_StatsPercentileSuffixAliases(t *testing.T) {
	tests := []struct {
		input    string
		wantFunc string
	}{
		{`| stats percentile95(duration) as p95`, "perc95"},
		{`| stats exactperc95(duration) as p95`, "perc95"},
		{`| stats upperperc95(duration) as p95`, "perc95"},
		{`| stats percentile25(duration) as p25`, "perc25"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.input, err)
			}
			stats, ok := q.Commands[0].(*StatsCommand)
			if !ok {
				t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
			}
			if got := stats.Aggregations[0].Func; got != tt.wantFunc {
				t.Errorf("Func: got %q, want %q", got, tt.wantFunc)
			}
		})
	}
}

func TestParse_StatsGenericPercentileAliases(t *testing.T) {
	tests := []struct {
		input    string
		wantFunc string
	}{
		{`| stats perc(duration, 95) as p95`, "perc95"},
		{`| stats percentile(duration, 95) as p95`, "perc95"},
		{`| stats exactperc(duration, 95) as p95`, "perc95"},
		{`| stats upperperc(duration, 95) as p95`, "perc95"},
		{`| stats percentile(duration, 25) as p25`, "perc25"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			q, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tt.input, err)
			}
			stats, ok := q.Commands[0].(*StatsCommand)
			if !ok {
				t.Fatalf("cmd[0]: expected StatsCommand, got %T", q.Commands[0])
			}
			agg := stats.Aggregations[0]
			if agg.Func != tt.wantFunc {
				t.Errorf("Func: got %q, want %q", agg.Func, tt.wantFunc)
			}
			if len(agg.Args) != 1 {
				t.Fatalf("Args: got %d, want 1", len(agg.Args))
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

func TestParse_MvexpandCommand(t *testing.T) {
	q, err := Parse(`FROM main | mvexpand tags`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*UnrollCommand)
	if !ok {
		t.Fatalf("expected UnrollCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "tags" {
		t.Errorf("field: got %q, want tags", cmd.Field)
	}
	if cmd.Limit != 0 {
		t.Errorf("limit: got %d, want 0", cmd.Limit)
	}
}

func TestParse_MvexpandLimitBeforeField(t *testing.T) {
	q, err := Parse(`FROM main | mvexpand limit=2 tags`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnrollCommand)
	if cmd.Field != "tags" {
		t.Errorf("field: got %q, want tags", cmd.Field)
	}
	if cmd.Limit != 2 {
		t.Errorf("limit: got %d, want 2", cmd.Limit)
	}
}

func TestParse_MvexpandLimitAfterField(t *testing.T) {
	q, err := Parse(`FROM main | mvexpand tags limit=3`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd := q.Commands[0].(*UnrollCommand)
	if cmd.Field != "tags" {
		t.Errorf("field: got %q, want tags", cmd.Field)
	}
	if cmd.Limit != 3 {
		t.Errorf("limit: got %d, want 3", cmd.Limit)
	}
}

func TestParse_ExpandCommand(t *testing.T) {
	q, err := Parse(`FROM main | expand records`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*UnrollCommand)
	if !ok {
		t.Fatalf("expected UnrollCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "records" {
		t.Errorf("field: got %q, want records", cmd.Field)
	}
}

func TestParse_MakeresultsCommand(t *testing.T) {
	tests := []struct {
		name         string
		query        string
		want         int
		wantAnnotate bool
		wantServer   string
		wantFormat   string
		wantData     string
	}{
		{name: "default", query: `| makeresults`, want: 1},
		{name: "count option", query: `| makeresults count=3`, want: 3},
		{name: "positional count", query: `| makeresults 4`, want: 4},
		{name: "annotate option", query: `| makeresults count=2 annotate=true splunk_server=local`, want: 2, wantAnnotate: true, wantServer: "local"},
		{name: "inline data options", query: `| makeresults format=json data="[{\"name\":\"Ada\"}]"`, want: 1, wantFormat: "json", wantData: `[{"name":"Ada"}]`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			cmd, ok := q.Commands[0].(*MakeresultsCommand)
			if !ok {
				t.Fatalf("expected MakeresultsCommand, got %T", q.Commands[0])
			}
			if cmd.Count != tc.want {
				t.Errorf("count: got %d, want %d", cmd.Count, tc.want)
			}
			if cmd.Annotate != tc.wantAnnotate {
				t.Errorf("annotate: got %v, want %v", cmd.Annotate, tc.wantAnnotate)
			}
			if cmd.SplunkServer != tc.wantServer {
				t.Errorf("splunk_server: got %q, want %q", cmd.SplunkServer, tc.wantServer)
			}
			if cmd.Format != tc.wantFormat {
				t.Errorf("format: got %q, want %q", cmd.Format, tc.wantFormat)
			}
			if cmd.Data != tc.wantData {
				t.Errorf("data: got %q, want %q", cmd.Data, tc.wantData)
			}
		})
	}
}

func TestParse_UntableCommand(t *testing.T) {
	q, err := Parse(`FROM main | untable host metric value`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*UntableCommand)
	if !ok {
		t.Fatalf("expected UntableCommand, got %T", q.Commands[0])
	}
	if cmd.XField != "host" {
		t.Errorf("x field: got %q, want host", cmd.XField)
	}
	if cmd.YNameField != "metric" {
		t.Errorf("y name field: got %q, want metric", cmd.YNameField)
	}
	if cmd.YDataField != "value" {
		t.Errorf("y data field: got %q, want value", cmd.YDataField)
	}
}

func TestParse_NomvCommand(t *testing.T) {
	q, err := Parse(`FROM main | nomv senders`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*NomvCommand)
	if !ok {
		t.Fatalf("expected NomvCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "senders" {
		t.Errorf("field: got %q, want senders", cmd.Field)
	}
}

func TestParse_MakemvCommand(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantField string
		wantDelim string
		wantTok   string
		wantEmpty bool
		wantSetSV bool
	}{
		{name: "default", query: `FROM main | makemv tags`, wantField: "tags", wantDelim: " "},
		{name: "delim", query: `FROM main | makemv delim="," tags`, wantField: "tags", wantDelim: ","},
		{name: "options after field", query: `FROM main | makemv tags delim="|" allowempty=true`, wantField: "tags", wantDelim: "|", wantEmpty: true},
		{name: "tokenizer", query: `FROM main | makemv tokenizer="([^,]+),?" setsv=t tags`, wantField: "tags", wantDelim: " ", wantTok: "([^,]+),?", wantSetSV: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			cmd, ok := q.Commands[0].(*MakemvCommand)
			if !ok {
				t.Fatalf("expected MakemvCommand, got %T", q.Commands[0])
			}
			if cmd.Field != tc.wantField {
				t.Errorf("field: got %q, want %q", cmd.Field, tc.wantField)
			}
			if cmd.Delim != tc.wantDelim {
				t.Errorf("delim: got %q, want %q", cmd.Delim, tc.wantDelim)
			}
			if cmd.Tokenizer != tc.wantTok {
				t.Errorf("tokenizer: got %q, want %q", cmd.Tokenizer, tc.wantTok)
			}
			if cmd.AllowEmpty != tc.wantEmpty {
				t.Errorf("allowempty: got %v, want %v", cmd.AllowEmpty, tc.wantEmpty)
			}
			if cmd.SetSV != tc.wantSetSV {
				t.Errorf("setsv: got %v, want %v", cmd.SetSV, tc.wantSetSV)
			}
		})
	}
}

func TestParse_MvcombineCommand(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantField string
		wantDelim string
	}{
		{name: "default", query: `FROM main | mvcombine host`, wantField: "host", wantDelim: " "},
		{name: "delim", query: `FROM main | mvcombine delim="," host`, wantField: "host", wantDelim: ","},
		{name: "delim after field", query: `FROM main | mvcombine host delim=":"`, wantField: "host", wantDelim: ":"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			cmd, ok := q.Commands[0].(*MvcombineCommand)
			if !ok {
				t.Fatalf("expected MvcombineCommand, got %T", q.Commands[0])
			}
			if cmd.Field != tc.wantField {
				t.Errorf("field: got %q, want %q", cmd.Field, tc.wantField)
			}
			if cmd.Delim != tc.wantDelim {
				t.Errorf("delim: got %q, want %q", cmd.Delim, tc.wantDelim)
			}
		})
	}
}

func TestParse_ReplaceCommand(t *testing.T) {
	q, err := Parse(`FROM main | replace 0 WITH Critical, "* localhost" WITH "localhost *" IN msg_level host`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*ReplaceCommand)
	if !ok {
		t.Fatalf("expected ReplaceCommand, got %T", q.Commands[0])
	}
	if len(cmd.Pairs) != 2 {
		t.Fatalf("pairs: got %d, want 2", len(cmd.Pairs))
	}
	if cmd.Pairs[0] != (ReplacePair{Old: "0", New: "Critical"}) {
		t.Errorf("pair[0]: got %+v", cmd.Pairs[0])
	}
	if cmd.Pairs[1] != (ReplacePair{Old: "* localhost", New: "localhost *"}) {
		t.Errorf("pair[1]: got %+v", cmd.Pairs[1])
	}
	if len(cmd.Fields) != 2 || cmd.Fields[0] != "msg_level" || cmd.Fields[1] != "host" {
		t.Errorf("fields: got %v, want [msg_level host]", cmd.Fields)
	}
}

func TestParse_FieldformatCommand(t *testing.T) {
	q, err := Parse(`FROM main | fieldformat totalCount=tostring(totalCount, "commas")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*FieldformatCommand)
	if !ok {
		t.Fatalf("expected FieldformatCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "totalCount" {
		t.Errorf("field: got %q, want totalCount", cmd.Field)
	}
	call, ok := cmd.Expr.(*FuncCallExpr)
	if !ok {
		t.Fatalf("expr: got %T, want FuncCallExpr", cmd.Expr)
	}
	if call.Name != "tostring" || len(call.Args) != 2 {
		t.Fatalf("call: got %s with %d args, want tostring with 2 args", call.Name, len(call.Args))
	}
}

func TestParse_FieldformatQuotedField(t *testing.T) {
	q, err := Parse(`FROM main | fieldformat "First Event"=strftime(firstTime, "%c")`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*FieldformatCommand)
	if !ok {
		t.Fatalf("expected FieldformatCommand, got %T", q.Commands[0])
	}
	if cmd.Field != "First Event" {
		t.Errorf("field: got %q, want %q", cmd.Field, "First Event")
	}
}

func TestParse_CapabilityCommands(t *testing.T) {
	tests := []struct {
		query string
		name  string
		args  int
	}{
		{`FROM main | addinfo`, "addinfo", 0},
		{`FROM main | convert timeformat="%Y-%m-%d" ctime(_time)`, "convert", 7},
		{`FROM main | fieldsummary maxvals=10`, "fieldsummary", 3},
		{`FROM main | flatten payload`, "flatten", 1},
		{`FROM main | iplocation ip`, "iplocation", 1},
		{`FROM main | tags host`, "tags", 1},
		{`FROM main | typer`, "typer", 0},
		{`FROM main | thru audit`, "thru", 1},
		{`FROM main | timewrap 1w`, "timewrap", 2},
		{`FROM main | tstats count where index=main by host`, "tstats", 7},
		{`FROM main | mstats avg(cpu) where index=metrics by host`, "mstats", 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, err := Parse(tc.query)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			cmd, ok := q.Commands[0].(*CapabilityCommand)
			if !ok {
				t.Fatalf("expected CapabilityCommand, got %T", q.Commands[0])
			}
			if cmd.Name != tc.name {
				t.Errorf("name: got %q, want %q", cmd.Name, tc.name)
			}
			if len(cmd.Args) != tc.args {
				t.Errorf("args: got %d (%v), want %d", len(cmd.Args), cmd.Args, tc.args)
			}
		})
	}
}

func TestParse_CapabilityCommandNameCanBeBareFilterField(t *testing.T) {
	q, err := Parse(`FROM main | tags="prod"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := q.Commands[0].(*WhereCommand); !ok {
		t.Fatalf("expected WhereCommand, got %T", q.Commands[0])
	}
}

func TestParse_ChartCommand(t *testing.T) {
	q, err := Parse(`FROM main | chart count over host by status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*ChartCommand)
	if !ok {
		t.Fatalf("expected ChartCommand, got %T", q.Commands[0])
	}
	if len(cmd.Aggregations) != 1 || cmd.Aggregations[0].Func != "count" {
		t.Fatalf("aggs: got %+v, want count", cmd.Aggregations)
	}
	if cmd.RowSplit != "host" || cmd.ColumnSplit != "status" {
		t.Errorf("splits: got row=%q column=%q, want host/status", cmd.RowSplit, cmd.ColumnSplit)
	}
}

func TestParse_ChartCommandByTwoFields(t *testing.T) {
	q, err := Parse(`FROM main | chart avg(duration_ms) by host,status`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*ChartCommand)
	if !ok {
		t.Fatalf("expected ChartCommand, got %T", q.Commands[0])
	}
	if cmd.RowSplit != "host" || cmd.ColumnSplit != "status" {
		t.Errorf("splits: got row=%q column=%q, want host/status", cmd.RowSplit, cmd.ColumnSplit)
	}
}

func TestParse_UnionCommand(t *testing.T) {
	q, err := Parse(`FROM main | union maxout=1000 maxtime=120 timeout=600 customers, orders [search error | stats count by source]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*UnionCommand)
	if !ok {
		t.Fatalf("expected UnionCommand, got %T", q.Commands[0])
	}
	if len(cmd.Branches) != 3 {
		t.Fatalf("branches: got %d, want 3", len(cmd.Branches))
	}
	if cmd.Maxout != 1000 {
		t.Fatalf("maxout: got %d, want 1000", cmd.Maxout)
	}
	if cmd.Maxtime != 120 {
		t.Fatalf("maxtime: got %d, want 120", cmd.Maxtime)
	}
	if cmd.Timeout != 600 {
		t.Fatalf("timeout: got %d, want 600", cmd.Timeout)
	}
	if cmd.Branches[0].Source == nil || cmd.Branches[0].Source.Index != "customers" {
		t.Fatalf("branch[0] source: got %+v, want customers", cmd.Branches[0].Source)
	}
	if cmd.Branches[1].Source == nil || cmd.Branches[1].Source.Index != "orders" {
		t.Fatalf("branch[1] source: got %+v, want orders", cmd.Branches[1].Source)
	}
	if len(cmd.Branches[2].Commands) != 2 {
		t.Fatalf("branch[2] commands: got %d, want 2", len(cmd.Branches[2].Commands))
	}
}

func TestParse_AppendpipeCommand(t *testing.T) {
	q, err := Parse(`FROM main | appendpipe run_in_preview=false [stats count as total]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*AppendpipeCommand)
	if !ok {
		t.Fatalf("expected AppendpipeCommand, got %T", q.Commands[0])
	}
	if cmd.RunInPreview {
		t.Fatal("run_in_preview: got true, want false")
	}
	if cmd.Subquery == nil || len(cmd.Subquery.Commands) != 1 {
		t.Fatalf("subquery commands: got %+v, want one command", cmd.Subquery)
	}
	if _, ok := cmd.Subquery.Commands[0].(*StatsCommand); !ok {
		t.Fatalf("subquery command: got %T, want StatsCommand", cmd.Subquery.Commands[0])
	}
}

func TestParse_AppendpipeDefaultRunInPreview(t *testing.T) {
	q, err := Parse(`FROM main | appendpipe [stats count as total]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*AppendpipeCommand)
	if !ok {
		t.Fatalf("expected AppendpipeCommand, got %T", q.Commands[0])
	}
	if !cmd.RunInPreview {
		t.Fatal("run_in_preview: got false, want true")
	}
}

func TestParse_AppendcolsCommand(t *testing.T) {
	q, err := Parse(`FROM main | appendcols override=true maxout=100 maxtime=30 timeout=45 [stats count as total]`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cmd, ok := q.Commands[0].(*AppendcolsCommand)
	if !ok {
		t.Fatalf("expected AppendcolsCommand, got %T", q.Commands[0])
	}
	if !cmd.Override {
		t.Fatal("override: got false, want true")
	}
	if cmd.Maxout != 100 {
		t.Fatalf("maxout: got %d, want 100", cmd.Maxout)
	}
	if cmd.Maxtime != 30 {
		t.Fatalf("maxtime: got %d, want 30", cmd.Maxtime)
	}
	if cmd.Timeout != 45 {
		t.Fatalf("timeout: got %d, want 45", cmd.Timeout)
	}
	if cmd.Subquery == nil || len(cmd.Subquery.Commands) != 1 {
		t.Fatalf("subquery commands: got %+v, want one command", cmd.Subquery)
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

func TestParse_OptionalChaining(t *testing.T) {
	t.Run("where with optional chaining", func(t *testing.T) {
		q, err := Parse(`FROM main | where event?.user = "admin"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		where, ok := q.Commands[0].(*WhereCommand)
		if !ok {
			t.Fatalf("expected WhereCommand, got %T", q.Commands[0])
		}
		cmp, ok := where.Expr.(*CompareExpr)
		if !ok {
			t.Fatalf("expected CompareExpr, got %T", where.Expr)
		}
		fe, ok := cmp.Left.(*FieldExpr)
		if !ok {
			t.Fatalf("expected FieldExpr, got %T", cmp.Left)
		}
		if fe.Name != "event.user" {
			t.Errorf("field name: got %q, want %q", fe.Name, "event.user")
		}
		if !fe.Optional {
			t.Error("expected Optional=true")
		}
	})

	t.Run("eval with optional chaining", func(t *testing.T) {
		q, err := Parse(`FROM main | eval name = user?.profile?.name`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval, ok := q.Commands[0].(*EvalCommand)
		if !ok {
			t.Fatalf("expected EvalCommand, got %T", q.Commands[0])
		}
		fe, ok := eval.Expr.(*FieldExpr)
		if !ok {
			t.Fatalf("expected FieldExpr for eval expr, got %T", eval.Expr)
		}
		if fe.Name != "user.profile" {
			t.Errorf("field name: got %q, want %q", fe.Name, "user.profile")
		}
		if !fe.Optional {
			t.Error("expected Optional=true")
		}
	})

	t.Run("bare question mark is isnotnull", func(t *testing.T) {
		q, err := Parse(`FROM main | where event?`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		where, ok := q.Commands[0].(*WhereCommand)
		if !ok {
			t.Fatalf("expected WhereCommand, got %T", q.Commands[0])
		}
		fn, ok := where.Expr.(*FuncCallExpr)
		if !ok {
			t.Fatalf("expected FuncCallExpr, got %T", where.Expr)
		}
		if fn.Name != "isnotnull" {
			t.Errorf("function name: got %q, want %q", fn.Name, "isnotnull")
		}
	})
}

func TestParse_Trace(t *testing.T) {
	t.Run("default fields", func(t *testing.T) {
		q, err := Parse(`FROM main | trace`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		cmd, ok := q.Commands[0].(*TraceCommand)
		if !ok {
			t.Fatalf("expected TraceCommand, got %T", q.Commands[0])
		}
		if cmd.TraceIDField != "trace_id" {
			t.Errorf("TraceIDField: got %q, want %q", cmd.TraceIDField, "trace_id")
		}
		if cmd.SpanIDField != "span_id" {
			t.Errorf("SpanIDField: got %q, want %q", cmd.SpanIDField, "span_id")
		}
		if cmd.ParentIDField != "parent_span_id" {
			t.Errorf("ParentIDField: got %q, want %q", cmd.ParentIDField, "parent_span_id")
		}
	})

	t.Run("custom fields", func(t *testing.T) {
		q, err := Parse(`FROM main | trace trace_id=tid span_id=sid parent_id=pid`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		cmd, ok := q.Commands[0].(*TraceCommand)
		if !ok {
			t.Fatalf("expected TraceCommand, got %T", q.Commands[0])
		}
		if cmd.TraceIDField != "tid" {
			t.Errorf("TraceIDField: got %q, want %q", cmd.TraceIDField, "tid")
		}
		if cmd.SpanIDField != "sid" {
			t.Errorf("SpanIDField: got %q, want %q", cmd.SpanIDField, "sid")
		}
		if cmd.ParentIDField != "pid" {
			t.Errorf("ParentIDField: got %q, want %q", cmd.ParentIDField, "pid")
		}
	})
}

func TestParse_FString(t *testing.T) {
	t.Run("simple interpolation", func(t *testing.T) {
		q, err := Parse(`| eval msg = f"{status}: {uri}"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval, ok := q.Commands[0].(*EvalCommand)
		if !ok {
			t.Fatalf("expected EvalCommand, got %T", q.Commands[0])
		}
		fstr, ok := eval.Expr.(*FStringExpr)
		if !ok {
			t.Fatalf("expected FStringExpr, got %T", eval.Expr)
		}
		if len(fstr.Parts) != 3 {
			t.Fatalf("parts count: got %d, want 3", len(fstr.Parts))
		}
		if fstr.Parts[0].Literal != "" || fstr.Parts[0].Expr != "status" {
			t.Errorf("part 0: got %+v, want field=status", fstr.Parts[0])
		}
		if fstr.Parts[1].Literal != ": " {
			t.Errorf("part 1: got %q, want \": \"", fstr.Parts[1].Literal)
		}
		if fstr.Parts[2].Literal != "" || fstr.Parts[2].Expr != "uri" {
			t.Errorf("part 2: got %+v, want field=uri", fstr.Parts[2])
		}
	})

	t.Run("literal only", func(t *testing.T) {
		q, err := Parse(`| eval msg = f"hello world"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval := q.Commands[0].(*EvalCommand)
		fstr := eval.Expr.(*FStringExpr)
		if len(fstr.Parts) != 1 {
			t.Fatalf("parts count: got %d, want 1", len(fstr.Parts))
		}
		if fstr.Parts[0].Literal != "hello world" {
			t.Errorf("literal: got %q", fstr.Parts[0].Literal)
		}
	})

	t.Run("empty f-string", func(t *testing.T) {
		q, err := Parse(`| eval msg = f""`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval := q.Commands[0].(*EvalCommand)
		fstr := eval.Expr.(*FStringExpr)
		if len(fstr.Parts) != 0 {
			t.Fatalf("parts count: got %d, want 0", len(fstr.Parts))
		}
	})

	t.Run("escaped braces", func(t *testing.T) {
		q, err := Parse(`| eval msg = f"{{literal}}"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval := q.Commands[0].(*EvalCommand)
		fstr := eval.Expr.(*FStringExpr)
		if len(fstr.Parts) != 1 {
			t.Fatalf("parts count: got %d, want 1", len(fstr.Parts))
		}
		if fstr.Parts[0].Literal != "{literal}" {
			t.Errorf("literal: got %q, want \"{literal}\"", fstr.Parts[0].Literal)
		}
	})

	t.Run("f-string not confused with ident", func(t *testing.T) {
		// "from" is a keyword, not an f-string prefix
		q, err := Parse(`| eval msg = f"test"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval := q.Commands[0].(*EvalCommand)
		if _, ok := eval.Expr.(*FStringExpr); !ok {
			t.Fatalf("expected FStringExpr, got %T", eval.Expr)
		}
	})

	t.Run("field is keyword", func(t *testing.T) {
		q, err := Parse(`| eval msg = f"from={from}"`)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		eval := q.Commands[0].(*EvalCommand)
		fstr := eval.Expr.(*FStringExpr)
		if len(fstr.Parts) != 2 {
			t.Fatalf("parts count: got %d, want 2", len(fstr.Parts))
		}
		if fstr.Parts[0].Literal != "from=" {
			t.Errorf("part 0: got %q, want \"from=\"", fstr.Parts[0].Literal)
		}
		if fstr.Parts[1].Expr != "from" {
			t.Errorf("part 1 expr: got %q, want \"from\"", fstr.Parts[1].Expr)
		}
	})
}
