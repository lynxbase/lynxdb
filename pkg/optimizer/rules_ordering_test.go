package optimizer

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// Unit tests: removeDeadSortRule

func TestRemoveDeadSort_StatsDestroysSort(t *testing.T) {
	// sort _time | stats count → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand, got %T", result.Commands[0])
	}
}

func TestRemoveDeadSort_StatsWithGroupByDestroysSort(t *testing.T) {
	// sort _time | stats avg(x) by host → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "avg", Args: []spl2.Expr{&spl2.FieldExpr{Name: "x"}}, Alias: "avg_x"}},
				GroupBy:      []string{"host"},
			},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_DedupDestroysSort(t *testing.T) {
	// sort _time | dedup host → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.DedupCommand{Fields: []string{"host"}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_TopDestroysSort(t *testing.T) {
	// sort _time | top 10 uri → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.TopCommand{N: 10, Field: "uri"},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_RareDestroysSort(t *testing.T) {
	// sort _time | rare method → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.RareCommand{N: 10, Field: "method"},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_EvalPreservesStatsDestroys(t *testing.T) {
	// sort _time | eval x=1 | stats count → sort removed (eval preserves, stats destroys)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.EvalCommand); !ok {
		t.Fatalf("expected EvalCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at 1, got %T", result.Commands[1])
	}
}

func TestRemoveDeadSort_JoinDestroysSort(t *testing.T) {
	// sort _time | join ... → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.JoinCommand{JoinType: "inner", Field: "id"},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_StreamstatsPreservesSort(t *testing.T) {
	// sort _time | streamstats count → sort KEPT (streamstats depends on ordering)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StreamstatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — streamstats depends on sort ordering")
	}
}

func TestRemoveDeadSort_TerminalSort(t *testing.T) {
	// sort _time (terminal) → sort KEPT (user sees sorted output)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — terminal sort is user-visible")
	}
}

func TestRemoveDeadSort_SortHeadKept(t *testing.T) {
	// sort _time | head 10 → sort KEPT (head preserves, user sees sorted output)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — sort+head is user-visible")
	}
}

func TestRemoveDeadSort_WherePreservesStatsDestroys(t *testing.T) {
	// sort _time | where x>1 | stats count → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_TimechartEstablishes(t *testing.T) {
	// sort _time | timechart count span=5m → sort removed (timechart is OrderEstablishing)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire — timechart re-establishes ordering")
	}
}

func TestRemoveDeadSort_RexPreservesStatsDestroys(t *testing.T) {
	// sort _time | rex ... | stats count → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.RexCommand{Field: "_raw", Pattern: `(?P<host>\S+)`},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_FillnullPreservesStatsDestroys(t *testing.T) {
	// sort _time | fillnull | stats count → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.FillnullCommand{Value: "0"},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

func TestRemoveDeadSort_OnlyFirstSortInChain(t *testing.T) {
	// sort _time | stats count | sort -count | head 10 → first sort removed, sort -count | head 10 kept
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &removeDeadSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire for first sort")
	}
	if len(result.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.SortCommand); !ok {
		t.Fatalf("expected SortCommand at 1, got %T", result.Commands[1])
	}
	if _, ok := result.Commands[2].(*spl2.HeadCommand); !ok {
		t.Fatalf("expected HeadCommand at 2, got %T", result.Commands[2])
	}
}

func TestRemoveDeadSort_SortEvalStreamstatsKept(t *testing.T) {
	// sort _time | eval x=1 | streamstats count → sort KEPT
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.StreamstatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — streamstats depends on sort ordering")
	}
}

func TestRemoveDeadSort_BinPreservesStatsDestroys(t *testing.T) {
	// sort _time | bin _time span=5m | stats count by _time → sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.BinCommand{Field: "_time", Span: "5m"},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"_time"},
			},
		},
	}
	rule := &removeDeadSortRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
}

// Unit tests: removeRedundantSortRule

func TestRemoveRedundantSort_TwoConsecutiveSorts(t *testing.T) {
	// sort _time | sort -size → first sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "size", Desc: true}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(result.Commands))
	}
	sc := result.Commands[0].(*spl2.SortCommand)
	if sc.Fields[0].Name != "size" {
		t.Fatalf("expected second sort to remain, got field=%s", sc.Fields[0].Name)
	}
}

func TestRemoveRedundantSort_EvalBetween(t *testing.T) {
	// sort _time | eval x=1 | sort _time → first sort removed
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.EvalCommand); !ok {
		t.Fatalf("expected EvalCommand at 0, got %T", result.Commands[0])
	}
}

func TestRemoveRedundantSort_SortBeforeTopN(t *testing.T) {
	// sort _time | TopNCommand → first sort removed (TopN re-establishes)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.TopNCommand{Fields: []spl2.SortField{{Name: "size", Desc: true}}, Limit: 10},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.TopNCommand); !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
}

func TestRemoveRedundantSort_StreamstatsBetweenSorts(t *testing.T) {
	// sort _time | streamstats count | sort -size → first sort KEPT (streamstats depends on it)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StreamstatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "size", Desc: true}}},
		},
	}
	rule := &removeRedundantSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — streamstats depends on first sort")
	}
}

// Unit tests: removeRedundantSortRule — sort after same-ordering establisher

func TestRemoveRedundantSort_TimechartSortAscTime(t *testing.T) {
	// timechart count span=5m | sort _time → sort removed (timechart produces asc _time)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire — sort matches timechart output ordering")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.TimechartCommand); !ok {
		t.Fatalf("expected TimechartCommand, got %T", result.Commands[0])
	}
}

func TestRemoveRedundantSort_TimechartSortDescTimeKept(t *testing.T) {
	// timechart count span=5m | sort -_time → sort KEPT (direction mismatch)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: true}}},
		},
	}
	rule := &removeRedundantSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — sort direction doesn't match timechart (desc vs asc)")
	}
}

func TestRemoveRedundantSort_TimechartEvalSortAscTime(t *testing.T) {
	// timechart count span=5m | eval x=1 | sort _time → sort removed
	// (eval is order-preserving, timechart produces asc _time)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire — sort matches timechart output through preserving eval")
	}
	if len(result.Commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.TimechartCommand); !ok {
		t.Fatalf("expected TimechartCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.EvalCommand); !ok {
		t.Fatalf("expected EvalCommand at 1, got %T", result.Commands[1])
	}
}

func TestRemoveRedundantSort_TimechartSortNonTimeKept(t *testing.T) {
	// timechart count span=5m | sort count → sort KEPT (field mismatch)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — sort field doesn't match timechart ordering")
	}
}

func TestRemoveRedundantSort_TopNSortSameFields(t *testing.T) {
	// TopN{-count, limit=10} | sort -count → sort removed (TopN already produces -count order)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TopNCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}, Limit: 10},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire — sort matches TopN output ordering")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.TopNCommand); !ok {
		t.Fatalf("expected TopNCommand, got %T", result.Commands[0])
	}
}

func TestRemoveRedundantSort_StatsBreaksChain(t *testing.T) {
	// timechart count span=5m | stats count | sort _time → sort KEPT
	// (stats is OrderDestroying between timechart and sort)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — stats between timechart and sort destroys ordering")
	}
}

func TestRemoveRedundantSort_AnnotatesMessage(t *testing.T) {
	// timechart count span=5m | sort _time → sort removed, annotation present
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	v, ok := result.GetAnnotation("optimizerMessages")
	if !ok {
		t.Fatal("expected optimizerMessages annotation")
	}
	msgs, ok := v.([]string)
	if !ok || len(msgs) == 0 {
		t.Fatal("expected non-empty optimizerMessages")
	}
	// Verify the message mentions "timechart" and "redundant".
	found := false
	for _, m := range msgs {
		if contains(m, "timechart") && contains(m, "redundant") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected annotation mentioning 'timechart' and 'redundant', got %v", msgs)
	}
}

func TestRemoveRedundantSort_StreamstatsBetweenEstablisherAndSort(t *testing.T) {
	// timechart count span=5m | streamstats count | sort _time → sort KEPT
	// (streamstats depends on order — backward walk must stop)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.StreamstatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeRedundantSortRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — streamstats between timechart and sort")
	}
}

func TestRemoveRedundantSort_FullOptimizer_TimechartSort(t *testing.T) {
	// Full optimizer: timechart count span=5m | sort _time → no SortCommand, rule incremented
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.TimechartCommand{Span: "5m", Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected no SortCommand — sort should be eliminated as redundant after timechart")
		}
	}
	if opt.Stats["RemoveRedundantSort"] == 0 {
		t.Error("RemoveRedundantSort rule should have been applied")
	}
}

// Integration tests: full optimizer pipeline

func TestRemoveDeadSort_FullOptimizer(t *testing.T) {
	// search "foo" | sort _time | stats count by host → no SortCommand in output
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "foo"},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected no SortCommand in optimized output")
		}
	}
	if opt.Stats["RemoveDeadSort"] == 0 {
		t.Error("RemoveDeadSort rule should have been applied")
	}
}

func TestRemoveDeadSort_SortHeadBecomesTopN(t *testing.T) {
	// sort -x | head 10 → TopNCommand (ordering rules don't interfere with earlyLimitRule)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "x", Desc: true}}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	found := false
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.TopNCommand); ok {
			found = true
		}
	}
	if !found {
		t.Fatal("expected TopNCommand after sort+head fusion")
	}
}

func TestRemoveDeadSort_BothSortsEliminated(t *testing.T) {
	// sort _time | sort -size | stats count → both sorts removed (fixed-point)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "size", Desc: true}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected no SortCommand in optimized output")
		}
	}
	// First iteration: removeRedundantSort removes first sort.
	// Second iteration: removeDeadSort removes second sort (before stats).
	totalSortRemovals := opt.Stats["RemoveDeadSort"] + opt.Stats["RemoveRedundantSort"]
	if totalSortRemovals < 2 {
		t.Errorf("expected at least 2 sort removals, got %d", totalSortRemovals)
	}
}

func TestRemoveDeadSort_SortEvalHeadKept(t *testing.T) {
	// sort status | eval x=1 | head 10 → sort KEPT (user-visible ordering through preserving commands)
	// Uses status (not _time) to avoid removeSortOnScanOrderRule firing first.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "status"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	// The sort should NOT be eliminated by RemoveDeadSort.
	// It may be fused into TopN by earlyLimitRule (sort+eval+head → head pushes past eval → sort+head → TopN).
	hasSort := false
	hasTopN := false
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			hasSort = true
		}
		if _, ok := cmd.(*spl2.TopNCommand); ok {
			hasTopN = true
		}
	}
	if !hasSort && !hasTopN {
		t.Fatal("expected either SortCommand or TopNCommand to remain — sort is user-visible")
	}
	if opt.Stats["RemoveDeadSort"] > 0 {
		t.Error("RemoveDeadSort should NOT have fired — sort ordering is user-visible")
	}
}

func TestTotalRulesCount(t *testing.T) {
	opt := New()
	// Base rules: 35 (from allRules including transformAggPushdown). Adding 4 ordering rules → 39.
	// This test documents the expected count and catches accidental additions/removals.
	expected := 39
	got := opt.TotalRules()
	if got != expected {
		t.Errorf("TotalRules() = %d, want %d", got, expected)
	}
}

// orderingBehavior classification tests

func TestOrderingBehavior(t *testing.T) {
	tests := []struct {
		name     string
		cmd      spl2.Command
		expected OrderingBehavior
	}{
		// OrderPreserving
		{"SearchCommand", &spl2.SearchCommand{}, OrderPreserving},
		{"WhereCommand", &spl2.WhereCommand{}, OrderPreserving},
		{"EvalCommand", &spl2.EvalCommand{}, OrderPreserving},
		{"FieldsCommand", &spl2.FieldsCommand{}, OrderPreserving},
		{"TableCommand", &spl2.TableCommand{}, OrderPreserving},
		{"RenameCommand", &spl2.RenameCommand{}, OrderPreserving},
		{"RexCommand", &spl2.RexCommand{}, OrderPreserving},
		{"BinCommand", &spl2.BinCommand{}, OrderPreserving},
		{"FillnullCommand", &spl2.FillnullCommand{}, OrderPreserving},
		{"HeadCommand", &spl2.HeadCommand{}, OrderPreserving},
		{"TailCommand", &spl2.TailCommand{}, OrderPreserving},
		{"StreamstatsCommand", &spl2.StreamstatsCommand{}, OrderPreserving},
		{"EventstatsCommand", &spl2.EventstatsCommand{}, OrderPreserving},
		{"FromCommand", &spl2.FromCommand{}, OrderPreserving},
		{"MaterializeCommand", &spl2.MaterializeCommand{}, OrderPreserving},
		{"ViewsCommand", &spl2.ViewsCommand{}, OrderPreserving},
		{"DropviewCommand", &spl2.DropviewCommand{}, OrderPreserving},
		// OrderEstablishing
		{"SortCommand", &spl2.SortCommand{}, OrderEstablishing},
		{"TopNCommand", &spl2.TopNCommand{}, OrderEstablishing},
		{"TimechartCommand", &spl2.TimechartCommand{}, OrderEstablishing},
		// OrderDestroying
		{"StatsCommand", &spl2.StatsCommand{}, OrderDestroying},
		{"DedupCommand", &spl2.DedupCommand{}, OrderDestroying},
		{"TopCommand", &spl2.TopCommand{}, OrderDestroying},
		{"RareCommand", &spl2.RareCommand{}, OrderDestroying},
		{"JoinCommand", &spl2.JoinCommand{}, OrderDestroying},
		{"AppendCommand", &spl2.AppendCommand{}, OrderDestroying},
		{"MultisearchCommand", &spl2.MultisearchCommand{}, OrderDestroying},
		{"TransactionCommand", &spl2.TransactionCommand{}, OrderDestroying},
		{"XYSeriesCommand", &spl2.XYSeriesCommand{}, OrderDestroying},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := orderingBehavior(tt.cmd)
			if got != tt.expected {
				t.Errorf("orderingBehavior(%T) = %d, want %d", tt.cmd, got, tt.expected)
			}
		})
	}
}

// Unit tests: sortStatsReorderRule

func TestSortStatsReorder_BasicRewrite(t *testing.T) {
	// sort _time | stats count by _time → stats count by _time | sort _time
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"_time"},
			},
		},
	}
	rule := &sortStatsReorderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 2 {
		t.Fatalf("expected 2 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.SortCommand); !ok {
		t.Fatalf("expected SortCommand at 1, got %T", result.Commands[1])
	}
}

func TestSortStatsReorder_PrefixGroupBy(t *testing.T) {
	// sort _time | stats avg(x) by _time, host → stats avg(x) by _time, host | sort _time
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "avg", Args: []spl2.Expr{&spl2.FieldExpr{Name: "x"}}, Alias: "avg_x"}},
				GroupBy:      []string{"_time", "host"},
			},
		},
	}
	rule := &sortStatsReorderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if _, ok := result.Commands[0].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.SortCommand); !ok {
		t.Fatalf("expected SortCommand at 1, got %T", result.Commands[1])
	}
}

func TestSortStatsReorder_NonPrefixNoRewrite(t *testing.T) {
	// sort _time | stats count by host → no change (sort key ≠ group-by key)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"host"},
			},
		},
	}
	rule := &sortStatsReorderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — sort key not in group-by prefix")
	}
}

func TestSortStatsReorder_OutputSortOverrides(t *testing.T) {
	// sort _time | stats count by _time | sort -count → no rewrite (output sort is different)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"_time"},
			},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count", Desc: true}}},
		},
	}
	rule := &sortStatsReorderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — downstream sort overrides")
	}
}

func TestSortStatsReorder_NoGroupBy(t *testing.T) {
	// sort _time | stats count → no change (no group-by, removeDeadSort handles this)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
			},
		},
	}
	rule := &sortStatsReorderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire — no group-by fields")
	}
}

func TestSortStatsReorder_EvalBetween(t *testing.T) {
	// sort _time | eval x=1 | stats count by _time → eval x=1 | stats count by _time | sort _time
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.EvalCommand{Field: "x", Expr: &spl2.LiteralExpr{Value: "1"}},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
				GroupBy:      []string{"_time"},
			},
		},
	}
	rule := &sortStatsReorderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	if len(result.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.EvalCommand); !ok {
		t.Fatalf("expected EvalCommand at 0, got %T", result.Commands[0])
	}
	if _, ok := result.Commands[1].(*spl2.StatsCommand); !ok {
		t.Fatalf("expected StatsCommand at 1, got %T", result.Commands[1])
	}
	if _, ok := result.Commands[2].(*spl2.SortCommand); !ok {
		t.Fatalf("expected SortCommand at 2, got %T", result.Commands[2])
	}
}

// Diagnostic annotation tests

func TestRemoveDeadSort_AnnotatesMessage(t *testing.T) {
	// sort _time | stats count → sort removed, annotation present
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
		},
	}
	rule := &removeDeadSortRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire")
	}
	v, ok := result.GetAnnotation("optimizerMessages")
	if !ok {
		t.Fatal("expected optimizerMessages annotation")
	}
	msgs, ok := v.([]string)
	if !ok || len(msgs) == 0 {
		t.Fatal("expected non-empty optimizerMessages")
	}
}

func TestSortWarning_NoLimit(t *testing.T) {
	// sort _time (terminal, no head/tail) → warning annotation present
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeDeadSortRule{}
	rule.Apply(q)
	v, ok := q.GetAnnotation("optimizerWarnings")
	if !ok {
		t.Fatal("expected optimizerWarnings annotation for unbounded sort")
	}
	warns, ok := v.([]string)
	if !ok || len(warns) == 0 {
		t.Fatal("expected non-empty optimizerWarnings")
	}
}

func TestSortWarning_WithHeadNoWarning(t *testing.T) {
	// sort _time | head 10 → no warning (has downstream limit)
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
			&spl2.HeadCommand{Count: 10},
		},
	}
	rule := &removeDeadSortRule{}
	rule.Apply(q)
	_, ok := q.GetAnnotation("optimizerWarnings")
	if ok {
		t.Fatal("expected no optimizerWarnings when head limits the sort")
	}
}

// Unit tests: removeSortOnScanOrderRule

func TestRemoveSortOnScanOrder_AscendingSingleSource(t *testing.T) {
	// sort _time (ascending, single source) → removed, reverseScanOrder annotation set
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire for ascending sort _time on single source")
	}
	if len(result.Commands) != 0 {
		t.Fatalf("expected 0 commands after removing sort, got %d", len(result.Commands))
	}
	v, ok := result.GetAnnotation("reverseScanOrder")
	if !ok {
		t.Fatal("expected reverseScanOrder annotation")
	}
	if b, ok := v.(bool); !ok || !b {
		t.Fatalf("expected reverseScanOrder=true, got %v", v)
	}
	// Verify diagnostic message.
	msgs, ok := result.GetAnnotation("optimizerMessages")
	if !ok {
		t.Fatal("expected optimizerMessages annotation")
	}
	msgList, ok := msgs.([]string)
	if !ok || len(msgList) == 0 {
		t.Fatal("expected non-empty optimizerMessages")
	}
}

func TestRemoveSortOnScanOrder_DescendingSingleSource(t *testing.T) {
	// sort -_time (descending, single source) → removed, NO reverseScanOrder
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: true}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire for descending sort _time on single source")
	}
	if len(result.Commands) != 0 {
		t.Fatalf("expected 0 commands after removing sort, got %d", len(result.Commands))
	}
	// Should NOT have reverseScanOrder (descending matches default scan order).
	_, ok := result.GetAnnotation("reverseScanOrder")
	if ok {
		t.Fatal("expected no reverseScanOrder annotation for descending sort")
	}
}

func TestRemoveSortOnScanOrder_NilSourcePipeMode(t *testing.T) {
	// Pipe mode (no source clause) is single-source → rule should fire.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time", Desc: false}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire for pipe mode (nil source)")
	}
}

func TestRemoveSortOnScanOrder_MultiSourceNotRemoved(t *testing.T) {
	// FROM idx_a, idx_b | sort _time → NOT removed (multi-source, order not guaranteed)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Indices: []string{"idx_a", "idx_b"}},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire for multi-source query")
	}
}

func TestRemoveSortOnScanOrder_GlobSourceNotRemoved(t *testing.T) {
	// FROM idx_* | sort _time → NOT removed (glob source)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "idx_*", IsGlob: true},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire for glob source")
	}
}

func TestRemoveSortOnScanOrder_NonTimeFieldNotRemoved(t *testing.T) {
	// sort status → NOT removed (not the primary key)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "status"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire for non-_time sort field")
	}
}

func TestRemoveSortOnScanOrder_MultiFieldNotRemoved(t *testing.T) {
	// sort _time, host → NOT removed (multi-field sort)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{
				{Name: "_time"},
				{Name: "host"},
			}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire for multi-field sort")
	}
}

func TestRemoveSortOnScanOrder_IntermediateStatsNotRemoved(t *testing.T) {
	// stats count | sort _time → NOT removed (stats is order-destroying between source and sort)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.StatsCommand{Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire with order-destroying command between source and sort")
	}
}

func TestRemoveSortOnScanOrder_WherePreservingRemoved(t *testing.T) {
	// where x>1 | sort _time → removed (where is order-preserving)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire (where is order-preserving)")
	}
	if len(result.Commands) != 1 {
		t.Fatalf("expected 1 command (where), got %d", len(result.Commands))
	}
	if _, ok := result.Commands[0].(*spl2.WhereCommand); !ok {
		t.Fatalf("expected WhereCommand, got %T", result.Commands[0])
	}
}

func TestRemoveSortOnScanOrder_ChainOfPreservingRemoved(t *testing.T) {
	// search "foo" | where x>1 | eval y=2 | sort _time → removed
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "foo"},
			&spl2.WhereCommand{Expr: &spl2.CompareExpr{Left: &spl2.FieldExpr{Name: "x"}, Op: ">", Right: &spl2.LiteralExpr{Value: "1"}}},
			&spl2.EvalCommand{Field: "y", Expr: &spl2.LiteralExpr{Value: "2"}},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	result, changed := rule.Apply(q)
	if !changed {
		t.Fatal("expected rule to fire through chain of order-preserving commands")
	}
	if len(result.Commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(result.Commands))
	}
}

func TestRemoveSortOnScanOrder_VariableSourceNotRemoved(t *testing.T) {
	// FROM $var | sort _time → NOT removed (variable source)
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "$threats", IsVariable: true},
		Commands: []spl2.Command{
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	rule := &removeSortOnScanOrderRule{}
	_, changed := rule.Apply(q)
	if changed {
		t.Fatal("expected rule NOT to fire for variable source")
	}
}

func TestRemoveSortOnScanOrder_FullOptimizer(t *testing.T) {
	// Full optimizer pipeline: search "foo" | sort _time (single source) → sort eliminated
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.SearchCommand{Term: "foo"},
			&spl2.SortCommand{Fields: []spl2.SortField{{Name: "_time"}}},
		},
	}
	opt := New()
	result := opt.Optimize(q)
	for _, cmd := range result.Commands {
		if _, ok := cmd.(*spl2.SortCommand); ok {
			t.Fatal("expected no SortCommand after optimization — scan-order sort should be eliminated")
		}
	}
	if opt.Stats["RemoveSortOnScanOrder"] == 0 {
		t.Error("RemoveSortOnScanOrder rule should have been applied")
	}
}
