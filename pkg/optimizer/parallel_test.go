package optimizer

import (
	"testing"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// flattenNestedAppendRule tests

func TestFlattenNestedAppend_Basic(t *testing.T) {
	// A | APPEND [B | APPEND [C]] → MULTISEARCH [A] [B] [C]
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "a"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{
					Source: &spl2.SourceClause{Index: "b"},
					Commands: []spl2.Command{
						&spl2.AppendCommand{
							Subquery: &spl2.Query{
								Source: &spl2.SourceClause{Index: "c"},
							},
						},
					},
				},
			},
		},
	}

	rule := &flattenNestedAppendRule{}
	result, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to fire")
	}

	if len(result.Commands) < 1 {
		t.Fatal("expected at least 1 command")
	}
	multi, ok := result.Commands[0].(*spl2.MultisearchCommand)
	if !ok {
		t.Fatalf("expected MultisearchCommand, got %T", result.Commands[0])
	}
	if len(multi.Searches) != 3 {
		t.Fatalf("expected 3 branches, got %d", len(multi.Searches))
	}

	// Branch 0 = main query (source=a, no commands).
	if multi.Searches[0].Source == nil || multi.Searches[0].Source.Index != "a" {
		t.Errorf("branch 0: expected source=a, got %v", multi.Searches[0].Source)
	}
	// Branch 1 = source=b, no commands.
	if multi.Searches[1].Source == nil || multi.Searches[1].Source.Index != "b" {
		t.Errorf("branch 1: expected source=b, got %v", multi.Searches[1].Source)
	}
	// Branch 2 = source=c.
	if multi.Searches[2].Source == nil || multi.Searches[2].Source.Index != "c" {
		t.Errorf("branch 2: expected source=c, got %v", multi.Searches[2].Source)
	}

	// Should have flattenedFromAppend annotation.
	ann, ok := result.GetAnnotation("flattenedFromAppend")
	if !ok {
		t.Fatal("expected flattenedFromAppend annotation")
	}
	if ann != true {
		t.Errorf("expected flattenedFromAppend=true, got %v", ann)
	}
}

func TestFlattenNestedAppend_NoNested(t *testing.T) {
	// A | APPEND [B] — no nested APPEND, rule should not fire.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "a"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{
					Source: &spl2.SourceClause{Index: "b"},
				},
			},
		},
	}

	rule := &flattenNestedAppendRule{}
	_, applied := rule.Apply(q)
	if applied {
		t.Fatal("expected rule NOT to fire for non-nested APPEND")
	}
}

func TestFlattenNestedAppend_PostAppendCommands(t *testing.T) {
	// B | APPEND [C] | head 10 — subquery has commands after APPEND.
	// The flatten should NOT flatten this particular branch because
	// the post-APPEND commands would be lost.
	// Outer: A | APPEND [B | APPEND [C] | head 10]
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "a"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{
					Source: &spl2.SourceClause{Index: "b"},
					Commands: []spl2.Command{
						&spl2.AppendCommand{
							Subquery: &spl2.Query{
								Source: &spl2.SourceClause{Index: "c"},
							},
						},
						&spl2.HeadCommand{Count: 10}, // post-APPEND command
					},
				},
			},
		},
	}

	rule := &flattenNestedAppendRule{}
	result, applied := rule.Apply(q)

	// The rule should still fire at the outer level because hasNestedAppend
	// is true (there IS an APPEND in the subquery). But flattenAppendBranches
	// treats the subquery as a leaf because it has post-APPEND commands.
	if !applied {
		t.Fatal("expected rule to fire")
	}

	multi, ok := result.Commands[0].(*spl2.MultisearchCommand)
	if !ok {
		t.Fatalf("expected MultisearchCommand, got %T", result.Commands[0])
	}
	// Should produce 2 branches (not 3) because the inner flatten is blocked.
	// Branch 0 = main (source=a), Branch 1 = the entire subquery as-is.
	if len(multi.Searches) != 2 {
		t.Fatalf("expected 2 branches (no deep flatten), got %d", len(multi.Searches))
	}
}

func TestFlattenNestedAppend_NilSubquery(t *testing.T) {
	// APPEND with nil subquery — should not fire.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "a"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{Subquery: nil},
		},
	}

	rule := &flattenNestedAppendRule{}
	_, applied := rule.Apply(q)
	if applied {
		t.Fatal("expected rule NOT to fire with nil subquery")
	}
}

// relaxAppendOrderingRule tests

func TestRelaxAppendOrdering_Stats(t *testing.T) {
	// APPEND + stats → annotated with interleaved.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "other"}},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}

	rule := &relaxAppendOrderingRule{}
	result, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to fire for APPEND + stats")
	}

	ann, ok := result.GetAnnotation("appendOrdering")
	if !ok {
		t.Fatal("expected appendOrdering annotation")
	}
	if ann != "interleaved" {
		t.Errorf("expected interleaved, got %v", ann)
	}
}

func TestRelaxAppendOrdering_NoDownstream(t *testing.T) {
	// APPEND at end of pipeline — no downstream command, should NOT annotate.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "other"}},
			},
		},
	}

	rule := &relaxAppendOrderingRule{}
	_, applied := rule.Apply(q)
	if applied {
		t.Fatal("expected rule NOT to fire when APPEND is last command")
	}
}

func TestRelaxAppendOrdering_DedupNotRelaxed(t *testing.T) {
	// APPEND + dedup → should NOT be annotated because dedup is order-sensitive
	// (keeps first occurrence — which duplicate is kept depends on input order).
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "other"}},
			},
			&spl2.DedupCommand{Fields: []string{"host"}},
		},
	}

	rule := &relaxAppendOrderingRule{}
	_, applied := rule.Apply(q)
	if applied {
		t.Fatal("expected rule NOT to fire for APPEND + dedup (order-sensitive)")
	}
}

func TestRelaxAppendOrdering_AlreadyAnnotated(t *testing.T) {
	// Already annotated — should be idempotent.
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "other"}},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}
	q.Annotate("appendOrdering", "interleaved")

	rule := &relaxAppendOrderingRule{}
	_, applied := rule.Apply(q)
	if applied {
		t.Fatal("expected rule NOT to fire when already annotated")
	}
}

func TestRelaxAppendOrdering_Sort(t *testing.T) {
	// APPEND + sort → annotated (sort re-orders anyway).
	q := &spl2.Query{
		Source: &spl2.SourceClause{Index: "main"},
		Commands: []spl2.Command{
			&spl2.AppendCommand{
				Subquery: &spl2.Query{Source: &spl2.SourceClause{Index: "other"}},
			},
			&spl2.SortCommand{
				Fields: []spl2.SortField{{Name: "count", Desc: true}},
			},
		},
	}

	rule := &relaxAppendOrderingRule{}
	_, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to fire for APPEND + sort")
	}
}

func TestRelaxAppendOrdering_Multisearch(t *testing.T) {
	// MultisearchCommand + stats → annotated.
	q := &spl2.Query{
		Commands: []spl2.Command{
			&spl2.MultisearchCommand{
				Searches: []*spl2.Query{
					{Source: &spl2.SourceClause{Index: "a"}},
					{Source: &spl2.SourceClause{Index: "b"}},
				},
			},
			&spl2.StatsCommand{
				Aggregations: []spl2.AggExpr{{Func: "count"}},
			},
		},
	}

	rule := &relaxAppendOrderingRule{}
	_, applied := rule.Apply(q)
	if !applied {
		t.Fatal("expected rule to fire for MultisearchCommand + stats")
	}
}
