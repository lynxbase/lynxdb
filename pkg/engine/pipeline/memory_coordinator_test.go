package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestCoordinatorEqualSplit(t *testing.T) {
	// 3 operators, 300MB budget, 10% headroom.
	// Headroom = 10MB (capped at maxHeadroom).
	// Remaining = 300MB - 10MB - (256KB + 128KB + 256KB) = ~289.4MB.
	// Per-op share = ~96.5MB + reservation.
	budget := int64(300 << 20) // 300MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acct1 := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acct2 := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	acct3 := mc.RegisterOperator("join", mon.NewAccount("join"), reservationJoin)
	mc.Finalize()

	// All three should have roughly equal sub-limits.
	s := mc.Stats()
	if len(s) != 3 {
		t.Fatalf("expected 3 slots, got %d", len(s))
	}

	headroom := int64(10 << 20) // 10MB
	sumReservations := reservationSort + reservationAggregate + reservationJoin
	remaining := budget - headroom - sumReservations
	perOp := remaining / 3

	expectedSort := reservationSort + perOp
	expectedAgg := reservationAggregate + perOp
	expectedJoin := reservationJoin + perOp

	if s[0].SoftLimit != expectedSort {
		t.Errorf("sort soft limit: got %d, want %d", s[0].SoftLimit, expectedSort)
	}
	if s[1].SoftLimit != expectedAgg {
		t.Errorf("aggregate soft limit: got %d, want %d", s[1].SoftLimit, expectedAgg)
	}
	if s[2].SoftLimit != expectedJoin {
		t.Errorf("join soft limit: got %d, want %d", s[2].SoftLimit, expectedJoin)
	}

	// Verify accounts are functional.
	if err := acct1.Grow(1024); err != nil {
		t.Errorf("acct1.Grow: unexpected error: %v", err)
	}
	if acct1.Used() != 1024 {
		t.Errorf("acct1.Used: got %d, want 1024", acct1.Used())
	}
	if err := acct2.Grow(512); err != nil {
		t.Errorf("acct2.Grow: unexpected error: %v", err)
	}
	if err := acct3.Grow(256); err != nil {
		t.Errorf("acct3.Grow: unexpected error: %v", err)
	}
}

func TestCoordinatorRedistributeAfterSpill(t *testing.T) {
	// 2 operators, 100MB budget, 10% headroom.
	budget := int64(100 << 20) // 100MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	statsBefore := mc.Stats()
	limitA := statsBefore[0].SoftLimit
	limitB := statsBefore[1].SoftLimit

	// Operator A spills.
	acctA.NotifySpilled()

	statsAfter := mc.Stats()

	// A should be at reservation.
	if statsAfter[0].SoftLimit != reservationSort {
		t.Errorf("after spill: sort soft limit: got %d, want %d", statsAfter[0].SoftLimit, reservationSort)
	}
	if !statsAfter[0].Spilled {
		t.Error("after spill: sort should be marked as spilled")
	}

	// B should have received A's freed capacity.
	freed := limitA - reservationSort
	expectedB := limitB + freed
	if statsAfter[1].SoftLimit != expectedB {
		t.Errorf("after spill: aggregate soft limit: got %d, want %d", statsAfter[1].SoftLimit, expectedB)
	}
	if statsAfter[1].Spilled {
		t.Error("after spill: aggregate should not be marked as spilled")
	}
}

func TestCoordinatorOneWayRatchet(t *testing.T) {
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	// Spill once.
	acctA.NotifySpilled()
	statsAfter1 := mc.Stats()
	limitB1 := statsAfter1[1].SoftLimit

	// Spill again — should be idempotent.
	acctA.NotifySpilled()
	statsAfter2 := mc.Stats()

	if statsAfter2[0].SoftLimit != reservationSort {
		t.Errorf("double spill: sort soft limit changed: got %d, want %d", statsAfter2[0].SoftLimit, reservationSort)
	}
	if statsAfter2[1].SoftLimit != limitB1 {
		t.Errorf("double spill: aggregate soft limit changed: got %d, want %d", statsAfter2[1].SoftLimit, limitB1)
	}
}

func TestCoordinatorAllSpilled(t *testing.T) {
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acctB := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	// Both spill — should not panic.
	acctA.NotifySpilled()
	acctB.NotifySpilled()

	s := mc.Stats()
	if s[0].SoftLimit != reservationSort {
		t.Errorf("all spilled: sort soft limit: got %d, want %d", s[0].SoftLimit, reservationSort)
	}
	if s[1].SoftLimit != reservationAggregate {
		t.Errorf("all spilled: aggregate soft limit: got %d, want %d", s[1].SoftLimit, reservationAggregate)
	}
}

func TestCoordinatorGrowSubLimitEnforced(t *testing.T) {
	// Small budget so we can easily exceed a sub-limit.
	budget := int64(1 << 20) // 1MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acct := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	softLimit := mc.Stats()[0].SoftLimit

	// Grow beyond sub-limit should fail.
	err := acct.Grow(softLimit + 1)
	if err == nil {
		t.Fatal("expected error when growing beyond sub-limit")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Errorf("expected BudgetExceededError, got %T: %v", err, err)
	}

	// Grow within sub-limit should succeed.
	if err := acct.Grow(softLimit / 2); err != nil {
		t.Errorf("grow within sub-limit: unexpected error: %v", err)
	}
}

func TestCoordinatorGrowDelegatesInnerError(t *testing.T) {
	// Inner monitor has a very tight limit.
	innerLimit := int64(1024)
	mon := memgov.NewTestBudget("test", innerLimit)

	// Coordinator has a much larger budget (so sub-limit is not the bottleneck).
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	acct := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	mc.Finalize()

	// Sub-limit is large, but inner account's monitor limit is 1024.
	err := acct.Grow(innerLimit + 1)
	if err == nil {
		t.Fatal("expected error from inner account")
	}
	if !memgov.IsBudgetExceeded(err) {
		t.Errorf("expected BudgetExceededError from inner, got %T: %v", err, err)
	}
}

func TestCoordinatorSingleOperator(t *testing.T) {
	// When coordinator is nil, newCoordinatedAccount should return plain account.
	qc := &queryContext{}

	acct := qc.newCoordinatedAccount("sort", reservationSort)
	if acct == nil {
		t.Fatal("expected non-nil account")
	}

	// Should be a plain NopAccount, not a CoordinatedAccount.
	if _, ok := acct.(*CoordinatedAccount); ok {
		t.Error("expected plain account when coordinator is nil, got CoordinatedAccount")
	}

	// Should still work normally.
	if err := acct.Grow(1024); err != nil {
		t.Errorf("grow: unexpected error: %v", err)
	}
	acct.Close()
}

func TestCoordinatorNilSafe(t *testing.T) {
	// Nil coordinator on queryContext should not create coordinator.
	qc := &queryContext{}

	if qc.coordinator != nil {
		t.Error("expected nil coordinator")
	}

	acct := qc.newCoordinatedAccount("sort", reservationSort)
	if acct == nil {
		t.Fatal("expected non-nil account even with nil coordinator")
	}
}

func TestCountSpillableOps(t *testing.T) {
	tests := []struct {
		name     string
		prog     *spl2.Program
		expected int
	}{
		{
			name:     "nil program",
			prog:     nil,
			expected: 0,
		},
		{
			name: "no spillable ops",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.WhereCommand{},
						&spl2.HeadCommand{Count: 10},
					},
				},
			},
			expected: 0,
		},
		{
			name: "sort only",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.SortCommand{},
					},
				},
			},
			expected: 1,
		},
		{
			name: "sort + stats",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.StatsCommand{},
						&spl2.SortCommand{},
					},
				},
			},
			expected: 2,
		},
		{
			name: "timechart + sort",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.TimechartCommand{},
						&spl2.SortCommand{},
					},
				},
			},
			expected: 2,
		},
		{
			name: "join with subquery containing stats",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.JoinCommand{
							Subquery: &spl2.Query{
								Commands: []spl2.Command{
									&spl2.StatsCommand{},
								},
							},
						},
						&spl2.SortCommand{},
					},
				},
			},
			expected: 3, // join + subquery stats + sort
		},
		{
			name: "dedup + eventstats",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.DedupCommand{Fields: []string{"host"}},
						&spl2.EventstatsCommand{},
					},
				},
			},
			expected: 2,
		},
		{
			name: "CTE with stats + main with sort",
			prog: &spl2.Program{
				Datasets: []spl2.DatasetDef{
					{
						Name: "cte1",
						Query: &spl2.Query{
							Commands: []spl2.Command{
								&spl2.StatsCommand{},
							},
						},
					},
				},
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.SortCommand{},
					},
				},
			},
			expected: 2,
		},
		{
			name: "multisearch with spillable branches",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.MultisearchCommand{
							Searches: []*spl2.Query{
								{Commands: []spl2.Command{&spl2.StatsCommand{}}},
								{Commands: []spl2.Command{&spl2.SortCommand{}}},
							},
						},
					},
				},
			},
			expected: 2,
		},
		{
			name: "append with spillable subquery",
			prog: &spl2.Program{
				Main: &spl2.Query{
					Commands: []spl2.Command{
						&spl2.StatsCommand{},
						&spl2.AppendCommand{
							Subquery: &spl2.Query{
								Commands: []spl2.Command{
									&spl2.SortCommand{},
								},
							},
						},
					},
				},
			},
			expected: 2, // stats + sort in subquery
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := countSpillableOps(tt.prog)
			if got != tt.expected {
				t.Errorf("countSpillableOps() = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestCoordinatorShrinkAfterSpill(t *testing.T) {
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acct := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	// Grow, then shrink.
	if err := acct.Grow(1024); err != nil {
		t.Fatalf("grow: %v", err)
	}
	if acct.Used() != 1024 {
		t.Errorf("used after grow: got %d, want 1024", acct.Used())
	}

	acct.Shrink(512)
	if acct.Used() != 512 {
		t.Errorf("used after shrink: got %d, want 512", acct.Used())
	}

	// Spill.
	acct.NotifySpilled()

	// Shrink remaining.
	acct.Shrink(acct.Used())
	if acct.Used() != 0 {
		t.Errorf("used after full shrink: got %d, want 0", acct.Used())
	}

	// MaxUsed should still reflect the peak.
	if acct.MaxUsed() != 1024 {
		t.Errorf("maxUsed: got %d, want 1024", acct.MaxUsed())
	}
}

func TestCoordinatorSmallBudget(t *testing.T) {
	// Budget smaller than sum of reservations.
	budget := reservationSort / 2 // much less than one reservation
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget*10) // inner limit is generous
	_ = mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	s := mc.Stats()
	// Each operator should get exactly its reservation (remaining = 0).
	if s[0].SoftLimit != reservationSort {
		t.Errorf("small budget: sort soft limit: got %d, want %d", s[0].SoftLimit, reservationSort)
	}
	if s[1].SoftLimit != reservationAggregate {
		t.Errorf("small budget: aggregate soft limit: got %d, want %d", s[1].SoftLimit, reservationAggregate)
	}
}

func TestSortAndAggregateCoordinated(t *testing.T) {
	// Integration test: pipeline "| stats count by host | sort -count" with small budget.
	// Verify correct results and that redistribution happened.
	budget := int64(256 * 1024) // 256KB — enough for scan but forces spill on sort/aggregate
	gov := memgov.NewGovernor(memgov.GovernorConfig{TotalLimit: budget})

	tmpDir := t.TempDir()
	spillMgr, err := NewSpillManager(tmpDir, nil)
	if err != nil {
		t.Fatalf("create spill manager: %v", err)
	}
	defer spillMgr.CleanupAll()

	// Build events: 100 events across 10 hosts.
	events := make([]*event.Event, 100)
	for i := range events {
		host := "host-" + string(rune('A'+i%10))
		events[i] = &event.Event{
			Fields: map[string]event.Value{
				"host":  event.StringValue(host),
				"level": event.StringValue("info"),
			},
		}
	}

	store := &ServerIndexStore{
		Events: map[string][]*event.Event{"main": events},
	}

	prog := &spl2.Program{
		Main: &spl2.Query{
			Source: &spl2.SourceClause{Index: "main"},
			Commands: []spl2.Command{
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{
						{Func: "count", Alias: "count"},
					},
					GroupBy: []string{"host"},
				},
				&spl2.SortCommand{
					Fields: []spl2.SortField{
						{Name: "count", Desc: true},
					},
				},
			},
		},
	}

	ctx := context.Background()
	result, err := BuildProgramWithGovernor(ctx, prog, store, nil, nil, 64, "", gov, budget, spillMgr, false, nil)
	if err != nil {
		t.Fatalf("build program: %v", err)
	}
	if result.GovBudget != nil {
		defer result.GovBudget.Close()
	}

	rows, err := CollectAll(ctx, result.Iterator)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	// Should have 10 groups (one per host), each with count=10.
	if len(rows) != 10 {
		t.Errorf("expected 10 result rows, got %d", len(rows))
	}

	for _, row := range rows {
		countVal, ok := row["count"]
		if !ok {
			t.Error("missing count field")

			continue
		}
		if countVal.AsInt() != 10 {
			t.Errorf("expected count=10, got %d for host=%s", countVal.AsInt(), row["host"])
		}
	}

	// Verify sort order: results should be sorted by count descending.
	// Since all counts are equal, any order is valid — just verify they all have count=10.
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["count"].AsInt()
		curr := rows[i]["count"].AsInt()
		if prev < curr {
			t.Errorf("results not sorted: row %d count=%d < row %d count=%d", i-1, prev, i, curr)
		}
	}
}

func TestCoordinatorPhaseReclaim(t *testing.T) {
	// 2 operators: first completes, second should get freed capacity.
	budget := int64(100 << 20) // 100MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	statsBefore := mc.Stats()
	limitA := statsBefore[0].SoftLimit
	limitB := statsBefore[1].SoftLimit

	// Operator A completes — all its capacity should be redistributed.
	acctA.SetPhase(PhaseComplete)

	statsAfter := mc.Stats()

	// A should have soft limit = 0 (completed operators get nothing).
	if statsAfter[0].SoftLimit != 0 {
		t.Errorf("after complete: sort soft limit: got %d, want 0", statsAfter[0].SoftLimit)
	}
	if statsAfter[0].Phase != PhaseComplete {
		t.Errorf("after complete: sort phase: got %d, want %d", statsAfter[0].Phase, PhaseComplete)
	}

	// B should have received all of A's capacity.
	expectedB := limitB + limitA
	if statsAfter[1].SoftLimit != expectedB {
		t.Errorf("after complete: aggregate soft limit: got %d, want %d", statsAfter[1].SoftLimit, expectedB)
	}
}

func TestCoordinatorPhaseInStats(t *testing.T) {
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acctB := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	// Initial: both idle.
	s := mc.Stats()
	if s[0].Phase != PhaseIdle {
		t.Errorf("initial sort phase: got %d, want %d", s[0].Phase, PhaseIdle)
	}
	if s[1].Phase != PhaseIdle {
		t.Errorf("initial aggregate phase: got %d, want %d", s[1].Phase, PhaseIdle)
	}

	// Set various phases.
	acctA.SetPhase(PhaseBuilding)
	acctB.SetPhase(PhaseProbing)

	s = mc.Stats()
	if s[0].Phase != PhaseBuilding {
		t.Errorf("sort phase after set: got %d, want %d", s[0].Phase, PhaseBuilding)
	}
	if s[1].Phase != PhaseProbing {
		t.Errorf("aggregate phase after set: got %d, want %d", s[1].Phase, PhaseProbing)
	}

	// Transition sort to probing.
	acctA.SetPhase(PhaseProbing)
	s = mc.Stats()
	if s[0].Phase != PhaseProbing {
		t.Errorf("sort phase after probing: got %d, want %d", s[0].Phase, PhaseProbing)
	}
}

func TestCoordinatorReclaimIdempotent(t *testing.T) {
	// Completing the same operator twice should not cause double redistribution.
	budget := int64(100 << 20)
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctA := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	// Complete once.
	acctA.SetPhase(PhaseComplete)
	statsAfter1 := mc.Stats()
	limitB1 := statsAfter1[1].SoftLimit

	// Complete again — should be idempotent.
	acctA.SetPhase(PhaseComplete)
	statsAfter2 := mc.Stats()

	if statsAfter2[0].SoftLimit != 0 {
		t.Errorf("double complete: sort soft limit changed: got %d, want 0", statsAfter2[0].SoftLimit)
	}
	if statsAfter2[1].SoftLimit != limitB1 {
		t.Errorf("double complete: aggregate soft limit changed: got %d, want %d", statsAfter2[1].SoftLimit, limitB1)
	}
}

func TestCoordinatorThreeOpPhaseHandoff(t *testing.T) {
	// 3 operators: sort(complete) → aggregate(building) → join(idle).
	// Freed from sort should split between aggregate and join.
	budget := int64(300 << 20) // 300MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctSort := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acctAgg := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	_ = mc.RegisterOperator("join", mon.NewAccount("join"), reservationJoin)
	mc.Finalize()

	statsBefore := mc.Stats()
	sortLimit := statsBefore[0].SoftLimit
	aggLimit := statsBefore[1].SoftLimit
	joinLimit := statsBefore[2].SoftLimit

	// Set aggregate to building (sort is still idle → will transition to complete).
	acctAgg.SetPhase(PhaseBuilding)

	// Sort completes — freed capacity goes to aggregate and join (both active).
	acctSort.SetPhase(PhaseComplete)

	statsAfter := mc.Stats()

	// Sort should be at 0.
	if statsAfter[0].SoftLimit != 0 {
		t.Errorf("sort soft limit after complete: got %d, want 0", statsAfter[0].SoftLimit)
	}

	// Sort's freed capacity split equally between aggregate and join.
	perActive := sortLimit / 2
	remainder := sortLimit % 2
	expectedAgg := aggLimit + perActive
	expectedJoin := joinLimit + perActive
	// First active slot gets the remainder.
	expectedAgg += remainder

	if statsAfter[1].SoftLimit != expectedAgg {
		t.Errorf("aggregate soft limit: got %d, want %d", statsAfter[1].SoftLimit, expectedAgg)
	}
	if statsAfter[2].SoftLimit != expectedJoin {
		t.Errorf("join soft limit: got %d, want %d", statsAfter[2].SoftLimit, expectedJoin)
	}

	// Verify phases in stats.
	if statsAfter[0].Phase != PhaseComplete {
		t.Errorf("sort phase: got %d, want %d", statsAfter[0].Phase, PhaseComplete)
	}
	if statsAfter[1].Phase != PhaseBuilding {
		t.Errorf("aggregate phase: got %d, want %d", statsAfter[1].Phase, PhaseBuilding)
	}
	if statsAfter[2].Phase != PhaseIdle {
		t.Errorf("join phase: got %d, want %d", statsAfter[2].Phase, PhaseIdle)
	}
}

func TestCoordinatorTwoConcurrentSpill(t *testing.T) {
	// 3 operators: sort and aggregate both spill, join should get freed capacity from both.
	budget := int64(300 << 20) // 300MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctSort := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acctAgg := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	_ = mc.RegisterOperator("join", mon.NewAccount("join"), reservationJoin)
	mc.Finalize()

	statsBefore := mc.Stats()
	sortLimit := statsBefore[0].SoftLimit
	aggLimit := statsBefore[1].SoftLimit
	joinLimit := statsBefore[2].SoftLimit

	// Sort spills.
	acctSort.NotifySpilled()
	statsAfter1 := mc.Stats()

	// Sort at reservation, freed capacity split between aggregate and join.
	freedSort := sortLimit - reservationSort
	perActive := freedSort / 2
	remainder := freedSort % 2
	expectedAgg1 := aggLimit + perActive + remainder
	expectedJoin1 := joinLimit + perActive

	if statsAfter1[0].SoftLimit != reservationSort {
		t.Errorf("after sort spill: sort soft limit: got %d, want %d", statsAfter1[0].SoftLimit, reservationSort)
	}
	if statsAfter1[1].SoftLimit != expectedAgg1 {
		t.Errorf("after sort spill: aggregate soft limit: got %d, want %d", statsAfter1[1].SoftLimit, expectedAgg1)
	}
	if statsAfter1[2].SoftLimit != expectedJoin1 {
		t.Errorf("after sort spill: join soft limit: got %d, want %d", statsAfter1[2].SoftLimit, expectedJoin1)
	}

	// Aggregate also spills.
	acctAgg.NotifySpilled()
	statsAfter2 := mc.Stats()

	// Aggregate at reservation, join gets all remaining freed capacity.
	freedAgg := expectedAgg1 - reservationAggregate
	expectedJoin2 := expectedJoin1 + freedAgg

	if statsAfter2[0].SoftLimit != reservationSort {
		t.Errorf("after both spill: sort soft limit: got %d, want %d", statsAfter2[0].SoftLimit, reservationSort)
	}
	if statsAfter2[1].SoftLimit != reservationAggregate {
		t.Errorf("after both spill: aggregate soft limit: got %d, want %d", statsAfter2[1].SoftLimit, reservationAggregate)
	}
	if statsAfter2[2].SoftLimit != expectedJoin2 {
		t.Errorf("after both spill: join soft limit: got %d, want %d", statsAfter2[2].SoftLimit, expectedJoin2)
	}

	// Neither sort nor aggregate dropped below their reservations.
	if statsAfter2[0].SoftLimit < reservationSort {
		t.Error("sort dropped below reservation")
	}
	if statsAfter2[1].SoftLimit < reservationAggregate {
		t.Error("aggregate dropped below reservation")
	}
}

func TestCoordinatorPhaseAwareReclamation(t *testing.T) {
	// Sort completes → freed memory flows to aggregate → aggregate completes → join gets all.
	budget := int64(300 << 20) // 300MB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget)
	acctSort := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	acctAgg := mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	_ = mc.RegisterOperator("join", mon.NewAccount("join"), reservationJoin)
	mc.Finalize()

	statsBefore := mc.Stats()
	sortLimit := statsBefore[0].SoftLimit
	aggLimit := statsBefore[1].SoftLimit
	joinLimit := statsBefore[2].SoftLimit

	// Sort completes — freed capacity split between aggregate and join.
	acctSort.SetPhase(PhaseComplete)
	stats1 := mc.Stats()

	if stats1[0].SoftLimit != 0 {
		t.Errorf("after sort complete: sort soft limit: got %d, want 0", stats1[0].SoftLimit)
	}

	// Aggregate and join should have received sort's capacity.
	perActive := sortLimit / 2
	remainder := sortLimit % 2
	expectedAgg := aggLimit + perActive + remainder
	expectedJoin := joinLimit + perActive

	if stats1[1].SoftLimit != expectedAgg {
		t.Errorf("after sort complete: aggregate soft limit: got %d, want %d", stats1[1].SoftLimit, expectedAgg)
	}
	if stats1[2].SoftLimit != expectedJoin {
		t.Errorf("after sort complete: join soft limit: got %d, want %d", stats1[2].SoftLimit, expectedJoin)
	}

	// Aggregate completes — all its capacity goes to join (only active operator left).
	acctAgg.SetPhase(PhaseComplete)
	stats2 := mc.Stats()

	if stats2[1].SoftLimit != 0 {
		t.Errorf("after aggregate complete: aggregate soft limit: got %d, want 0", stats2[1].SoftLimit)
	}

	// Join should get everything.
	expectedJoinFinal := expectedJoin + expectedAgg
	if stats2[2].SoftLimit != expectedJoinFinal {
		t.Errorf("after aggregate complete: join soft limit: got %d, want %d", stats2[2].SoftLimit, expectedJoinFinal)
	}
}

func TestCoordinatorMinReservationGuarantee(t *testing.T) {
	// Budget insufficient for all reservations — each operator should still get at least its reservation.
	budget := int64(100) // Extremely small budget
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("test", budget*1000) // inner limit generous
	_ = mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	_ = mc.RegisterOperator("join", mon.NewAccount("join"), reservationJoin)
	mc.Finalize()

	s := mc.Stats()

	// Even with tiny budget, operators get at least their reservations.
	if s[0].SoftLimit < reservationSort {
		t.Errorf("sort got less than reservation: %d < %d", s[0].SoftLimit, reservationSort)
	}
	if s[1].SoftLimit < reservationAggregate {
		t.Errorf("aggregate got less than reservation: %d < %d", s[1].SoftLimit, reservationAggregate)
	}
	if s[2].SoftLimit < reservationJoin {
		t.Errorf("join got less than reservation: %d < %d", s[2].SoftLimit, reservationJoin)
	}
}

func BenchmarkCoordinatedAccountGrow(b *testing.B) {
	budget := int64(1 << 30) // 1GB
	mc := NewMemoryCoordinator(budget, 0.10)

	mon := memgov.NewTestBudget("bench", budget)
	acct := mc.RegisterOperator("sort", mon.NewAccount("sort"), reservationSort)
	_ = mc.RegisterOperator("aggregate", mon.NewAccount("aggregate"), reservationAggregate)
	mc.Finalize()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := acct.Grow(64); err != nil {
			// Reset to avoid hitting the limit.
			acct.Shrink(acct.Used())
		}
	}
}

func BenchmarkAccountAdapterGrow(b *testing.B) {
	// Baseline: plain AccountAdapter without coordinator.
	budget := int64(1 << 30) // 1GB
	mon := memgov.NewTestBudget("bench", budget)
	acct := mon.NewAccount("sort")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := acct.Grow(64); err != nil {
			acct.Shrink(acct.Used())
		}
	}
}
