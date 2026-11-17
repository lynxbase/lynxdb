package query

import (
	"context"
	"errors"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/query"
)

func split(t *testing.T, q string) *IRDistributedPlan {
	t.Helper()
	c, err := query.Compile(q)
	if err != nil {
		t.Fatalf("compile %q: %v", q, err)
	}
	dp, err := PlanDistributedQueryIR(c.Plan)
	if err != nil {
		t.Fatalf("split %q: %v", q, err)
	}
	return dp
}

func TestPlanDistributedQueryIR_NilPlan(t *testing.T) {
	dp, err := PlanDistributedQueryIR(nil)
	if err != nil || dp.Strategy != MergeConcat {
		t.Fatalf("got %+v, %v; want concat", dp, err)
	}
}

func TestPlanDistributedQueryIR_PartialAggregate(t *testing.T) {
	dp := split(t, "source=main | where status >= 500 | stats count() as c, avg(latency) as a by host")
	if dp.Strategy != MergePartialAgg {
		t.Fatalf("strategy = %s, want partial_agg", dp.Strategy)
	}
	spec := dp.PartialAggSpec
	if len(spec.GroupBy) != 1 || spec.GroupBy[0] != "host" {
		t.Errorf("group by = %v", spec.GroupBy)
	}
	if len(spec.Funcs) != 2 || spec.Funcs[0].Name != "count" || spec.Funcs[1].Name != "avg" || spec.Funcs[1].Field != "latency" {
		t.Errorf("funcs = %+v", spec.Funcs)
	}
	if len(dp.CoordNodes) != 1 {
		t.Errorf("coordinator nodes = %d, want the output projection only", len(dp.CoordNodes))
	}
}

func TestPlanDistributedQueryIR_TopK(t *testing.T) {
	dp := split(t, "source=main | stats count() as c by host | sort - c | head 3")
	if dp.Strategy != MergeTopK || dp.TopK != 3 {
		t.Fatalf("strategy = %s, topk = %d; want topk 3", dp.Strategy, dp.TopK)
	}
	if len(dp.TopKSortFields) != 1 || dp.TopKSortFields[0].Name != "c" || !dp.TopKSortFields[0].Desc {
		t.Errorf("sort fields = %+v", dp.TopKSortFields)
	}
}

func TestPlanDistributedQueryIR_UnmergeableAggregateRunsOnCoordinator(t *testing.T) {
	dp := split(t, "source=main | stats dc(user) by host")
	if dp.Strategy != MergeConcat || len(dp.CoordNodes) != 2 {
		t.Fatalf("strategy = %s, coordinator nodes = %d; want concat with the aggregate and projection", dp.Strategy, len(dp.CoordNodes))
	}
}

func TestPlanDistributedQueryIR_JoinIsNotDistributable(t *testing.T) {
	c, err := query.Compile("source=a | join left = l right = r ON l.id = r.id b")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if _, err := PlanDistributedQueryIR(c.Plan); !errors.Is(err, ErrNotDistributable) {
		t.Fatalf("err = %v, want ErrNotDistributable", err)
	}
}

func TestApplyCoordCommands_SortAndLimit(t *testing.T) {
	dp := split(t, "source=main | sort - count | head 3")
	if dp.Strategy != MergeConcat || len(dp.CoordNodes) == 0 {
		t.Fatalf("strategy = %s, coordinator nodes = %d", dp.Strategy, len(dp.CoordNodes))
	}
	var rows []map[string]event.Value
	for _, n := range []int64{10, 30, 20, 5, 40} {
		rows = append(rows, map[string]event.Value{"count": event.IntValue(n)})
	}
	out, err := applyCoordCommands(context.Background(), rows, dp.CoordNodes)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	want := []int64{40, 30, 20}
	if len(out) != len(want) {
		t.Fatalf("rows = %d, want %d", len(out), len(want))
	}
	for i, w := range want {
		if got := out[i]["count"].AsInt(); got != w {
			t.Errorf("row %d count = %d, want %d", i, got, w)
		}
	}
}

func TestApplyCoordCommands_NoNodes(t *testing.T) {
	rows := []map[string]event.Value{{"a": event.IntValue(1)}}
	out, err := applyCoordCommands(context.Background(), rows, nil)
	if err != nil || len(out) != 1 {
		t.Fatalf("got %d rows, %v", len(out), err)
	}
}
