package query

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestPlanDistributedJoin_NilCommand(t *testing.T) {
	got := PlanDistributedJoin(nil, nil, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for nil, got %v", got)
	}
}

func TestPlanDistributedJoin_NilSubquery(t *testing.T) {
	got := PlanDistributedJoin(&spl2.JoinCommand{}, nil, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for nil subquery, got %v", got)
	}
}

func TestPlanDistributedJoin_SmallHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.HeadCommand{Count: 100},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil, nil)
	if got != JoinBroadcast {
		t.Errorf("expected JoinBroadcast for small head, got %v", got)
	}
}

func TestPlanDistributedJoin_LargeHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.HeadCommand{Count: 20000},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for large head, got %v", got)
	}
}

func TestPlanDistributedJoin_NoHead(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Subquery: &spl2.Query{
			Commands: []spl2.Command{
				&spl2.SearchCommand{},
				&spl2.WhereCommand{},
			},
		},
	}

	got := PlanDistributedJoin(cmd, nil, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for no head, got %v", got)
	}
}

func TestPlanDistributedJoin_Colocated_SameIndex_SourceField(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Field: "source",
		Subquery: &spl2.Query{
			Source:   &spl2.SourceClause{Index: "web_logs"},
			Commands: []spl2.Command{&spl2.SearchCommand{}},
		},
	}
	leftSource := &spl2.SourceClause{Index: "web_logs"}

	got := PlanDistributedJoin(cmd, leftSource, nil)
	if got != JoinColocated {
		t.Errorf("expected JoinColocated for same index + source field, got %v", got)
	}
}

func TestPlanDistributedJoin_Colocated_SameIndex_HostField(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Field: "host",
		Subquery: &spl2.Query{
			Source:   &spl2.SourceClause{Index: "web_logs"},
			Commands: []spl2.Command{&spl2.SearchCommand{}},
		},
	}
	leftSource := &spl2.SourceClause{Index: "web_logs"}

	got := PlanDistributedJoin(cmd, leftSource, nil)
	if got != JoinColocated {
		t.Errorf("expected JoinColocated for same index + host field, got %v", got)
	}
}

func TestPlanDistributedJoin_NotColocated_DifferentIndex(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Field: "source",
		Subquery: &spl2.Query{
			Source:   &spl2.SourceClause{Index: "audit_logs"},
			Commands: []spl2.Command{&spl2.SearchCommand{}},
		},
	}
	leftSource := &spl2.SourceClause{Index: "web_logs"}

	got := PlanDistributedJoin(cmd, leftSource, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for different indexes, got %v", got)
	}
}

func TestPlanDistributedJoin_NotColocated_NonShardField(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Field: "user_id",
		Subquery: &spl2.Query{
			Source:   &spl2.SourceClause{Index: "web_logs"},
			Commands: []spl2.Command{&spl2.SearchCommand{}},
		},
	}
	leftSource := &spl2.SourceClause{Index: "web_logs"}

	got := PlanDistributedJoin(cmd, leftSource, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for non-shard field, got %v", got)
	}
}

func TestPlanDistributedJoin_NotColocated_GlobSource(t *testing.T) {
	cmd := &spl2.JoinCommand{
		Field: "source",
		Subquery: &spl2.Query{
			Source:   &spl2.SourceClause{Index: "web_logs"},
			Commands: []spl2.Command{&spl2.SearchCommand{}},
		},
	}
	leftSource := &spl2.SourceClause{Index: "web_*", IsGlob: true}

	got := PlanDistributedJoin(cmd, leftSource, nil)
	if got != JoinCoordinator {
		t.Errorf("expected JoinCoordinator for glob source, got %v", got)
	}
}

func TestJoinStrategy_String(t *testing.T) {
	tests := []struct {
		s    JoinStrategy
		want string
	}{
		{JoinBroadcast, "broadcast"},
		{JoinColocated, "colocated"},
		{JoinCoordinator, "coordinator"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("JoinStrategy(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}
