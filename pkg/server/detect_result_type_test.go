package server

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func TestDetectResultType(t *testing.T) {
	tests := []struct {
		name     string
		commands []spl2.Command
		want     ResultType
	}{
		{
			name:     "empty_pipeline_returns_events",
			commands: nil,
			want:     ResultTypeEvents,
		},
		{
			name:     "stats_returns_aggregate",
			commands: []spl2.Command{&spl2.StatsCommand{}},
			want:     ResultTypeAggregate,
		},
		{
			name:     "top_returns_aggregate",
			commands: []spl2.Command{&spl2.TopCommand{Field: "level"}},
			want:     ResultTypeAggregate,
		},
		{
			name:     "rare_returns_aggregate",
			commands: []spl2.Command{&spl2.RareCommand{Field: "level"}},
			want:     ResultTypeAggregate,
		},
		{
			name:     "timechart_returns_timechart",
			commands: []spl2.Command{&spl2.TimechartCommand{}},
			want:     ResultTypeTimechart,
		},
		{
			name:     "xyseries_returns_aggregate",
			commands: []spl2.Command{&spl2.XYSeriesCommand{}},
			want:     ResultTypeAggregate,
		},
		{
			name: "eventstats_returns_events",
			commands: []spl2.Command{
				&spl2.EventstatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "total"}},
					GroupBy:      []string{"group"},
				},
			},
			want: ResultTypeEvents,
		},
		{
			name: "eventstats_with_trailing_head_returns_events",
			commands: []spl2.Command{
				&spl2.EventstatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "total"}},
				},
				&spl2.HeadCommand{Count: 10},
			},
			want: ResultTypeEvents,
		},
		{
			name: "stats_with_trailing_sort_returns_aggregate",
			commands: []spl2.Command{
				&spl2.StatsCommand{
					Aggregations: []spl2.AggExpr{{Func: "count", Alias: "count"}},
					GroupBy:      []string{"level"},
				},
				&spl2.SortCommand{Fields: []spl2.SortField{{Name: "count"}}},
			},
			want: ResultTypeAggregate,
		},
		{
			name: "where_returns_events",
			commands: []spl2.Command{
				&spl2.WhereCommand{},
			},
			want: ResultTypeEvents,
		},
		{
			name: "head_only_returns_events",
			commands: []spl2.Command{
				&spl2.HeadCommand{Count: 5},
			},
			want: ResultTypeEvents,
		},
		{
			name: "streamstats_returns_events",
			commands: []spl2.Command{
				&spl2.StreamstatsCommand{},
			},
			want: ResultTypeEvents,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prog := &spl2.Program{
				Main: &spl2.Query{Commands: tt.commands},
			}
			got := DetectResultType(prog)
			if got != tt.want {
				t.Errorf("DetectResultType() = %q, want %q", got, tt.want)
			}
		})
	}
}
