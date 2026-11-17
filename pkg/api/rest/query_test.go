package rest

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/query"
)

func TestDetectResultType(t *testing.T) {
	tests := []struct {
		query string
		want  ResultType
	}{
		{"source=main", ResultTypeEvents},
		{"source=main | where status >= 500 | head 5", ResultTypeEvents},
		{"source=main | stats count() by host", ResultTypeAggregate},
		{"source=main | stats count() by host | sort - `count()` | head 3", ResultTypeAggregate},
		{"source=main | stats count() as c by host | where c > 1 | fields host", ResultTypeAggregate},
		{"source=main | top 3 host", ResultTypeAggregate},
		{"source=main | eventstats count() by host", ResultTypeEvents},
		{"describe main", ResultTypeAggregate},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			c, err := query.Compile(tt.query)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			if got := DetectResultType(c.Plan); got != tt.want {
				t.Errorf("DetectResultType() = %q, want %q", got, tt.want)
			}
		})
	}
	if got := DetectResultType(nil); got != ResultTypeEvents {
		t.Errorf("DetectResultType(nil) = %q, want events", got)
	}
}
