//go:build e2e

package e2e

import (
	"testing"
)

// TestE2E_SourceDebug_DirectNormalizedQuery sends the already-normalized query
// directly to check if the issue is in normalization or pipeline execution.
func TestE2E_SourceDebug_DirectNormalizedQuery(t *testing.T) {
	h := setupMultiSource(t)

	// These queries are already in normalized form (start with FROM).
	// NormalizeQuery will not modify them.
	tests := []struct {
		name  string
		query string
		want  int
	}{
		// Baseline: FROM * without filter returns all events
		{"FROM * all", `FROM * | STATS count`, 18},
		// FROM specific index
		{"FROM nginx", `FROM nginx | STATS count`, 10},
		// FROM * with _source!= (this is what index!=redis normalizes to)
		{"FROM * where _source!=redis", `FROM * | where _source!="redis" | STATS count`, 15},
		// FROM * with _source= (this is what source=nginx normalizes to)
		{"FROM * where _source=nginx", `FROM * | where _source="nginx" | STATS count`, 10},
		// Try using source field name directly
		{"FROM * where source=nginx", `FROM * | where source="nginx" | STATS count`, 10},
		// Exact same predicate in SEARCH instead of WHERE
		{"FROM * search source=nginx", `FROM * | search _source=nginx | STATS count`, 10},
		// Check: does WHERE index="nginx" also fail?
		{"FROM * where index=nginx", `FROM * | where index="nginx" | STATS count`, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := h.MustQuery(tt.query)
			got := GetInt(r, "count")
			if got != tt.want {
				t.Errorf("query %q: got count=%d, want %d", tt.query, got, tt.want)
			}
		})
	}
}
