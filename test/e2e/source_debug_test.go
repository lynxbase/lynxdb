//go:build e2e

package e2e

import (
	"testing"
)

// TestE2E_SourceDebug_DirectNormalizedQuery tests various query forms for
// multi-source filtering.
func TestE2E_SourceDebug_DirectNormalizedQuery(t *testing.T) {
	h := setupMultiSource(t)

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{"all indexes", `source=* | stats count() as count`, 18},
		{"single index", `source=nginx | stats count() as count`, 10},
		{"index list", `source=nginx, redis | stats count() as count`, 13},
		{"search single index", `search source=nginx | stats count() as count`, 10},
		{"where source!=redis", `source=* | where source != "redis" | stats count() as count`, 15},
		{"where source=nginx", `source=* | where source = "nginx" | stats count() as count`, 10},
		{"search sourcetype=raw", `search source=* sourcetype=raw | stats count() as count`, 18},
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
