package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
)

func TestPlanHints(t *testing.T) {
	now := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	c, err := Compile(`search source=web-* "connection refused" (error OR fatal) host="db-1" earliest=-1h | fields host | head 5`)
	if err != nil {
		t.Fatal(err)
	}
	q := physical.PlanHints(c.Plan, "main", now)
	if !reflect.DeepEqual(q.SearchTerms, []string{"connection", "refused"}) {
		t.Errorf("search terms = %v", q.SearchTerms)
	}
	if len(q.FieldPredicates) != 1 || q.FieldPredicates[0] != (physical.FieldEqual{Field: "host", Value: "db-1"}) {
		t.Errorf("field predicates = %+v", q.FieldPredicates)
	}
	if q.TimeBounds == nil || !q.TimeBounds.Earliest.Equal(now.Add(-time.Hour)) || !q.TimeBounds.Latest.IsZero() {
		t.Errorf("time bounds = %+v", q.TimeBounds)
	}
	if q.SourceScopeType != "glob" || q.SourceScopePattern != "web-*" {
		t.Errorf("scope = %s %s", q.SourceScopeType, q.SourceScopePattern)
	}
	// A search of free text reads every field.
	if len(q.RequiredCols) != 0 {
		t.Errorf("required columns = %v", q.RequiredCols)
	}
}
