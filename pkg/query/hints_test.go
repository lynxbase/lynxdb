package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/model"
)

func firstScan(n ir.Node) *ir.Scan {
	var found *ir.Scan
	ir.WalkPlan(n, func(x ir.Node) bool {
		if s, ok := x.(*ir.Scan); ok && found == nil {
			found = s
		}
		return found == nil
	})
	return found
}

func TestScanHintsToQueryHints(t *testing.T) {
	now := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	c, err := Compile(`search source=web-* "connection refused" (error OR fatal) host="db-1" earliest=-1h | fields host | head 5`)
	if err != nil {
		t.Fatal(err)
	}
	q := physical.ScanHints(firstScan(c.Plan.Root), now).QueryHints()
	if !reflect.DeepEqual(q.SearchTerms, []string{"connection", "refused"}) {
		t.Errorf("search terms = %v", q.SearchTerms)
	}
	if q.SearchTermTree == nil || len(q.SearchTermTree.Children) != 3 || q.SearchTermTree.Children[2].Op != "OR" {
		t.Errorf("term tree = %+v", q.SearchTermTree)
	}
	if len(q.FieldPredicates) != 1 || q.FieldPredicates[0] != (model.FieldPredicate{Field: "host", Op: "=", Value: "db-1"}) {
		t.Errorf("field predicates = %+v", q.FieldPredicates)
	}
	if q.TimeBounds == nil || !q.TimeBounds.Earliest.Equal(now.Add(-time.Hour)) || !q.TimeBounds.Latest.IsZero() {
		t.Errorf("time bounds = %+v", q.TimeBounds)
	}
	if q.SourceScopeType != model.SourceScopeGlob || q.SourceGlob != "web-*" {
		t.Errorf("scope = %s %s", q.SourceScopeType, q.SourceGlob)
	}
	if q.Limit != 0 {
		t.Errorf("a limit above a filter must not reach the scan, got %d", q.Limit)
	}
	if !reflect.DeepEqual(q.RequiredCols, []string{"_raw", "_time", "host", "index"}) {
		t.Errorf("required columns = %v", q.RequiredCols)
	}
}
