package planner

import (
	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/storage/views"
)

// ViewLister provides a list of materialized views. Server.Engine satisfies this.
type ViewLister interface {
	ListMV() []views.ViewDefinition
}

// viewCatalogAdapter adapts ViewLister to optimizer.ViewCatalog.
type viewCatalogAdapter struct {
	lister ViewLister
}

// NewViewCatalog wraps a ViewLister as an optimizer.ViewCatalog.
func NewViewCatalog(lister ViewLister) optimizer.ViewCatalog {
	return &viewCatalogAdapter{lister: lister}
}

func (a *viewCatalogAdapter) ListViews() []optimizer.ViewInfo {
	defs := a.lister.ListMV()
	infos := make([]optimizer.ViewInfo, len(defs))
	for i, d := range defs {
		aggs := make([]string, len(d.Aggregations))
		for j, agg := range d.Aggregations {
			aggs[j] = agg.Type
		}
		infos[i] = optimizer.ViewInfo{
			Name:         d.Name,
			Filter:       d.Filter,
			GroupBy:      d.GroupBy,
			Aggregations: aggs,
			Status:       string(d.Status),
		}
	}

	return infos
}
