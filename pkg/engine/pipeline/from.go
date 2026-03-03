package pipeline

import (
	"context"
	"fmt"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// ViewResolver provides access to materialized view data.
type ViewResolver interface {
	// ResolveView returns the events for the given view name.
	// Events come from both flushed segments and the current memtable.
	ResolveView(name string) ([]*event.Event, error)
}

// ViewInfo holds view metadata returned by ViewManager.ListViews.
type ViewInfo struct {
	Name      string
	Status    string
	Query     string
	Type      string
	CreatedAt string
}

// ViewManager provides materialized view lifecycle operations for pipeline execution.
type ViewManager interface {
	CreateView(name, query, retention string) error
	ListViews() []ViewInfo
	DropView(name string) error
}

// FromIterator reads data from a materialized view via ViewResolver.
type FromIterator struct {
	viewName  string
	resolver  ViewResolver
	batchSize int
	events    []*event.Event
	offset    int
	inited    bool
}

// NewFromIterator creates a new FromIterator for reading from a materialized view.
func NewFromIterator(viewName string, resolver ViewResolver, batchSize int) *FromIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &FromIterator{
		viewName:  viewName,
		resolver:  resolver,
		batchSize: batchSize,
	}
}

func (f *FromIterator) Init(ctx context.Context) error {
	if f.resolver == nil {
		return fmt.Errorf("from: no view resolver configured")
	}
	events, err := f.resolver.ResolveView(f.viewName)
	if err != nil {
		return fmt.Errorf("from: resolve view %q: %w", f.viewName, err)
	}
	f.events = events
	f.inited = true

	return nil
}

func (f *FromIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !f.inited {
		return nil, fmt.Errorf("from: not initialized")
	}
	if f.offset >= len(f.events) {
		return nil, nil
	}

	end := f.offset + f.batchSize
	if end > len(f.events) {
		end = len(f.events)
	}

	slice := f.events[f.offset:end]
	f.offset = end

	return BatchFromEvents(slice), nil
}

func (f *FromIterator) Close() error        { return nil }
func (f *FromIterator) Schema() []FieldInfo { return nil }

// MaterializeIterator creates a materialized view and returns a status row.
type MaterializeIterator struct {
	name      string
	query     string
	retention string
	manager   ViewManager
	done      bool
}

func NewMaterializeIterator(name, query, retention string, mgr ViewManager) *MaterializeIterator {
	return &MaterializeIterator{name: name, query: query, retention: retention, manager: mgr}
}

func (m *MaterializeIterator) Init(ctx context.Context) error {
	if m.manager == nil {
		return fmt.Errorf("materialize: no view manager configured")
	}

	return m.manager.CreateView(m.name, m.query, m.retention)
}

func (m *MaterializeIterator) Next(ctx context.Context) (*Batch, error) {
	if m.done {
		return nil, nil
	}
	m.done = true
	row := map[string]event.Value{
		"name":   event.StringValue(m.name),
		"status": event.StringValue("created"),
	}

	return BatchFromRows([]map[string]event.Value{row}), nil
}

func (m *MaterializeIterator) Close() error        { return nil }
func (m *MaterializeIterator) Schema() []FieldInfo { return nil }

// ViewsIterator lists all materialized views as result rows.
type ViewsIterator struct {
	manager ViewManager
	rows    []map[string]event.Value
	offset  int
	inited  bool
}

func NewViewsIterator(mgr ViewManager, batchSize int) *ViewsIterator {
	return &ViewsIterator{manager: mgr}
}

func (v *ViewsIterator) Init(ctx context.Context) error {
	if v.manager == nil {
		return fmt.Errorf("views: no view manager configured")
	}
	views := v.manager.ListViews()
	v.rows = make([]map[string]event.Value, len(views))
	for i, vi := range views {
		v.rows[i] = map[string]event.Value{
			"name":       event.StringValue(vi.Name),
			"status":     event.StringValue(vi.Status),
			"query":      event.StringValue(vi.Query),
			"type":       event.StringValue(vi.Type),
			"created_at": event.StringValue(vi.CreatedAt),
		}
	}
	v.inited = true

	return nil
}

func (v *ViewsIterator) Next(ctx context.Context) (*Batch, error) {
	if !v.inited {
		return nil, fmt.Errorf("views: not initialized")
	}
	if v.offset >= len(v.rows) {
		return nil, nil
	}
	batch := BatchFromRows(v.rows[v.offset:])
	v.offset = len(v.rows)

	return batch, nil
}

func (v *ViewsIterator) Close() error        { return nil }
func (v *ViewsIterator) Schema() []FieldInfo { return nil }

// DropviewIterator drops a materialized view and returns a status row.
type DropviewIterator struct {
	name    string
	manager ViewManager
	done    bool
}

func NewDropviewIterator(name string, mgr ViewManager) *DropviewIterator {
	return &DropviewIterator{name: name, manager: mgr}
}

func (d *DropviewIterator) Init(ctx context.Context) error {
	if d.manager == nil {
		return fmt.Errorf("dropview: no view manager configured")
	}

	return d.manager.DropView(d.name)
}

func (d *DropviewIterator) Next(ctx context.Context) (*Batch, error) {
	if d.done {
		return nil, nil
	}
	d.done = true
	row := map[string]event.Value{
		"name":   event.StringValue(d.name),
		"status": event.StringValue("dropped"),
	}

	return BatchFromRows([]map[string]event.Value{row}), nil
}

func (d *DropviewIterator) Close() error        { return nil }
func (d *DropviewIterator) Schema() []FieldInfo { return nil }
