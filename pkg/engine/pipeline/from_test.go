package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func spl2Parse(input string) (*spl2.Query, error) {
	return spl2.Parse(input)
}

// mockViewResolver implements ViewResolver for testing.
type mockViewResolver struct {
	views map[string][]*event.Event
}

func (m *mockViewResolver) ResolveView(name string) ([]*event.Event, error) {
	events, ok := m.views[name]
	if !ok {
		return nil, fmt.Errorf("view not found: %s", name)
	}

	return events, nil
}

func makeFromTestEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := 0; i < n; i++ {
		e := event.NewEvent(time.Now().Add(time.Duration(i)*time.Second), fmt.Sprintf("line %d", i))
		e.Source = "nginx"
		e.Index = "mv_test"
		e.SetField("uri", event.StringValue(fmt.Sprintf("/api/%d", i)))
		e.SetField("status", event.IntValue(200))
		events[i] = e
	}

	return events
}

func TestFromIterator_Init_ViewNotFound(t *testing.T) {
	resolver := &mockViewResolver{views: map[string][]*event.Event{}}
	iter := NewFromIterator("nonexistent", resolver, 100)
	err := iter.Init(context.Background())
	if err == nil {
		t.Error("expected error for unknown view")
	}
}

func TestFromIterator_Next_EmptyView(t *testing.T) {
	resolver := &mockViewResolver{
		views: map[string][]*event.Event{
			"mv_empty": {},
		},
	}
	iter := NewFromIterator("mv_empty", resolver, 100)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	batch, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch != nil {
		t.Errorf("expected nil batch for empty view, got %d rows", batch.Len)
	}
}

func TestFromIterator_Next_FromMemTable(t *testing.T) {
	events := makeFromTestEvents(5)
	resolver := &mockViewResolver{
		views: map[string][]*event.Event{
			"mv_test": events,
		},
	}
	iter := NewFromIterator("mv_test", resolver, 100)
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}

	batch, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch == nil {
		t.Fatal("expected batch, got nil")
	}
	if batch.Len != 5 {
		t.Errorf("Len: got %d, want 5", batch.Len)
	}

	// Verify exhaustion.
	batch2, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Next2: %v", err)
	}
	if batch2 != nil {
		t.Error("expected nil after exhaustion")
	}
}

func TestFromIterator_Schema(t *testing.T) {
	resolver := &mockViewResolver{views: map[string][]*event.Event{"mv_test": {}}}
	iter := NewFromIterator("mv_test", resolver, 100)
	schema := iter.Schema()
	if schema != nil {
		t.Errorf("expected nil schema, got %v", schema)
	}
}

func TestFromIterator_Pipeline_WithWhere(t *testing.T) {
	events := makeFromTestEvents(10)
	// Set different sources on some events.
	for i := 0; i < 5; i++ {
		events[i].Source = "nginx"
	}
	for i := 5; i < 10; i++ {
		events[i].Source = "api"
	}

	resolver := &mockViewResolver{
		views: map[string][]*event.Event{
			"mv_test": events,
		},
	}

	// Parse: | from mv_test | where _source="nginx"
	query, err := spl2Parse(`| from mv_test | where _source="nginx"`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	iter, err := BuildPipelineWithViews(context.Background(), query, nil, resolver, 100)
	if err != nil {
		t.Fatalf("BuildPipelineWithViews: %v", err)
	}
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer iter.Close()

	var total int
	for {
		batch, err := iter.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		total += batch.Len
	}

	if total != 5 {
		t.Errorf("filtered count: got %d, want 5", total)
	}
}

// mockViewManager implements ViewManager for testing.
type mockViewManager struct {
	created []string
	dropped []string
	views   []ViewInfo
}

func (m *mockViewManager) CreateView(name, query, retention string) error {
	m.created = append(m.created, name)

	return nil
}

func (m *mockViewManager) ListViews() []ViewInfo {
	return m.views
}

func (m *mockViewManager) DropView(name string) error {
	m.dropped = append(m.dropped, name)

	return nil
}

func TestMaterializeIterator(t *testing.T) {
	mgr := &mockViewManager{}
	iter := NewMaterializeIterator("mv_test", "", "", mgr)
	ctx := context.Background()

	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if len(mgr.created) != 1 || mgr.created[0] != "mv_test" {
		t.Errorf("expected create call for mv_test, got %v", mgr.created)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch == nil || batch.Len != 1 {
		t.Fatalf("expected 1 status row, got %v", batch)
	}
	row := batch.Row(0)
	if v := row["status"].AsString(); v != "created" {
		t.Errorf("status: got %q, want 'created'", v)
	}

	// Second Next should be nil.
	batch2, _ := iter.Next(ctx)
	if batch2 != nil {
		t.Error("expected nil after first batch")
	}
}

func TestViewsIterator(t *testing.T) {
	mgr := &mockViewManager{
		views: []ViewInfo{
			{Name: "mv_a", Status: "active", Query: "search index=main", Type: "projection", CreatedAt: "2024-01-01T00:00:00Z"},
			{Name: "mv_b", Status: "active", Query: "search index=web", Type: "projection", CreatedAt: "2024-01-02T00:00:00Z"},
		},
	}
	iter := NewViewsIterator(mgr, 100)
	ctx := context.Background()

	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch == nil || batch.Len != 2 {
		t.Fatalf("expected 2 view rows, got %v", batch)
	}

	row0 := batch.Row(0)
	if v := row0["name"].AsString(); v != "mv_a" {
		t.Errorf("row0 name: got %q, want 'mv_a'", v)
	}
}

func TestDropviewIterator(t *testing.T) {
	mgr := &mockViewManager{}
	iter := NewDropviewIterator("mv_test", mgr)
	ctx := context.Background()

	if err := iter.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if len(mgr.dropped) != 1 || mgr.dropped[0] != "mv_test" {
		t.Errorf("expected drop call for mv_test, got %v", mgr.dropped)
	}

	batch, err := iter.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if batch == nil || batch.Len != 1 {
		t.Fatalf("expected 1 status row, got %v", batch)
	}
	row := batch.Row(0)
	if v := row["status"].AsString(); v != "dropped" {
		t.Errorf("status: got %q, want 'dropped'", v)
	}
}

func TestFromIterator_Pipeline_WithHead(t *testing.T) {
	events := makeFromTestEvents(20)
	resolver := &mockViewResolver{
		views: map[string][]*event.Event{
			"mv_test": events,
		},
	}

	query, err := spl2Parse(`| from mv_test | head 5`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	iter, err := BuildPipelineWithViews(context.Background(), query, nil, resolver, 100)
	if err != nil {
		t.Fatalf("BuildPipelineWithViews: %v", err)
	}
	if err := iter.Init(context.Background()); err != nil {
		t.Fatalf("Init: %v", err)
	}
	defer iter.Close()

	var total int
	for {
		batch, err := iter.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if batch == nil {
			break
		}
		total += batch.Len
	}

	if total != 5 {
		t.Errorf("head count: got %d, want 5", total)
	}
}
