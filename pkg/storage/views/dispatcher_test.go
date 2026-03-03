package views

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage/segment"
)

func setupDispatcher(t *testing.T) (*Dispatcher, *ViewRegistry, string) {
	t.Helper()
	dir := t.TempDir()
	viewsDir := filepath.Join(dir, "views")
	os.MkdirAll(viewsDir, 0o755)

	reg, err := Open(viewsDir)
	if err != nil {
		t.Fatalf("Open registry: %v", err)
	}

	layout := storage.NewLayout(dir)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	d := NewDispatcher(reg, layout, logger)

	return d, reg, dir
}

func createProjectionView(t *testing.T, reg *ViewRegistry, name, filter string) ViewDefinition {
	t.Helper()
	def := ViewDefinition{
		Name:    name,
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  filter,
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "_source", Type: event.FieldTypeString},
			{Name: "uri", Type: event.FieldTypeString},
			{Name: "status", Type: event.FieldTypeString},
		},
		Status:    ViewStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := reg.Create(def); err != nil {
		t.Fatalf("Create view %s: %v", name, err)
	}

	return def
}

func TestDispatcher_DispatchMatchingEvents(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_errors", "_source=nginx")
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEvent("nginx", "/api/health", "200"),
		makeTestEvent("nginx", "/api/error", "500"),
	}

	if err := d.Dispatch(events); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	memEvents := d.ViewBufferedEvents("mv_errors")
	if len(memEvents) != 2 {
		t.Errorf("memtable events: got %d, want 2", len(memEvents))
	}
}

func TestDispatcher_DispatchNonMatchingEvents(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_errors", "_source=nginx")
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEvent("api", "/v1/users", "200"),
		makeTestEvent("worker", "/jobs", "200"),
	}

	if err := d.Dispatch(events); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	memEvents := d.ViewBufferedEvents("mv_errors")
	if len(memEvents) != 0 {
		t.Errorf("memtable events: got %d, want 0", len(memEvents))
	}
}

func TestDispatcher_FlushView(t *testing.T) {
	d, reg, dir := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_flush", "_source=nginx")
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEvent("nginx", "/api/health", "200"),
	}
	d.Dispatch(events)

	if err := d.FlushView("mv_flush"); err != nil {
		t.Fatalf("FlushView: %v", err)
	}

	// Verify segment file exists.
	segDir := filepath.Join(dir, "views", "mv_flush", "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Error("no segment files created")
	}
}

func TestDispatcher_FlushView_SegmentReadable(t *testing.T) {
	d, reg, dir := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_read", "_source=nginx")
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEvent("nginx", "/api/a", "200"),
		makeTestEvent("nginx", "/api/b", "500"),
	}
	d.Dispatch(events)
	d.FlushView("mv_read")

	// Find and open the segment.
	segDir := filepath.Join(dir, "views", "mv_read", "segments")
	entries, _ := os.ReadDir(segDir)
	if len(entries) == 0 {
		t.Fatal("no segment files")
	}

	segPath := filepath.Join(segDir, entries[0].Name())
	ms, err := segment.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	r := ms.Reader()
	if r.EventCount() != 2 {
		t.Errorf("EventCount: got %d, want 2", r.EventCount())
	}
}

func TestDispatcher_MultipleViews(t *testing.T) {
	d, reg, _ := setupDispatcher(t)

	def1 := createProjectionView(t, reg, "mv_nginx", "_source=nginx")
	def2 := createProjectionView(t, reg, "mv_api", "_source=api")
	d.ActivateView(def1)
	d.ActivateView(def2)

	events := []*event.Event{
		makeTestEvent("nginx", "/index", "200"),
		makeTestEvent("api", "/v1/users", "200"),
		makeTestEvent("nginx", "/health", "200"),
		makeTestEvent("worker", "/jobs", "200"),
	}
	d.Dispatch(events)

	nginxEvents := d.ViewBufferedEvents("mv_nginx")
	if len(nginxEvents) != 2 {
		t.Errorf("mv_nginx events: got %d, want 2", len(nginxEvents))
	}

	apiEvents := d.ViewBufferedEvents("mv_api")
	if len(apiEvents) != 1 {
		t.Errorf("mv_api events: got %d, want 1", len(apiEvents))
	}
}

func TestDispatcher_EmptyDispatch(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_empty", "")
	d.ActivateView(def)

	if err := d.Dispatch(nil); err != nil {
		t.Errorf("empty dispatch should not error: %v", err)
	}
	if err := d.Dispatch([]*event.Event{}); err != nil {
		t.Errorf("empty slice dispatch should not error: %v", err)
	}
}

func TestDispatcher_StartLoadsViews(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_start", "_source=nginx")
	_ = def

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Dispatch should work since Start loaded the view.
	events := []*event.Event{makeTestEvent("nginx", "/test", "200")}
	d.Dispatch(events)

	memEvents := d.ViewBufferedEvents("mv_start")
	if len(memEvents) != 1 {
		t.Errorf("events after Start: got %d, want 1", len(memEvents))
	}
}

func TestDispatcher_ViewAllEvents(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_all", "_source=nginx")
	d.ActivateView(def)

	// Dispatch first batch and flush to disk.
	batch1 := []*event.Event{
		makeTestEvent("nginx", "/api/a", "200"),
		makeTestEvent("nginx", "/api/b", "500"),
	}
	d.Dispatch(batch1)
	if err := d.FlushView("mv_all"); err != nil {
		t.Fatalf("FlushView: %v", err)
	}

	// Dispatch second batch (stays in memtable).
	batch2 := []*event.Event{
		makeTestEvent("nginx", "/api/c", "201"),
	}
	d.Dispatch(batch2)

	// ViewAllEvents should return segment + memtable events.
	all, err := d.ViewAllEvents("mv_all")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	if len(all) != 3 {
		t.Errorf("ViewAllEvents: got %d events, want 3", len(all))
	}

	// Memtable-only should return just the unflushed events.
	mem := d.ViewBufferedEvents("mv_all")
	if len(mem) != 1 {
		t.Errorf("ViewBufferedEvents: got %d, want 1", len(mem))
	}
}

func TestDispatcher_ViewAllEvents_NotFound(t *testing.T) {
	d, _, _ := setupDispatcher(t)

	_, err := d.ViewAllEvents("nonexistent")
	if !errors.Is(err, ErrViewNotFound) {
		t.Errorf("expected ErrViewNotFound, got %v", err)
	}
}

func TestDispatcher_SortKeyEnforcement(t *testing.T) {
	d, reg, dir := setupDispatcher(t)

	// Create a view with a sort key.
	def := ViewDefinition{
		Name:    "mv_sorted",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "status", Type: event.FieldTypeString},
		},
		SortKey:   []string{"status"},
		Status:    ViewStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	reg.Create(def)
	d.ActivateView(def)

	// Dispatch events in unsorted order.
	events := []*event.Event{
		makeTestEvent("nginx", "/c", "500"),
		makeTestEvent("nginx", "/a", "200"),
		makeTestEvent("nginx", "/b", "301"),
	}
	d.Dispatch(events)

	if err := d.FlushView("mv_sorted"); err != nil {
		t.Fatalf("FlushView: %v", err)
	}

	// Read back the segment and verify sort order.
	segDir := filepath.Join(dir, "views", "mv_sorted", "segments")
	entries, _ := os.ReadDir(segDir)
	if len(entries) == 0 {
		t.Fatal("no segment files")
	}

	segPath := filepath.Join(segDir, entries[0].Name())
	ms, err := segment.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	r := ms.Reader()
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != 3 {
		t.Fatalf("expected 3 events, got %d", len(readEvents))
	}

	// Events should be sorted by status: 200 < 301 < 500.
	for i := 1; i < len(readEvents); i++ {
		prev := readEvents[i-1].GetField("status").String()
		curr := readEvents[i].GetField("status").String()
		if prev > curr {
			t.Errorf("sort order violated at %d: %q > %q", i, prev, curr)
		}
	}
}

func TestDispatcher_UpdateView_SafeMutation(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_update", "_source=nginx")
	d.ActivateView(def)

	// Update retention (safe mutation, no rebuild).
	updated := def
	updated.Retention = 24 * time.Hour
	if err := d.UpdateView(updated); err != nil {
		t.Fatalf("UpdateView: %v", err)
	}

	stored, err := reg.Get("mv_update")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Retention != 24*time.Hour {
		t.Errorf("retention: got %v, want 24h", stored.Retention)
	}
}

func TestDispatcher_UpdateView_Rebuild(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createProjectionView(t, reg, "mv_rebuild", "_source=nginx")
	d.ActivateView(def)

	// Change filter (triggers rebuild).
	updated := def
	updated.Filter = "_source=api"
	if err := d.UpdateView(updated); err != nil {
		t.Fatalf("UpdateView: %v", err)
	}

	stored, err := reg.Get("mv_rebuild")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.Version != 2 {
		t.Errorf("version: got %d, want 2", stored.Version)
	}
	if stored.Status != ViewStatusBackfill {
		t.Errorf("status: got %v, want backfill", stored.Status)
	}
}

func TestDispatcher_FlushView_PrimaryIndex(t *testing.T) {
	d, reg, dir := setupDispatcher(t)

	def := ViewDefinition{
		Name:    "mv_pidx",
		Version: 1,
		Type:    ViewTypeProjection,
		Filter:  "",
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
			{Name: "status", Type: event.FieldTypeString},
		},
		SortKey:   []string{"status"},
		Status:    ViewStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	reg.Create(def)
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEvent("nginx", "/c", "500"),
		makeTestEvent("nginx", "/a", "200"),
		makeTestEvent("nginx", "/b", "301"),
	}
	d.Dispatch(events)

	if err := d.FlushView("mv_pidx"); err != nil {
		t.Fatalf("FlushView: %v", err)
	}

	// Read segment and verify primary index exists.
	segDir := filepath.Join(dir, "views", "mv_pidx", "segments")
	entries, err := os.ReadDir(segDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no segment files")
	}

	segPath := filepath.Join(segDir, entries[0].Name())
	ms, err := segment.OpenSegmentFile(segPath)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	r := ms.Reader()
	idx, err := r.PrimaryIndex()
	if err != nil {
		t.Fatalf("PrimaryIndex: %v", err)
	}
	if idx == nil {
		t.Fatal("expected primary index in MV segment, got nil")
	}
	if len(idx.SortFields) != 1 || idx.SortFields[0] != "status" {
		t.Errorf("SortFields: got %v, want [status]", idx.SortFields)
	}
	if len(idx.Entries) == 0 {
		t.Error("expected at least one primary index entry")
	}
}

func makeTestEvent(source, uri, status string) *event.Event {
	e := event.NewEvent(time.Now(), "test log line")
	e.Source = source
	e.Index = "main"
	e.SetField("uri", event.StringValue(uri))
	e.SetField("status", event.StringValue(status))

	return e
}

// Aggregation View Tests

func createAggView(t *testing.T, reg *ViewRegistry, name, query string) ViewDefinition {
	t.Helper()

	analysis, err := AnalyzeQuery(query)
	if err != nil {
		t.Fatalf("AnalyzeQuery: %v", err)
	}

	def := ViewDefinition{
		Name:        name,
		Version:     1,
		Type:        ViewTypeAggregation,
		Query:       query,
		SourceIndex: analysis.SourceIndex,
		AggSpec:     analysis.AggSpec,
		GroupBy:     analysis.GroupBy,
		Columns: []ColumnDef{
			{Name: "_time", Type: event.FieldTypeTimestamp},
		},
		Status:    ViewStatusActive,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := reg.Create(def); err != nil {
		t.Fatalf("Create view %s: %v", name, err)
	}

	return def
}

func TestDispatcher_AggregationView_CountByHost(t *testing.T) {
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_count", `FROM main | stats count by host`)
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEventWithHost("main", "web1"),
		makeTestEventWithHost("main", "web2"),
		makeTestEventWithHost("main", "web1"),
		makeTestEventWithHost("main", "web1"),
	}
	if err := d.Dispatch(events); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// ViewAllEvents should finalize: web1=3, web2=1.
	all, err := d.ViewAllEvents("mv_count")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(all))
	}

	byHost := make(map[string]int64)
	for _, ev := range all {
		host := ev.GetField("host").String()
		count := ev.GetField("count").AsInt()
		byHost[host] = count
	}
	if byHost["web1"] != 3 {
		t.Errorf("web1 count: got %d, want 3", byHost["web1"])
	}
	if byHost["web2"] != 1 {
		t.Errorf("web2 count: got %d, want 1", byHost["web2"])
	}
}

func TestDispatcher_AggregationView_AvgMerge(t *testing.T) {
	// Critical test: avg merge must use weighted average (sum/count), not
	// arithmetic mean of means.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_avg", `FROM main | stats avg(duration) by host`)
	d.ActivateView(def)

	// Batch 1: host=web1, durations [10, 20] → sum=30, count=2.
	batch1 := []*event.Event{
		makeTestEventWithDuration("main", "web1", 10),
		makeTestEventWithDuration("main", "web1", 20),
	}
	d.Dispatch(batch1)

	// Flush to disk so batch 2 goes into a separate in-memory batch.
	d.FlushView("mv_avg")

	// Batch 2: host=web1, durations [30] → sum=30, count=1.
	batch2 := []*event.Event{
		makeTestEventWithDuration("main", "web1", 30),
	}
	d.Dispatch(batch2)

	// Correct avg = (10+20+30)/3 = 20.0 (NOT (15+30)/2 = 22.5).
	all, err := d.ViewAllEvents("mv_avg")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 group, got %d", len(all))
	}

	avgVal := all[0].GetField("avg(duration)")
	if avgVal.IsNull() {
		t.Fatal("avg(duration) is null")
	}
	got := avgVal.AsFloat()
	want := 20.0
	if got != want {
		t.Errorf("avg(duration): got %f, want %f (weighted, not mean-of-means)", got, want)
	}
}

func TestDispatcher_AggregationView_SourceFilter(t *testing.T) {
	// Events from wrong index should be filtered out.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_nginx_count", `FROM nginx | stats count by status`)
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEventWithIndex("nginx", "200"),
		makeTestEventWithIndex("nginx", "500"),
		makeTestEventWithIndex("api", "200"),   // wrong index
		makeTestEventWithIndex("other", "500"), // wrong index
	}
	d.Dispatch(events)

	all, err := d.ViewAllEvents("mv_nginx_count")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	// Should only have nginx events: status=200 count=1, status=500 count=1.
	totalCount := int64(0)
	for _, ev := range all {
		totalCount += ev.GetField("count").AsInt()
	}
	if totalCount != 2 {
		t.Errorf("total count: got %d, want 2 (only nginx events)", totalCount)
	}
}

func TestDispatcher_AggregationView_PausedSkipped(t *testing.T) {
	// Bug #4: paused views should not receive events.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_paused", `FROM main | stats count by host`)
	def.Status = ViewStatusPaused
	reg.Update(def)
	d.ActivateView(def)

	events := []*event.Event{makeTestEventWithHost("main", "web1")}
	d.Dispatch(events)

	mem := d.ViewBufferedEvents("mv_paused")
	if len(mem) != 0 {
		t.Errorf("paused view should not receive events, got %d", len(mem))
	}
}

func TestDispatcher_Serialization_Roundtrip(t *testing.T) {
	// Verify that partial state survives serialize → deserialize.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_roundtrip",
		`FROM main | stats count, sum(bytes), avg(duration), min(latency), max(latency) by host`)
	d.ActivateView(def)

	events := []*event.Event{
		makeTestEventFull("main", "web1", 100, 10, 5),
		makeTestEventFull("main", "web1", 200, 20, 15),
		makeTestEventFull("main", "web2", 50, 30, 25),
	}
	d.Dispatch(events)

	all, err := d.ViewAllEvents("mv_roundtrip")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}

	byHost := make(map[string]*event.Event)
	for _, ev := range all {
		byHost[ev.GetField("host").String()] = ev
	}

	web1 := byHost["web1"]
	if web1 == nil {
		t.Fatal("missing web1 group")
	}
	if web1.GetField("count").AsInt() != 2 {
		t.Errorf("web1 count: got %d, want 2", web1.GetField("count").AsInt())
	}
	if web1.GetField("sum(bytes)").AsFloat() != 300 {
		t.Errorf("web1 sum(bytes): got %f, want 300", web1.GetField("sum(bytes)").AsFloat())
	}
	// avg(duration) = (10+20)/2 = 15.
	if web1.GetField("avg(duration)").AsFloat() != 15 {
		t.Errorf("web1 avg(duration): got %f, want 15", web1.GetField("avg(duration)").AsFloat())
	}
}

func TestDispatcher_DeactivationGuard(t *testing.T) {
	// Verify no panic when view is deactivated mid-dispatch.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_deact", `FROM main | stats count by host`)
	d.ActivateView(def)

	// Deactivate immediately.
	d.DeactivateView("mv_deact")

	// Dispatch should not panic.
	events := []*event.Event{makeTestEventWithHost("main", "web1")}
	if err := d.Dispatch(events); err != nil {
		t.Errorf("Dispatch after deactivate: %v", err)
	}
}

func TestDispatcher_AggregationView_DCMerge(t *testing.T) {
	// Verify dc (distinct count) survives serialize → deserialize → merge.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_dc", `FROM main | stats dc(user) by host`)
	d.ActivateView(def)

	// Batch 1: host=web1, users [alice, bob].
	batch1 := []*event.Event{
		makeTestEventWithUser("main", "web1", "alice"),
		makeTestEventWithUser("main", "web1", "bob"),
	}
	d.Dispatch(batch1)

	// Flush to disk so batch 2 creates a separate partial group.
	d.FlushView("mv_dc")

	// Batch 2: host=web1, users [bob, charlie] — bob overlaps.
	batch2 := []*event.Event{
		makeTestEventWithUser("main", "web1", "bob"),
		makeTestEventWithUser("main", "web1", "charlie"),
	}
	d.Dispatch(batch2)

	// After merge: dc = |{alice, bob, charlie}| = 3 (NOT 2+2=4).
	all, err := d.ViewAllEvents("mv_dc")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 group, got %d", len(all))
	}

	dcVal := all[0].GetField("dc(user)")
	if dcVal.IsNull() {
		t.Fatal("dc(user) is null")
	}
	got := dcVal.AsInt()
	if got != 3 {
		t.Errorf("dc(user): got %d, want 3 (union of {alice,bob,charlie})", got)
	}
}

func TestDispatcher_AggregationView_DCBackfillFallback(t *testing.T) {
	// Verify dc works via the Count fallback when DistinctSet is empty.
	// This simulates the backfill path where finalizedResultsToPartialGroups
	// sets Count but not DistinctSet.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_dc_fallback", `FROM main | stats dc(user) by host`)
	d.ActivateView(def)

	// Manually inject a partial state event with dc_count but empty dc_set,
	// simulating what backfill produces.
	fakeEvent := event.NewEvent(time.Now(), "")
	fakeEvent.Index = "mv_dc_fallback"
	fakeEvent.SetField("host", event.StringValue("web1"))
	fakeEvent.SetField("_pa_dc(user)_dc_set", event.StringValue("[]"))
	fakeEvent.SetField("_pa_dc(user)_dc_count", event.IntValue(5))

	// Inject directly into the view's events buffer.
	d.mu.RLock()
	av := d.views["mv_dc_fallback"]
	d.mu.RUnlock()
	av.mu.Lock()
	av.events = append(av.events, fakeEvent)
	av.mu.Unlock()

	all, err := d.ViewAllEvents("mv_dc_fallback")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 group, got %d", len(all))
	}

	dcVal := all[0].GetField("dc(user)")
	if dcVal.IsNull() {
		t.Fatal("dc(user) is null")
	}
	got := dcVal.AsInt()
	if got != 5 {
		t.Errorf("dc(user): got %d, want 5 (fallback from dc_count)", got)
	}
}

func TestDispatcher_AggregationView_Timechart(t *testing.T) {
	// End-to-end test: timechart MV should bucket _time and produce
	// time-bucketed groups, not one group per nanosecond-precision timestamp.
	d, reg, _ := setupDispatcher(t)
	def := createAggView(t, reg, "mv_timechart", `FROM main | timechart span=1h count`)
	d.ActivateView(def)

	// Create events spread across 2 hours.
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	events := []*event.Event{
		makeTestEventAtTime("main", base.Add(5*time.Minute)),
		makeTestEventAtTime("main", base.Add(15*time.Minute)),
		makeTestEventAtTime("main", base.Add(30*time.Minute)),
		makeTestEventAtTime("main", base.Add(65*time.Minute)),  // hour 2
		makeTestEventAtTime("main", base.Add(90*time.Minute)),  // hour 2
		makeTestEventAtTime("main", base.Add(119*time.Minute)), // hour 2
	}
	d.Dispatch(events)

	all, err := d.ViewAllEvents("mv_timechart")
	if err != nil {
		t.Fatalf("ViewAllEvents: %v", err)
	}

	// Should produce 2 groups (hour 10 and hour 11), not 6 (one per event).
	if len(all) != 2 {
		t.Fatalf("expected 2 time buckets, got %d (should be bucketed, not per-event)", len(all))
	}

	// Verify counts: 3 in first hour, 3 in second.
	totalCount := int64(0)
	for _, ev := range all {
		totalCount += ev.GetField("count").AsInt()
	}
	if totalCount != 6 {
		t.Errorf("total count: got %d, want 6", totalCount)
	}
}

// Test event helpers

func makeTestEventWithHost(index, host string) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = index
	e.SetField("host", event.StringValue(host))

	return e
}

func makeTestEventWithDuration(index, host string, duration float64) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = index
	e.SetField("host", event.StringValue(host))
	e.SetField("duration", event.FloatValue(duration))

	return e
}

func makeTestEventWithIndex(index, status string) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = index
	e.SetField("status", event.StringValue(status))

	return e
}

func makeTestEventFull(index, host string, bytes int64, duration, latency float64) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = index
	e.SetField("host", event.StringValue(host))
	e.SetField("bytes", event.IntValue(bytes))
	e.SetField("duration", event.FloatValue(duration))
	e.SetField("latency", event.FloatValue(latency))

	return e
}

func makeTestEventWithUser(index, host, user string) *event.Event {
	e := event.NewEvent(time.Now(), "test")
	e.Index = index
	e.SetField("host", event.StringValue(host))
	e.SetField("user", event.StringValue(user))

	return e
}

func makeTestEventAtTime(index string, t time.Time) *event.Event {
	e := event.NewEvent(t, "test")
	e.Index = index

	return e
}
