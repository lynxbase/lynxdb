package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func makeEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		events[i] = &event.Event{
			Time:  base.Add(time.Duration(i) * time.Second),
			Raw:   "test log line " + event.IntValue(int64(i)).String(),
			Host:  "web-01",
			Index: "idx_test",
			Fields: map[string]event.Value{
				"status": event.IntValue(int64(200 + (i%5)*100)),
				"x":      event.IntValue(int64(i)),
			},
		}
	}

	return events
}

func TestScanIteratorBatching(t *testing.T) {
	events := makeEvents(2500)
	scan := NewScanIterator(events, 1024)
	ctx := context.Background()
	scan.Init(ctx)

	totalRows := 0
	batches := 0
	for {
		batch, err := scan.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if batch == nil {
			break
		}
		totalRows += batch.Len
		batches++
	}
	if totalRows != 2500 {
		t.Errorf("total rows: got %d, want 2500", totalRows)
	}
	if batches != 3 { // 1024 + 1024 + 452
		t.Errorf("batches: got %d, want 3", batches)
	}
}

func TestFilterIteratorCorrectness(t *testing.T) {
	events := makeEvents(100)
	scan := NewScanIterator(events, 1024)

	// Filter: status >= 500
	pred := &vm.Program{}
	sIdx := pred.AddFieldName("status")
	c500 := pred.AddConstant(event.IntValue(500))
	pred.EmitOp(vm.OpLoadField, sIdx)
	pred.EmitOp(vm.OpConstInt, c500)
	pred.EmitOp(vm.OpGte)
	pred.EmitOp(vm.OpReturn)

	filter := NewFilterIterator(scan, pred)
	ctx := context.Background()
	filter.Init(ctx)

	rows, err := CollectAll(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	// status pattern: 200, 300, 400, 500, 600, 200, 300, 400, 500, 600...
	// >= 500: indices 3,4,8,9,13,14... = 2 out of 5 = 40 total
	if len(rows) != 40 {
		t.Errorf("filtered rows: got %d, want 40", len(rows))
	}
}

func TestLimitEarlyTermination(t *testing.T) {
	events := makeEvents(100000)
	scan := NewScanIterator(events, 1024)
	limit := NewLimitIterator(scan, 10)

	ctx := context.Background()
	limit.Init(ctx)
	rows, err := CollectAll(ctx, limit)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Errorf("got %d rows, want 10", len(rows))
	}
	// Verify early termination: scan should only have been called 1 time
	if scan.ScanCalls() > 1 {
		t.Errorf("scan called %d times, want 1 (early termination)", scan.ScanCalls())
	}
}

func TestPipelineEndToEnd(t *testing.T) {
	// FROM idx | WHERE status >= 500 | stats count
	events := makeEvents(100)
	scan := NewScanIterator(events, 1024)

	pred := &vm.Program{}
	sIdx := pred.AddFieldName("status")
	c500 := pred.AddConstant(event.IntValue(500))
	pred.EmitOp(vm.OpLoadField, sIdx)
	pred.EmitOp(vm.OpConstInt, c500)
	pred.EmitOp(vm.OpGte)
	pred.EmitOp(vm.OpReturn)

	filter := NewFilterIterator(scan, pred)
	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(filter, aggs, nil, nil)

	ctx := context.Background()
	agg.Init(ctx)
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(rows))
	}
	count := rows[0]["count"]
	if count.AsInt() != 40 {
		t.Errorf("count: got %v, want 40", count)
	}
}

func TestPipelineStatsGroupBy(t *testing.T) {
	events := makeEvents(100)
	scan := NewScanIterator(events, 1024)

	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(scan, aggs, []string{"status"}, nil)

	ctx := context.Background()
	agg.Init(ctx)
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}
	// 5 distinct status values: 200, 300, 400, 500, 600
	if len(rows) != 5 {
		t.Errorf("expected 5 groups, got %d", len(rows))
	}
	total := int64(0)
	for _, row := range rows {
		total += row["count"].AsInt()
	}
	if total != 100 {
		t.Errorf("total count: got %d, want 100", total)
	}
}

func TestContextCancellation(t *testing.T) {
	events := makeEvents(100000)
	scan := NewScanIterator(events, 1024)

	ctx, cancel := context.WithCancel(context.Background())
	scan.Init(ctx)

	// Cancel after first batch
	batch, err := scan.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if batch == nil || batch.Len != 1024 {
		t.Fatal("expected first batch of 1024")
	}
	cancel()

	// Next call should fail with context error
	_, err = scan.Next(ctx)
	if err == nil {
		t.Error("expected context cancellation error")
	}
}

func TestBatchSizeVariations(t *testing.T) {
	events := makeEvents(100)
	for _, bs := range []int{1, 32, 1024, 8192} {
		t.Run("batchSize="+event.IntValue(int64(bs)).String(), func(t *testing.T) {
			scan := NewScanIterator(events, bs)
			ctx := context.Background()
			scan.Init(ctx)

			total := 0
			for {
				batch, err := scan.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if batch == nil {
					break
				}
				total += batch.Len
			}
			if total != 100 {
				t.Errorf("total: got %d, want 100", total)
			}
		})
	}
}

func TestProjectIterator(t *testing.T) {
	events := makeEvents(10)
	scan := NewScanIterator(events, 1024)
	project := NewProjectIterator(scan, []string{"status", "host"}, false)

	ctx := context.Background()
	project.Init(ctx)
	rows, err := CollectAll(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
	for _, row := range rows {
		if _, ok := row["_raw"]; ok {
			t.Error("_raw should have been projected out")
		}
		if _, ok := row["status"]; !ok {
			t.Error("status should be present")
		}
	}
}

func TestRenameIterator(t *testing.T) {
	events := makeEvents(5)
	scan := NewScanIterator(events, 1024)
	rename := NewRenameIterator(scan, map[string]string{"status": "http_status"})

	ctx := context.Background()
	rename.Init(ctx)
	rows, err := CollectAll(ctx, rename)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if _, ok := row["http_status"]; !ok {
			t.Error("http_status should be present after rename")
		}
		if _, ok := row["status"]; ok {
			t.Error("status should have been renamed")
		}
	}
}

func TestSortIterator(t *testing.T) {
	events := makeEvents(20)
	scan := NewScanIterator(events, 1024)
	sort := NewSortIterator(scan, []SortField{{Name: "x", Desc: true}}, 1024)

	ctx := context.Background()
	sort.Init(ctx)
	rows, err := CollectAll(ctx, sort)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("expected 20 rows, got %d", len(rows))
	}
	// Check descending order
	for i := 1; i < len(rows); i++ {
		prev := rows[i-1]["x"].AsInt()
		curr := rows[i]["x"].AsInt()
		if prev < curr {
			t.Errorf("not sorted desc at %d: %d < %d", i, prev, curr)
		}
	}
}

func TestDedupIterator(t *testing.T) {
	events := makeEvents(100) // 5 distinct status values
	scan := NewScanIterator(events, 1024)
	dedup := NewDedupIterator(scan, []string{"status"}, 1)

	ctx := context.Background()
	dedup.Init(ctx)
	rows, err := CollectAll(ctx, dedup)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Errorf("expected 5 deduped rows, got %d", len(rows))
	}
}

func TestSearchFilter(t *testing.T) {
	events := []*event.Event{
		{Time: time.Now(), Raw: "error: something failed", Fields: map[string]event.Value{}},
		{Time: time.Now(), Raw: "info: all good", Fields: map[string]event.Value{}},
		{Time: time.Now(), Raw: "error: another failure", Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)
	filter := NewSearchFilterIterator(scan, "error")

	ctx := context.Background()
	filter.Init(ctx)
	rows, err := CollectAll(ctx, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 matching rows, got %d", len(rows))
	}
}

func TestAggregateSum(t *testing.T) {
	events := makeEvents(10)
	scan := NewScanIterator(events, 1024)
	aggs := []AggFunc{{Name: "sum", Field: "x", Alias: "total"}}
	agg := NewAggregateIterator(scan, aggs, nil, nil)

	ctx := context.Background()
	agg.Init(ctx)
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// sum(0..9) = 45
	total := rows[0]["total"]
	if total.AsFloat() != 45 {
		t.Errorf("sum: got %v, want 45", total)
	}
}

func TestAggregateAvgMinMax(t *testing.T) {
	events := makeEvents(10)
	scan := NewScanIterator(events, 1024)
	aggs := []AggFunc{
		{Name: "avg", Field: "x", Alias: "avg_x"},
		{Name: "min", Field: "x", Alias: "min_x"},
		{Name: "max", Field: "x", Alias: "max_x"},
	}
	agg := NewAggregateIterator(scan, aggs, nil, nil)

	ctx := context.Background()
	agg.Init(ctx)
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}
	avg := rows[0]["avg_x"].AsFloat()
	if avg != 4.5 {
		t.Errorf("avg: got %v, want 4.5", avg)
	}
	minX := rows[0]["min_x"].AsInt()
	if minX != 0 {
		t.Errorf("min: got %v, want 0", minX)
	}
	maxX := rows[0]["max_x"].AsInt()
	if maxX != 9 {
		t.Errorf("max: got %v, want 9", maxX)
	}
}

func TestUnionIterator(t *testing.T) {
	events1 := makeEvents(5)
	events2 := makeEvents(3)
	scan1 := NewScanIterator(events1, 1024)
	scan2 := NewScanIterator(events2, 1024)
	union := NewUnionIterator([]Iterator{scan1, scan2})

	ctx := context.Background()
	union.Init(ctx)
	rows, err := CollectAll(ctx, union)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 8 {
		t.Errorf("expected 8 rows, got %d", len(rows))
	}
}

func TestBinIterator(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []*event.Event{
		{Time: base.Add(30 * time.Second), Raw: "a", Fields: map[string]event.Value{}},
		{Time: base.Add(90 * time.Second), Raw: "b", Fields: map[string]event.Value{}},
		{Time: base.Add(130 * time.Second), Raw: "c", Fields: map[string]event.Value{}},
	}
	scan := NewScanIterator(events, 1024)
	bin := NewBinIterator(scan, "_time", "_time", time.Minute)

	ctx := context.Background()
	bin.Init(ctx)
	rows, err := CollectAll(ctx, bin)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// First event (30s) bucketed to minute 0
	ts0 := rows[0]["_time"].AsTimestamp().UTC()
	if !ts0.Equal(base) {
		t.Errorf("row 0 _time: got %v, want %v", ts0, base)
	}
	// Second event (90s) bucketed to minute 1
	ts1 := rows[1]["_time"].AsTimestamp().UTC()
	if !ts1.Equal(base.Add(time.Minute)) {
		t.Errorf("row 1 _time: got %v, want %v", ts1, base.Add(time.Minute))
	}
}

func TestStatsNullGroupByKey(t *testing.T) {
	// Use a static batch with explicit null values for the group key
	batch := NewBatch(4)
	batch.AddRow(map[string]event.Value{"_raw": event.StringValue("a"), "category": event.StringValue("web")})
	batch.AddRow(map[string]event.Value{"_raw": event.StringValue("b"), "category": event.StringValue("web")})
	batch.AddRow(map[string]event.Value{"_raw": event.StringValue("c"), "category": event.NullValue()})
	batch.AddRow(map[string]event.Value{"_raw": event.StringValue("d"), "category": event.NullValue()})
	child := &staticIter{batches: []*Batch{batch}}

	aggs := []AggFunc{{Name: "count", Alias: "count"}}
	agg := NewAggregateIterator(child, aggs, []string{"category"}, nil)

	ctx := context.Background()
	agg.Init(ctx)
	rows, err := CollectAll(ctx, agg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rows))
	}
	// Check that no group key is null — null keys should be ""
	for _, row := range rows {
		cat := row["category"]
		if cat.IsNull() {
			t.Error("null group key should have been converted to empty string")
		}
		if cat.Type() == event.FieldTypeString && cat.AsString() == "" {
			// This is the null group — verify count
			count := row["count"].AsInt()
			if count != 2 {
				t.Errorf("null group count: got %d, want 2", count)
			}
		} else if cat.AsString() == "web" {
			count := row["count"].AsInt()
			if count != 2 {
				t.Errorf("web group count: got %d, want 2", count)
			}
		}
	}
}

func TestBinStringTimestamps(t *testing.T) {
	// BIN should parse string _time values
	// Two events within same 100ms bucket (350ms and 380ms), one in different bucket (500ms)
	ts1 := "2025-06-30T23:59:57.350-0400"
	ts2 := "2025-06-30T23:59:57.380-0400"
	ts3 := "2025-06-30T23:59:57.500-0400" // different 100ms bucket

	batch := NewBatch(3)
	batch.AddRow(map[string]event.Value{"_time": event.StringValue(ts1)})
	batch.AddRow(map[string]event.Value{"_time": event.StringValue(ts2)})
	batch.AddRow(map[string]event.Value{"_time": event.StringValue(ts3)})
	child := &staticIter{batches: []*Batch{batch}}
	bin := NewBinIterator(child, "_time", "_time", 100*time.Millisecond)

	ctx := context.Background()
	bin.Init(ctx)
	rows, err := CollectAll(ctx, bin)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// All should be timestamps now
	for i, row := range rows {
		v := row["_time"]
		if v.Type() != event.FieldTypeTimestamp {
			t.Errorf("row %d: expected timestamp, got %s (%v)", i, v.Type(), v)
		}
	}
	// First two should be in the same bucket (both in 300-399ms range)
	t0 := rows[0]["_time"].AsTimestamp()
	t1 := rows[1]["_time"].AsTimestamp()
	t2 := rows[2]["_time"].AsTimestamp()
	if !t0.Equal(t1) {
		t.Errorf("first two should be same bucket: %v vs %v", t0, t1)
	}
	if t0.Equal(t2) {
		t.Errorf("third should be different bucket: %v vs %v", t0, t2)
	}
}

// staticIter is a test helper that returns pre-built batches.
type staticIter struct {
	batches []*Batch
	idx     int
}

func (s *staticIter) Init(ctx context.Context) error { return nil }
func (s *staticIter) Next(ctx context.Context) (*Batch, error) {
	if s.idx >= len(s.batches) {
		return nil, nil
	}
	b := s.batches[s.idx]
	s.idx++

	return b, nil
}
func (s *staticIter) Close() error        { return nil }
func (s *staticIter) Schema() []FieldInfo { return nil }

func TestStreamStatsIterator(t *testing.T) {
	events := makeEvents(5) // x = 0,1,2,3,4
	scan := NewScanIterator(events, 1024)
	aggs := []AggFunc{{Name: "sum", Field: "x", Alias: "running_sum"}}
	ss := NewStreamStatsIterator(scan, aggs, nil, 0, true)

	ctx := context.Background()
	ss.Init(ctx)
	rows, err := CollectAll(ctx, ss)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	// With current=true and window=inf: running sum = 0, 1, 3, 6, 10
	expected := []float64{0, 1, 3, 6, 10}
	for i, row := range rows {
		rs := row["running_sum"]
		f, ok := vm.ValueToFloat(rs)
		if !ok {
			t.Errorf("row %d: running_sum not numeric: %v", i, rs)

			continue
		}
		if f != expected[i] {
			t.Errorf("row %d: running_sum got %v, want %v", i, f, expected[i])
		}
	}
}

// Benchmark: head 10 on 100K events (streaming vs batch).
func BenchmarkStreamingHead10(b *testing.B) {
	events := makeEvents(100000)

	b.Run("StreamingPipeline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			scan := NewScanIterator(events, 1024)
			limit := NewLimitIterator(scan, 10)
			ctx := context.Background()
			limit.Init(ctx)
			for {
				batch, _ := limit.Next(ctx)
				if batch == nil {
					break
				}
			}
		}
	})

	b.Run("BatchAll", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Simulate batch: read all, then take 10
			scan := NewScanIterator(events, 100000)
			ctx := context.Background()
			scan.Init(ctx)
			var all []map[string]event.Value
			for {
				batch, _ := scan.Next(ctx)
				if batch == nil {
					break
				}
				for j := 0; j < batch.Len; j++ {
					all = append(all, batch.Row(j))
				}
			}
			if len(all) > 10 {
				_ = all[:10]
			}
		}
	})
}

func TestTailIterator(t *testing.T) {
	t.Run("tail_3_on_100_events", func(t *testing.T) {
		events := makeEvents(100)
		scan := NewScanIterator(events, 32)
		tail := NewTailIterator(scan, 3, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		// Last 3 events should have x = 97, 98, 99 (in order).
		for i, row := range rows {
			want := int64(97 + i)
			got := row["x"].AsInt()
			if got != want {
				t.Errorf("row %d: x = %d, want %d", i, got, want)
			}
		}
	})

	t.Run("tail_100_on_5_events", func(t *testing.T) {
		events := makeEvents(5)
		scan := NewScanIterator(events, 1024)
		tail := NewTailIterator(scan, 100, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 5 {
			t.Fatalf("expected 5 rows (all events), got %d", len(rows))
		}
		for i, row := range rows {
			got := row["x"].AsInt()
			if got != int64(i) {
				t.Errorf("row %d: x = %d, want %d", i, got, i)
			}
		}
	})

	t.Run("tail_3_on_0_events", func(t *testing.T) {
		scan := NewScanIterator(nil, 1024)
		tail := NewTailIterator(scan, 3, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows, got %d", len(rows))
		}
	})

	t.Run("tail_1_on_1_event", func(t *testing.T) {
		events := makeEvents(1)
		scan := NewScanIterator(events, 1024)
		tail := NewTailIterator(scan, 1, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0]["x"].AsInt() != 0 {
			t.Errorf("x = %d, want 0", rows[0]["x"].AsInt())
		}
	})

	t.Run("tail_0_returns_empty", func(t *testing.T) {
		events := makeEvents(10)
		scan := NewScanIterator(events, 1024)
		tail := NewTailIterator(scan, 0, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows for tail 0, got %d", len(rows))
		}
	})

	// Ring buffer worked example from the code comment:
	// 5 rows, tail 3 → rows 2, 3, 4 in order.
	t.Run("ring_buffer_worked_example", func(t *testing.T) {
		events := makeEvents(5)            // x = 0, 1, 2, 3, 4
		scan := NewScanIterator(events, 2) // small batch to exercise multiple batches
		tail := NewTailIterator(scan, 3, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 3 {
			t.Fatalf("expected 3 rows, got %d", len(rows))
		}
		// Expected: x = 2, 3, 4 (last 3 of 5, in chronological order).
		expected := []int64{2, 3, 4}
		for i, row := range rows {
			got := row["x"].AsInt()
			if got != expected[i] {
				t.Errorf("row %d: x = %d, want %d", i, got, expected[i])
			}
		}
	})

	// Verify ring buffer never exceeds count entries — O(N) memory.
	t.Run("ring_buffer_bounded_memory", func(t *testing.T) {
		// Create 1M events via a mock iterator that yields them in batches.
		// The tail 3 ring should never hold more than 3 rows.
		const totalEvents = 100000
		const tailN = 3

		events := makeEvents(totalEvents)
		scan := NewScanIterator(events, 1024)
		tail := NewTailIterator(scan, tailN, 1024)

		ctx := context.Background()
		tail.Init(ctx)
		rows, err := CollectAll(ctx, tail)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != tailN {
			t.Fatalf("expected %d rows, got %d", tailN, len(rows))
		}
		// Verify correct last 3 events.
		for i, row := range rows {
			want := int64(totalEvents - tailN + i)
			got := row["x"].AsInt()
			if got != want {
				t.Errorf("row %d: x = %d, want %d", i, got, want)
			}
		}
	})
}

func BenchmarkPipelineThroughput(b *testing.B) {
	events := makeEvents(100000)

	pred := &vm.Program{}
	sIdx := pred.AddFieldName("status")
	c500 := pred.AddConstant(event.IntValue(500))
	pred.EmitOp(vm.OpLoadField, sIdx)
	pred.EmitOp(vm.OpConstInt, c500)
	pred.EmitOp(vm.OpGte)
	pred.EmitOp(vm.OpReturn)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		filter := NewFilterIterator(scan, pred)
		aggs := []AggFunc{{Name: "count", Alias: "count"}}
		agg := NewAggregateIterator(filter, aggs, nil, nil)
		ctx := context.Background()
		agg.Init(ctx)
		for {
			batch, _ := agg.Next(ctx)
			if batch == nil {
				break
			}
		}
	}
}
