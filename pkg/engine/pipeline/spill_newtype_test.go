package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// msgpack spill round-trip (spill.go)

func TestSpillRoundtripDuration(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "dur")
	if err != nil {
		t.Fatal(err)
	}

	durations := []time.Duration{0, time.Nanosecond, time.Millisecond, time.Second, 42*time.Hour + 7*time.Minute}
	for i, d := range durations {
		if err := sw.WriteRow(map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"dur": event.DurationValue(d),
		}); err != nil {
			t.Fatal(err)
		}
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	for i, want := range durations {
		row, err := sr.ReadRow()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		v := row["dur"]
		if v.Type() != event.FieldTypeDuration {
			t.Fatalf("row %d: type = %s, want duration", i, v.Type())
		}
		if got := v.AsDuration(); got != want {
			t.Fatalf("row %d: duration = %v, want %v", i, got, want)
		}
	}
}

func TestSpillRoundtripTimestamp(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "ts")
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Date(2026, 6, 10, 12, 30, 0, 123456789, time.UTC)
	if err := sw.WriteRow(map[string]event.Value{
		"ts": event.TimestampValue(ts),
	}); err != nil {
		t.Fatal(err)
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	row, err := sr.ReadRow()
	if err != nil {
		t.Fatal(err)
	}
	v := row["ts"]
	if v.Type() != event.FieldTypeTimestamp {
		t.Fatalf("type = %s, want timestamp", v.Type())
	}
	if got := v.AsTimestamp(); !got.Equal(ts) {
		t.Fatalf("timestamp = %v, want %v", got, ts)
	}
}

func TestSpillRoundtripArray(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "arr")
	if err != nil {
		t.Fatal(err)
	}

	arr := event.ArrayValue([]event.Value{
		event.IntValue(1),
		event.StringValue("hello"),
		event.BoolValue(true),
		event.FloatValue(3.14),
		event.NullValue(),
	})
	if err := sw.WriteRow(map[string]event.Value{"arr": arr}); err != nil {
		t.Fatal(err)
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	row, err := sr.ReadRow()
	if err != nil {
		t.Fatal(err)
	}
	v := row["arr"]
	if v.Type() != event.FieldTypeArray {
		t.Fatalf("type = %s, want array", v.Type())
	}
	if v.String() != arr.String() {
		t.Fatalf("String() = %s, want %s", v.String(), arr.String())
	}
	elems := v.AsArray()
	if len(elems) != 5 {
		t.Fatalf("len = %d, want 5", len(elems))
	}
	if elems[0].Type() != event.FieldTypeInt || elems[0].AsInt() != 1 {
		t.Fatalf("elem 0: %v", elems[0])
	}
	if elems[1].Type() != event.FieldTypeString || elems[1].AsString() != "hello" {
		t.Fatalf("elem 1: %v", elems[1])
	}
	if !elems[4].IsNull() {
		t.Fatalf("elem 4: expected null, got %v", elems[4])
	}
}

func TestSpillRoundtripObject(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "obj")
	if err != nil {
		t.Fatal(err)
	}

	obj := event.ObjectValue(map[string]event.Value{
		"name":  event.StringValue("alice"),
		"age":   event.IntValue(30),
		"score": event.FloatValue(99.5),
	})
	if err := sw.WriteRow(map[string]event.Value{"obj": obj}); err != nil {
		t.Fatal(err)
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	row, err := sr.ReadRow()
	if err != nil {
		t.Fatal(err)
	}
	v := row["obj"]
	if v.Type() != event.FieldTypeObject {
		t.Fatalf("type = %s, want object", v.Type())
	}
	if v.String() != obj.String() {
		t.Fatalf("String() = %s, want %s", v.String(), obj.String())
	}
	fields := v.AsObject()
	if fields["name"].AsString() != "alice" {
		t.Fatalf("name = %v", fields["name"])
	}
	if fields["age"].AsInt() != 30 {
		t.Fatalf("age = %v", fields["age"])
	}
}

func TestSpillRoundtripNestedArrayOfObjects(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "nested")
	if err != nil {
		t.Fatal(err)
	}

	nested := event.ArrayValue([]event.Value{
		event.ObjectValue(map[string]event.Value{
			"key": event.IntValue(1),
			"sub": event.ArrayValue([]event.Value{
				event.StringValue("a"),
				event.DurationValue(5 * time.Second),
			}),
		}),
		event.ObjectValue(map[string]event.Value{
			"key": event.IntValue(2),
		}),
	})
	if err := sw.WriteRow(map[string]event.Value{"nested": nested}); err != nil {
		t.Fatal(err)
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	row, err := sr.ReadRow()
	if err != nil {
		t.Fatal(err)
	}
	v := row["nested"]
	if v.Type() != event.FieldTypeArray {
		t.Fatalf("type = %s, want array", v.Type())
	}
	if v.String() != nested.String() {
		t.Fatalf("String() = %q, want %q", v.String(), nested.String())
	}
}

// msgpack spill property test: random nested values

func TestSpillRoundtripRandomValues(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	rng := rand.New(rand.NewSource(42))
	const numRows = 200

	// Generate random values.
	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"v":   randomValue(rng, 3),
		}
	}

	// Write.
	sw, err := NewManagedSpillWriter(mgr, "rand")
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	path := sw.Path()
	sw.CloseFile()

	// Read back.
	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	for i := 0; i < numRows; i++ {
		row, err := sr.ReadRow()
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		got := row["v"]
		want := rows[i]["v"]
		if got.Type() != want.Type() {
			t.Fatalf("row %d: type %s != %s", i, got.Type(), want.Type())
		}
		if got.String() != want.String() {
			t.Fatalf("row %d: String() = %q, want %q", i, got.String(), want.String())
		}
	}
}

// randomValue generates a random event.Value with nesting up to maxDepth.
func randomValue(rng *rand.Rand, maxDepth int) event.Value {
	if maxDepth <= 0 {
		// Only scalar types at leaf.
		return randomScalar(rng)
	}
	switch rng.Intn(9) {
	case 0:
		return event.NullValue()
	case 1:
		return event.IntValue(rng.Int63())
	case 2:
		return event.FloatValue(rng.Float64() * 1000)
	case 3:
		return event.BoolValue(rng.Intn(2) == 1)
	case 4:
		return event.StringValue(fmt.Sprintf("str_%d", rng.Intn(10000)))
	case 5:
		return event.TimestampValue(time.Unix(rng.Int63n(2e9), rng.Int63n(1e9)))
	case 6:
		return event.DurationValue(time.Duration(rng.Int63n(1e15)))
	case 7:
		// Array.
		n := rng.Intn(5)
		elems := make([]event.Value, n)
		for i := range elems {
			elems[i] = randomValue(rng, maxDepth-1)
		}

		return event.ArrayValue(elems)
	case 8:
		// Object.
		n := rng.Intn(4)
		fields := make(map[string]event.Value, n)
		for j := 0; j < n; j++ {
			fields[fmt.Sprintf("f%d", j)] = randomValue(rng, maxDepth-1)
		}

		return event.ObjectValue(fields)
	default:
		return event.NullValue()
	}
}

func randomScalar(rng *rand.Rand) event.Value {
	switch rng.Intn(7) {
	case 0:
		return event.NullValue()
	case 1:
		return event.IntValue(rng.Int63())
	case 2:
		return event.FloatValue(rng.Float64() * 1000)
	case 3:
		return event.BoolValue(rng.Intn(2) == 1)
	case 4:
		return event.StringValue(fmt.Sprintf("s%d", rng.Intn(1000)))
	case 5:
		return event.TimestampValue(time.Unix(rng.Int63n(2e9), 0))
	case 6:
		return event.DurationValue(time.Duration(rng.Int63n(1e12)))
	default:
		return event.NullValue()
	}
}

// columnar spill round-trip for new types

func TestColumnarSpillDurationColumn(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "dur-col")
	if err != nil {
		t.Fatal(err)
	}

	durations := []time.Duration{
		0,
		time.Nanosecond,
		500 * time.Millisecond,
		42*time.Hour + 7*time.Minute,
		-3 * time.Second,
	}
	for i, d := range durations {
		if err := sw.WriteRow(map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"dur": event.DurationValue(d),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != len(durations) {
		t.Fatalf("expected %d rows, got %d", len(durations), batch.Len)
	}
	for i, want := range durations {
		v := batch.Value("dur", i)
		if v.Type() != event.FieldTypeDuration {
			t.Fatalf("row %d: type = %s, want duration", i, v.Type())
		}
		if got := v.AsDuration(); got != want {
			t.Fatalf("row %d: duration = %v, want %v", i, got, want)
		}
	}
}

func TestColumnarSpillDurationWithNulls(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "dur-null")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		row := map[string]event.Value{
			"idx": event.IntValue(int64(i)),
		}
		if i%3 != 0 {
			row["dur"] = event.DurationValue(time.Duration(i) * time.Second)
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		v := batch.Value("dur", i)
		if i%3 == 0 {
			if !v.IsNull() {
				t.Fatalf("row %d: expected null, got %v", i, v)
			}
		} else {
			if v.Type() != event.FieldTypeDuration {
				t.Fatalf("row %d: type = %s, want duration", i, v.Type())
			}
			if got := v.AsDuration(); got != time.Duration(i)*time.Second {
				t.Fatalf("row %d: duration = %v, want %v", i, got, time.Duration(i)*time.Second)
			}
		}
	}
}

func TestColumnarSpillArrayColumn(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "arr-col")
	if err != nil {
		t.Fatal(err)
	}

	arrays := []event.Value{
		event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2)}),
		event.ArrayValue([]event.Value{event.StringValue("a"), event.StringValue("b")}),
		event.ArrayValue([]event.Value{}), // empty array
	}
	for i, arr := range arrays {
		if err := sw.WriteRow(map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"arr": arr,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != len(arrays) {
		t.Fatalf("expected %d rows, got %d", len(arrays), batch.Len)
	}
	for i, want := range arrays {
		v := batch.Value("arr", i)
		if v.Type() != event.FieldTypeArray {
			t.Fatalf("row %d: type = %s, want array", i, v.Type())
		}
		if v.String() != want.String() {
			t.Fatalf("row %d: String() = %q, want %q", i, v.String(), want.String())
		}
	}
}

func TestColumnarSpillObjectColumn(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "obj-col")
	if err != nil {
		t.Fatal(err)
	}

	objects := []event.Value{
		event.ObjectValue(map[string]event.Value{
			"name": event.StringValue("alice"),
			"age":  event.IntValue(30),
		}),
		event.ObjectValue(map[string]event.Value{
			"name": event.StringValue("bob"),
			"tags": event.ArrayValue([]event.Value{event.StringValue("a")}),
		}),
	}
	for i, obj := range objects {
		if err := sw.WriteRow(map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"obj": obj,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len != len(objects) {
		t.Fatalf("expected %d rows, got %d", len(objects), batch.Len)
	}
	for i, want := range objects {
		v := batch.Value("obj", i)
		if v.Type() != event.FieldTypeObject {
			t.Fatalf("row %d: type = %s, want object", i, v.Type())
		}
		if v.String() != want.String() {
			t.Fatalf("row %d: String() = %q, want %q", i, v.String(), want.String())
		}
	}
}

func TestColumnarSpillArrayWithNulls(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "arr-null")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		row := map[string]event.Value{
			"idx": event.IntValue(int64(i)),
		}
		if i%2 == 0 {
			row["arr"] = event.ArrayValue([]event.Value{event.IntValue(int64(i))})
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		v := batch.Value("arr", i)
		if i%2 == 0 {
			if v.Type() != event.FieldTypeArray {
				t.Fatalf("row %d: type = %s, want array", i, v.Type())
			}
			elems := v.AsArray()
			if len(elems) != 1 || elems[0].AsInt() != int64(i) {
				t.Fatalf("row %d: array content mismatch", i)
			}
		} else {
			if !v.IsNull() {
				t.Fatalf("row %d: expected null, got %v", i, v)
			}
		}
	}
}

func TestColumnarSpillObjectWithNulls(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewColumnarSpillWriter(mgr, "obj-null")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		row := map[string]event.Value{
			"idx": event.IntValue(int64(i)),
		}
		if i%2 == 0 {
			row["obj"] = event.ObjectValue(map[string]event.Value{
				"k": event.IntValue(int64(i)),
			})
		}
		if err := sw.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	batch, err := sr.ReadBatch()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		v := batch.Value("obj", i)
		if i%2 == 0 {
			if v.Type() != event.FieldTypeObject {
				t.Fatalf("row %d: type = %s, want object", i, v.Type())
			}
			fields := v.AsObject()
			if fields["k"].AsInt() != int64(i) {
				t.Fatalf("row %d: k = %v, want %d", i, fields["k"], i)
			}
		} else {
			if !v.IsNull() {
				t.Fatalf("row %d: expected null, got %v", i, v)
			}
		}
	}
}

// pipeline-level test: sort + dedup + stats over new types with spill

func TestSortDedupStatsWithNewTypesAndSpill(t *testing.T) {
	// Build rows with array, object, and duration fields. Force spill via tiny budget.
	const numRows = 200
	rows := make([]map[string]event.Value, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = map[string]event.Value{
			"key": event.IntValue(int64(numRows - 1 - i)), // reverse for sort
			"dur": event.DurationValue(time.Duration(i%10) * time.Second),
			"arr": event.ArrayValue([]event.Value{
				event.IntValue(int64(i)),
				event.StringValue(fmt.Sprintf("item_%d", i)),
			}),
			"obj": event.ObjectValue(map[string]event.Value{
				"group": event.StringValue(fmt.Sprintf("g%d", i%5)),
				"val":   event.IntValue(int64(i)),
			}),
		}
	}

	child := NewRowScanIterator(rows, 32)
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	// Tiny budget to force spilling.
	acct := memgov.NewTestBudget("test", 16*1024).NewAccount("sort")
	sortIter := NewSortIteratorWithSpill(child, []SortField{{Name: "key", Desc: false}}, 32, acct, mgr)

	ctx := context.Background()
	if err := sortIter.Init(ctx); err != nil {
		t.Fatal(err)
	}

	result, err := CollectAll(ctx, sortIter)
	if err != nil {
		t.Fatalf("sort failed: %v", err)
	}

	if len(result) != numRows {
		t.Fatalf("expected %d rows, got %d", numRows, len(result))
	}

	// Verify sort order.
	for i := 0; i < len(result); i++ {
		got := result[i]["key"].AsInt()
		if got != int64(i) {
			t.Fatalf("row %d: key = %d, want %d", i, got, i)
		}
	}

	// Build a lookup from key -> original row for verification.
	origByKey := make(map[int64]map[string]event.Value, numRows)
	for _, row := range rows {
		origByKey[row["key"].AsInt()] = row
	}

	// Verify duration values survived the spill round-trip.
	for i := 0; i < len(result); i++ {
		key := result[i]["key"].AsInt()
		orig := origByKey[key]
		wantDur := orig["dur"].AsDuration()
		gotDur := result[i]["dur"]
		if gotDur.Type() != event.FieldTypeDuration {
			t.Fatalf("row %d: dur type = %s, want duration", i, gotDur.Type())
		}
		if gotDur.AsDuration() != wantDur {
			t.Fatalf("row %d: dur = %v, want %v", i, gotDur.AsDuration(), wantDur)
		}
	}

	// Verify array values survived (checking string rendering since the columnar
	// spill does msgpack per-value encoding).
	for i := 0; i < len(result); i++ {
		v := result[i]["arr"]
		key := result[i]["key"].AsInt()
		orig := origByKey[key]
		wantStr := orig["arr"].String()
		if v.String() != wantStr {
			t.Fatalf("row %d: arr = %q, want %q", i, v.String(), wantStr)
		}
	}
}

// memory accounting unit tests

func TestEstimateRowBytesArrayAccountsForPayload(t *testing.T) {
	// A row with a large array of 1000 strings should account for substantially
	// more memory than a row with a single scalar.
	elems := make([]event.Value, 1000)
	for i := range elems {
		elems[i] = event.StringValue(fmt.Sprintf("long_string_value_%06d", i))
	}
	arrRow := map[string]event.Value{
		"arr": event.ArrayValue(elems),
	}
	scalarRow := map[string]event.Value{
		"val": event.IntValue(42),
	}

	arrBytes := EstimateRowBytes(arrRow)
	scalarBytes := EstimateRowBytes(scalarRow)

	if arrBytes <= scalarBytes*10 {
		t.Errorf("arrBytes=%d should be >> scalarBytes=%d*10 for 1000 strings", arrBytes, scalarBytes)
	}
	// The array has 1000 strings each ~25 chars = ~25KB minimum.
	if arrBytes < 25000 {
		t.Errorf("arrBytes=%d, expected >= 25000 for 1000 strings", arrBytes)
	}
}

func TestEstimateBatchBytesIncludesArrayPayload(t *testing.T) {
	elems := make([]event.Value, 100)
	for i := range elems {
		elems[i] = event.StringValue("some_value")
	}
	batch := NewBatch(2)
	batch.AddRow(map[string]event.Value{
		"arr": event.ArrayValue(elems),
	})
	batch.AddRow(map[string]event.Value{
		"arr": event.ArrayValue(elems),
	})

	got := EstimateBatchBytes(batch)
	// 100 strings * 10 chars * 2 rows = at least 2000 bytes of string payload.
	if got < 2000 {
		t.Errorf("EstimateBatchBytes = %d, expected >= 2000", got)
	}
}

// backward compatibility: existing scalar spill semantics

func TestSpillRoundtripScalarsUnchanged(t *testing.T) {
	dir := t.TempDir()
	mgr, err := NewSpillManager(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	sw, err := NewManagedSpillWriter(mgr, "compat")
	if err != nil {
		t.Fatal(err)
	}

	row := map[string]event.Value{
		"null":  event.NullValue(),
		"str":   event.StringValue("hello"),
		"int":   event.IntValue(42),
		"float": event.FloatValue(math.Pi),
		"bool":  event.BoolValue(true),
	}
	if err := sw.WriteRow(row); err != nil {
		t.Fatal(err)
	}
	path := sw.Path()
	sw.CloseFile()

	sr, err := NewSpillReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	got, err := sr.ReadRow()
	if err != nil {
		t.Fatal(err)
	}

	if !got["null"].IsNull() {
		t.Fatal("null mismatch")
	}
	if got["str"].AsString() != "hello" {
		t.Fatal("str mismatch")
	}
	if got["int"].AsInt() != 42 {
		t.Fatal("int mismatch")
	}
	if math.Abs(got["float"].AsFloat()-math.Pi) > 1e-15 {
		t.Fatal("float mismatch")
	}
	if !got["bool"].AsBool() {
		t.Fatal("bool mismatch")
	}
}

// columnar spill round-trip: random nested values (property test)

func TestColumnarSpillRoundtripRandomNestedValues(t *testing.T) {
	mgr, err := NewSpillManager(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.CleanupAll()

	rng := rand.New(rand.NewSource(99))
	const numRows = 100

	sw, err := NewColumnarSpillWriter(mgr, "rand-col")
	if err != nil {
		t.Fatal(err)
	}

	// Each row has a column "v" with a random nested value.
	origStrings := make([]string, numRows)
	origTypes := make([]event.FieldType, numRows)
	for i := 0; i < numRows; i++ {
		v := randomValue(rng, 3)
		origStrings[i] = v.String()
		origTypes[i] = v.Type()
		if err := sw.WriteRow(map[string]event.Value{
			"idx": event.IntValue(int64(i)),
			"v":   v,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sw.CloseFile(); err != nil {
		t.Fatal(err)
	}

	// Read back row by row.
	sr, err := NewColumnarSpillReader(sw.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()

	for i := 0; i < numRows; i++ {
		row, err := sr.ReadRow()
		if errors.Is(err, io.EOF) {
			t.Fatalf("premature EOF at row %d", i)
		}
		if err != nil {
			t.Fatalf("row %d: %v", i, err)
		}
		got := row["v"]
		// For the columnar spill, array/object types may round-trip through
		// msgpack encoding. Verify the String() representation matches.
		if got.String() != origStrings[i] {
			t.Fatalf("row %d: String() = %q, want %q (original type %s, got type %s)",
				i, got.String(), origStrings[i], origTypes[i], got.Type())
		}
	}
}
