package pipeline

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// makeBatch creates a batch with the given column data for testing.
func makeBatch(cols map[string][]event.Value) *Batch {
	n := 0
	for _, v := range cols {
		n = len(v)
		break
	}
	return &Batch{Columns: cols, Len: n}
}

func TestProjectIterator_KeepExact(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"host":   {event.StringValue("web-01")},
		"status": {event.IntValue(200)},
		"uri":    {event.StringValue("/api")},
	})
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"host", "status"},
		false,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("columns: got %d, want 2", len(out.Columns))
	}
	if _, ok := out.Columns["host"]; !ok {
		t.Error("expected host column")
	}
	if _, ok := out.Columns["status"]; !ok {
		t.Error("expected status column")
	}
	if _, ok := out.Columns["uri"]; ok {
		t.Error("uri column should be excluded")
	}
}

func TestProjectIterator_RemoveExact(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"host":   {event.StringValue("web-01")},
		"status": {event.IntValue(200)},
		"uri":    {event.StringValue("/api")},
	})
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"uri"},
		true,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("columns: got %d, want 2", len(out.Columns))
	}
	if _, ok := out.Columns["uri"]; ok {
		t.Error("uri column should be removed")
	}
}

func TestProjectIterator_KeepGlob(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"pg.host":     {event.StringValue("db-01")},
		"pg.port":     {event.IntValue(5432)},
		"pg.database": {event.StringValue("mydb")},
		"http.status": {event.IntValue(200)},
		"http.method": {event.StringValue("GET")},
		"level":       {event.StringValue("info")},
	})
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"pg.*"},
		false,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 3 {
		t.Fatalf("columns: got %d, want 3 (pg.host, pg.port, pg.database)", len(out.Columns))
	}
	for _, col := range []string{"pg.host", "pg.port", "pg.database"} {
		if _, ok := out.Columns[col]; !ok {
			t.Errorf("expected column %q", col)
		}
	}
	for _, col := range []string{"http.status", "http.method", "level"} {
		if _, ok := out.Columns[col]; ok {
			t.Errorf("column %q should be excluded", col)
		}
	}
}

func TestProjectIterator_KeepGlobAndExact(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"pg.host":     {event.StringValue("db-01")},
		"pg.port":     {event.IntValue(5432)},
		"http.status": {event.IntValue(200)},
		"level":       {event.StringValue("info")},
	})
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"pg.*", "level"},
		false,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 3 {
		t.Fatalf("columns: got %d, want 3 (pg.host, pg.port, level)", len(out.Columns))
	}
	for _, col := range []string{"pg.host", "pg.port", "level"} {
		if _, ok := out.Columns[col]; !ok {
			t.Errorf("expected column %q", col)
		}
	}
}

func TestProjectIterator_RemoveGlob(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"pg.host":     {event.StringValue("db-01")},
		"pg.port":     {event.IntValue(5432)},
		"http.status": {event.IntValue(200)},
		"level":       {event.StringValue("info")},
	})
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"pg.*"},
		true,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 2 {
		t.Fatalf("columns: got %d, want 2 (http.status, level)", len(out.Columns))
	}
	if _, ok := out.Columns["pg.host"]; ok {
		t.Error("pg.host should be removed")
	}
	if _, ok := out.Columns["pg.port"]; ok {
		t.Error("pg.port should be removed")
	}
}

func TestProjectIterator_KeepStar(t *testing.T) {
	batch := makeBatch(map[string][]event.Value{
		"host":   {event.StringValue("web-01")},
		"status": {event.IntValue(200)},
		"uri":    {event.StringValue("/api")},
	})
	// "table *" or "fields *" passes ["*"] which should match everything.
	proj := NewProjectIterator(
		&staticIterator{batches: []*Batch{batch}},
		[]string{"*"},
		false,
	)
	if err := proj.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	out, err := proj.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Columns) != 3 {
		t.Fatalf("columns: got %d, want 3 (all)", len(out.Columns))
	}
}

func TestProjectIterator_SchemaWithGlobs(t *testing.T) {
	proj := NewProjectIterator(
		&staticIterator{},
		[]string{"pg.*"},
		false,
	)
	if schema := proj.Schema(); schema != nil {
		t.Errorf("Schema() with globs should return nil, got %v", schema)
	}
}

func TestProjectIterator_SchemaWithoutGlobs(t *testing.T) {
	proj := NewProjectIterator(
		&staticIterator{},
		[]string{"host", "status"},
		false,
	)
	schema := proj.Schema()
	if len(schema) != 2 {
		t.Fatalf("Schema() length: got %d, want 2", len(schema))
	}
}
