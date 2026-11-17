package database

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestAppendEvents(t *testing.T) {
	db := openTest(t, Config{})
	a := event.NewEvent(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "a")
	a.Index = "x"
	b := event.NewEvent(time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC), "b")
	if err := db.AppendEvents([]*event.Event{a, b}); err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if got := db.Names(); len(got) != 2 || got[0] != "main" || got[1] != "x" {
		t.Fatalf("names: %v", got)
	}
	if got := run(t, db, "source=x | fields _time, _raw", QueryOptions{}); len(got) != 1 || got[0] != `{"_time":"2026-01-01T00:00:00Z","_raw":"a"}` {
		t.Fatalf("rows: %v", got)
	}
}
