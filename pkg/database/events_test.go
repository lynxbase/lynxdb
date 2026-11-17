package database

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestAppendEventJSON(t *testing.T) {
	ev := event.NewEvent(time.Date(2026, 1, 1, 0, 0, 0, 5, time.UTC), "line \"one\"\n\x01")
	ev.Host, ev.Source, ev.Index = "web-1", "/var/log/app.log", "web"
	ev.SetField("status", event.IntValue(500))
	ev.SetField("rt", event.FloatValue(0.25))
	ev.SetField("ok", event.BoolValue(false))
	ev.SetField("user", event.StringValue("ünï"))
	ev.SetField("host", event.StringValue("ignored"))
	got := string(AppendEventJSON(nil, ev))
	want := `{"_time":"2026-01-01T00:00:00.000000005Z","_raw":"line \"one\"\n\u0001","host":"web-1","source":"/var/log/app.log","ok":false,"rt":0.25,"status":500,"user":"ünï"}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatal(err)
	}
}

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
