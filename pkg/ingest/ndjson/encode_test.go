package ndjson

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestAppendEvent(t *testing.T) {
	ev := event.NewEvent(time.Date(2026, 1, 1, 0, 0, 0, 5, time.UTC), "line \"one\"\n\x01")
	ev.Host, ev.Source, ev.Index = "web-1", "/var/log/app.log", "web"
	ev.SetField("status", event.IntValue(500))
	ev.SetField("rt", event.FloatValue(0.25))
	ev.SetField("ok", event.BoolValue(false))
	ev.SetField("user", event.StringValue("ünï"))
	ev.SetField("host", event.StringValue("ignored"))
	got := string(AppendEvent(nil, ev))
	want := `{"_time":"2026-01-01T00:00:00.000000005Z","_raw":"line \"one\"\n\u0001","host":"web-1","source":"/var/log/app.log","ok":false,"rt":0.25,"status":500,"user":"ünï"}`
	if got != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(got), &m); err != nil {
		t.Fatal(err)
	}
}
