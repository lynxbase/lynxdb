package pipeline

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestKeyValueParserSkipsJSONEvents(t *testing.T) {
	raw := `{"kind":"commit","commit":{"collection":"app.bsky.feed.post"},` +
		`"text":"https://example.org/paper?commit=12de5e2&utm_source=bsky&kind=spoofed"}`
	e := event.NewEvent(time.Now(), raw)

	out, err := DefaultPipeline().Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	got := out[0]

	if v := got.GetField("kind"); v.AsString() != "commit" {
		t.Errorf("kind: got %v, want commit", v)
	}
	for _, name := range []string{"commit", "utm_source"} {
		if v := got.GetField(name); !v.IsNull() {
			t.Errorf("%s: got %v, want absent", name, v)
		}
	}
}

func TestKeyValueParserKeepsExistingFields(t *testing.T) {
	e := event.NewEvent(time.Now(), `see https://example.org/?kind=spoofed level=error`)
	e.SetField("kind", event.StringValue("commit"))

	out, err := (&KeyValueParser{}).Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if v := out[0].GetField("kind"); v.AsString() != "commit" {
		t.Errorf("kind: got %v, want commit", v)
	}
	if v := out[0].GetField("level"); v.AsString() != "error" {
		t.Errorf("level: got %v, want error", v)
	}
}

func TestKeyValueParserStillParsesPlainText(t *testing.T) {
	e := event.NewEvent(time.Now(), `status=500 user=bob`)

	out, err := DefaultPipeline().Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if v := out[0].GetField("status"); v.IsNull() {
		t.Errorf("status: got null")
	}
	if v := out[0].GetField("user"); v.AsString() != "bob" {
		t.Errorf("user: got %v, want bob", v)
	}
}

func TestKeyValueParserLeavesTheIndexToTheSender(t *testing.T) {
	e := event.NewEvent(time.Now(), `index=kvrouted host=web-01 status=500`)

	out, err := DefaultPipeline().Process([]*event.Event{e})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if out[0].Index != "main" {
		t.Errorf("index: got %q, want main", out[0].Index)
	}
	if v := out[0].Fields["index"]; v.AsString() != "kvrouted" {
		t.Errorf("index field: got %v, want kvrouted", v)
	}
	if out[0].Host != "web-01" {
		t.Errorf("host: got %q, want web-01", out[0].Host)
	}
}
