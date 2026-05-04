package splunkhec

import "testing"

func TestMapper_EventString_RawSet(t *testing.T) {
	ev := Event{Event: "hello", Source: "src", Index: "idx"}.ToEvent()
	if ev.Raw != "hello" {
		t.Fatalf("Raw = %q, want hello", ev.Raw)
	}
	if ev.Source != "src" {
		t.Fatalf("Source = %q, want src", ev.Source)
	}
	if ev.Index != "main" {
		t.Fatalf("Index = %q, want main", ev.Index)
	}
	if got := ev.Fields["source_tag"].AsString(); got != "idx" {
		t.Fatalf("source_tag = %q, want idx", got)
	}
}

func TestMapper_EventJSON_StringifiedRaw(t *testing.T) {
	ev := Event{Event: map[string]interface{}{"message": "hello"}}.ToEvent()
	if ev.Raw != `{"message":"hello"}` {
		t.Fatalf("Raw = %q", ev.Raw)
	}
}

func TestMapper_TimeFractional_Parsed(t *testing.T) {
	ts := 1700000000.25
	ev := Event{Time: &ts, Event: "hello"}.ToEvent()
	if got := ev.Time.UnixNano(); got != 1700000000250000000 {
		t.Fatalf("UnixNano = %d", got)
	}
}

func TestMapper_FieldsObject_Mapped(t *testing.T) {
	ev := Event{Event: "hello", Fields: map[string]interface{}{"code": float64(200), "ok": true}}.ToEvent()
	gotCode, err := ev.Fields["code"].AsFloatE()
	if err != nil {
		t.Fatalf("code type: %v", err)
	}
	if gotCode != 200 {
		t.Fatalf("code = %v, want 200", gotCode)
	}
	if got := ev.Fields["ok"].AsBool(); !got {
		t.Fatalf("ok = false, want true")
	}
}
