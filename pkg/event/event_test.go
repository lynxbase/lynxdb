package event

import (
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	e := NewEvent(ts, "error: disk full")
	if !e.Time.Equal(ts) {
		t.Fatalf("expected time %v, got %v", ts, e.Time)
	}
	if e.Raw != "error: disk full" {
		t.Fatalf("expected raw text, got %q", e.Raw)
	}
	if e.Fields == nil {
		t.Fatal("Fields map should be initialized")
	}
}

func TestEventSetGetField(t *testing.T) {
	e := NewEvent(time.Now(), "test")
	e.SetField("level", StringValue("error"))
	e.SetField("status", IntValue(500))

	v := e.GetField("level")
	if v.Type() != FieldTypeString || v.AsString() != "error" {
		t.Fatalf("expected string 'error', got %v", v)
	}

	v = e.GetField("status")
	if v.Type() != FieldTypeInt || v.AsInt() != 500 {
		t.Fatalf("expected int 500, got %v", v)
	}
}

func TestEventGetFieldMissing(t *testing.T) {
	e := NewEvent(time.Now(), "test")
	v := e.GetField("nonexistent")
	if !v.IsNull() {
		t.Fatalf("expected null, got %v", v)
	}
}

func TestEventBuiltInFields(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	e := NewEvent(ts, "raw log line")
	e.Source = "/var/log/syslog"
	e.SourceType = "syslog"
	e.Host = "web-01"
	e.Index = "main"

	tests := []struct {
		name string
		want string
	}{
		{"_raw", "raw log line"},
		{"_source", "/var/log/syslog"},
		{"source", "/var/log/syslog"},
		{"_sourcetype", "syslog"},
		{"sourcetype", "syslog"},
		{"host", "web-01"},
		// "index" is the physical partition key (Event.Index), not Event.Source.
		{"index", "main"},
	}
	for _, tt := range tests {
		v := e.GetField(tt.name)
		if v.Type() != FieldTypeString || v.AsString() != tt.want {
			t.Errorf("GetField(%q) = %v, want string %q", tt.name, v, tt.want)
		}
	}

	// _time returns a timestamp
	tv := e.GetField("_time")
	if tv.Type() != FieldTypeTimestamp {
		t.Fatalf("expected timestamp type for _time, got %s", tv.Type())
	}
	if !tv.AsTimestamp().Equal(ts) {
		t.Fatalf("_time mismatch: got %v, want %v", tv.AsTimestamp(), ts)
	}
}

func TestEventFieldNames(t *testing.T) {
	e := NewEvent(time.Now(), "test")
	e.SetField("level", StringValue("info"))
	e.SetField("pid", IntValue(1234))

	names := e.FieldNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 field names, got %d", len(names))
	}
	nameSet := make(map[string]bool)
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet["level"] || !nameSet["pid"] {
		t.Fatalf("expected level and pid, got %v", names)
	}
}

func TestEventSetFieldNilFields(t *testing.T) {
	e := &Event{Time: time.Now(), Raw: "test"}
	e.SetField("x", IntValue(1))
	if e.GetField("x").AsInt() != 1 {
		t.Fatal("SetField should initialize nil Fields map")
	}
}

func TestEventGetFieldNilFields(t *testing.T) {
	e := &Event{Time: time.Now(), Raw: "test"}
	v := e.GetField("missing")
	if !v.IsNull() {
		t.Fatal("GetField on nil Fields should return null")
	}
}

func TestEventString(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	e := NewEvent(ts, "hello world")
	e.Host = "web-01"
	e.Index = "main"
	s := e.String()
	if s == "" {
		t.Fatal("String() should not be empty")
	}
}

func TestTruncate(t *testing.T) {
	if truncate("short", 80) != "short" {
		t.Fatal("should not truncate short strings")
	}
	long := "abcdefghijklmnop"
	got := truncate(long, 10)
	if len(got) != 10 {
		t.Fatalf("expected truncated length 10, got %d", len(got))
	}
	if got[len(got)-3:] != "..." {
		t.Fatal("truncated string should end with ...")
	}
}
