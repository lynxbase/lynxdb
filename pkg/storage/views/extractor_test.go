package views

import (
	"testing"
	"time"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

func TestExtractor_ExtractBuiltinFields(t *testing.T) {
	cols := []ColumnDef{
		{Name: "_time", Type: event.FieldTypeTimestamp},
		{Name: "_source", Type: event.FieldTypeString},
	}
	x := NewExtractor(cols)

	now := time.Now().Truncate(time.Second)
	e := event.NewEvent(now, "test raw line")
	e.Source = "nginx"
	e.Index = "main"

	out := x.Extract(e)
	if !out.Time.Equal(now) {
		t.Errorf("Time: got %v, want %v", out.Time, now)
	}
	if out.Source != "nginx" {
		t.Errorf("Source: got %q, want %q", out.Source, "nginx")
	}
}

func TestExtractor_ExtractUserFields(t *testing.T) {
	cols := []ColumnDef{
		{Name: "uri", Type: event.FieldTypeString},
		{Name: "status", Type: event.FieldTypeInt},
	}
	x := NewExtractor(cols)

	e := event.NewEvent(time.Now(), "test")
	e.SetField("uri", event.StringValue("/api/health"))
	e.SetField("status", event.IntValue(200))
	e.Index = "main"

	out := x.Extract(e)
	if v := out.Fields["uri"]; v.String() != "/api/health" {
		t.Errorf("uri: got %q", v.String())
	}
	if v := out.Fields["status"]; v.String() != "200" {
		t.Errorf("status: got %q", v.String())
	}
}

func TestExtractor_ExtractMixedFields(t *testing.T) {
	cols := []ColumnDef{
		{Name: "_time", Type: event.FieldTypeTimestamp},
		{Name: "_source", Type: event.FieldTypeString},
		{Name: "uri", Type: event.FieldTypeString},
	}
	x := NewExtractor(cols)

	now := time.Now().Truncate(time.Second)
	e := event.NewEvent(now, "test")
	e.Source = "nginx"
	e.SetField("uri", event.StringValue("/api"))
	e.Index = "main"

	out := x.Extract(e)
	if !out.Time.Equal(now) {
		t.Errorf("Time mismatch")
	}
	if out.Source != "nginx" {
		t.Errorf("Source: got %q", out.Source)
	}
	if v := out.Fields["uri"]; v.String() != "/api" {
		t.Errorf("uri: got %q", v.String())
	}
}

func TestExtractor_MissingField(t *testing.T) {
	cols := []ColumnDef{
		{Name: "nonexistent", Type: event.FieldTypeString},
	}
	x := NewExtractor(cols)

	e := event.NewEvent(time.Now(), "test")
	e.Index = "main"

	out := x.Extract(e)
	v := out.Fields["nonexistent"]
	if !v.IsNull() {
		t.Errorf("missing field should be null, got %v", v)
	}
}
