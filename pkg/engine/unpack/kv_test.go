package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestKVParser_Default(t *testing.T) {
	input := `level=error msg="connection refused" duration_ms=3.14 count=42`

	p := &KVParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["level"].String() != "error" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "error")
	}
	if fields["msg"].String() != "connection refused" {
		t.Errorf("msg: got %q, want %q", fields["msg"].String(), "connection refused")
	}
	if f := fields["duration_ms"].AsFloat(); f < 3.13 || f > 3.15 {
		t.Errorf("duration_ms: got %v, want ~3.14", f)
	}
	if fields["count"].AsInt() != 42 {
		t.Errorf("count: got %v, want 42", fields["count"])
	}
}

func TestKVParser_CustomDelimiters(t *testing.T) {
	input := `level:error,msg:'hello world',status:200`

	p := &KVParser{Delim: ',', Assign: ':', Quote: '\''}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["level"].String() != "error" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "error")
	}
	if fields["msg"].String() != "hello world" {
		t.Errorf("msg: got %q, want %q", fields["msg"].String(), "hello world")
	}
	if fields["status"].AsInt() != 200 {
		t.Errorf("status: got %v, want 200", fields["status"])
	}
}

func TestKVParser_NoValues(t *testing.T) {
	input := `justkeys without_values`

	p := &KVParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// No key=value pairs → no fields emitted
	if len(fields) != 0 {
		t.Errorf("expected 0 fields, got %d: %v", len(fields), fields)
	}
}

func TestKVParser_Empty(t *testing.T) {
	p := &KVParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestKVParser_BoolInference(t *testing.T) {
	input := `enabled=true disabled=false`

	p := &KVParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["enabled"].AsBool() != true {
		t.Errorf("enabled: got %v, want true", fields["enabled"])
	}
	if fields["disabled"].AsBool() != false {
		t.Errorf("disabled: got %v, want false", fields["disabled"])
	}
}

func TestKVParser_Name(t *testing.T) {
	p := &KVParser{}
	if p.Name() != "kv" {
		t.Errorf("Name: got %q, want %q", p.Name(), "kv")
	}
}
