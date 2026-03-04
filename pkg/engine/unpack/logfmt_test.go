package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestLogfmtParser_BasicKV(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`ts=2026-02-14T14:52:01Z level=error msg=timeout`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "ts", "2026-02-14T14:52:01Z")
	assertStringValue(t, fields, "level", "error")
	assertStringValue(t, fields, "msg", "timeout")
}

func TestLogfmtParser_QuotedValues(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`level=info msg="user logged in" user=admin`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "level", "info")
	assertStringValue(t, fields, "msg", "user logged in")
	assertStringValue(t, fields, "user", "admin")
}

func TestLogfmtParser_NumericInference(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`status=200 duration_ms=45 ratio=0.95 active=true`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertIntValue(t, fields, "status", 200)
	assertIntValue(t, fields, "duration_ms", 45)

	v, ok := fields["ratio"]
	if !ok || v.Type() != event.FieldTypeFloat {
		t.Errorf("ratio: got type %s, want float", v.Type())
	}

	bv, ok := fields["active"]
	if !ok || bv.Type() != event.FieldTypeBool || !bv.AsBool() {
		t.Errorf("active: got %v, want true", bv)
	}
}

func TestLogfmtParser_EmptyValue(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`key= other=val`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Empty value should be empty string.
	v, ok := fields["key"]
	if !ok {
		t.Fatal("missing field: key")
	}
	if v.String() != "" {
		t.Errorf("key: got %q, want empty string", v.String())
	}

	assertStringValue(t, fields, "other", "val")
}

func TestLogfmtParser_TabSeparated(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse("level=error\thost=web-01", func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "level", "error")
	assertStringValue(t, fields, "host", "web-01")
}

func TestLogfmtParser_EmptyInput(t *testing.T) {
	p := &LogfmtParser{}
	err := p.Parse(``, func(k string, v event.Value) bool {
		t.Errorf("should not emit for empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
}

func TestLogfmtParser_ShortCircuit(t *testing.T) {
	p := &LogfmtParser{}
	count := 0
	err := p.Parse(`a=1 b=2 c=3`, func(k string, v event.Value) bool {
		count++
		return false
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if count != 1 {
		t.Errorf("emit count: got %d, want 1", count)
	}
}

func TestLogfmtParser_NoEquals(t *testing.T) {
	p := &LogfmtParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`bare_token level=error`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// "bare_token" has no "=" so should be skipped.
	if _, ok := fields["bare_token"]; ok {
		t.Error("bare_token should not be emitted")
	}
	assertStringValue(t, fields, "level", "error")
}

func TestLogfmtParser_Name(t *testing.T) {
	p := &LogfmtParser{}
	if p.Name() != "logfmt" {
		t.Errorf("Name: got %q, want %q", p.Name(), "logfmt")
	}
}
