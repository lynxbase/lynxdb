package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestPatternParser_Basic(t *testing.T) {
	p, err := NewPatternParser("%{name} [%{timestamp:timestamp}] %{status:int}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	fields := make(map[string]event.Value)
	err = p.Parse("web-01 [2026-02-14 14:52:01] 200", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["name"].String() != "web-01" {
		t.Errorf("name: got %q, want %q", fields["name"].String(), "web-01")
	}
	if fields["timestamp"].String() != "2026-02-14 14:52:01" {
		t.Errorf("timestamp: got %q, want %q", fields["timestamp"].String(), "2026-02-14 14:52:01")
	}
	if fields["status"].AsInt() != 200 {
		t.Errorf("status: got %v, want 200", fields["status"])
	}
}

func TestPatternParser_FloatType(t *testing.T) {
	p, err := NewPatternParser("duration=%{dur:float}ms")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	fields := make(map[string]event.Value)
	err = p.Parse("duration=3.14ms", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["dur"].AsFloat() != 3.14 {
		t.Errorf("dur: got %v, want 3.14", fields["dur"])
	}
}

func TestPatternParser_RestType(t *testing.T) {
	p, err := NewPatternParser("[%{level}] %{message:rest}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	fields := make(map[string]event.Value)
	err = p.Parse("[ERROR] connection refused: host=db1 port=5432", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["level"].String() != "ERROR" {
		t.Errorf("level: got %q, want %q", fields["level"].String(), "ERROR")
	}
	if fields["message"].String() != "connection refused: host=db1 port=5432" {
		t.Errorf("message: got %q, want %q", fields["message"].String(), "connection refused: host=db1 port=5432")
	}
}

func TestPatternParser_NoMatch(t *testing.T) {
	p, err := NewPatternParser("%{name} %{status:int}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	called := false
	err = p.Parse("this doesn't match the pattern at all", func(key string, val event.Value) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if called {
		t.Error("should not emit when pattern doesn't match")
	}
}

func TestPatternParser_LiteralSpecialChars(t *testing.T) {
	// Pattern with regex-special literal characters that need escaping.
	p, err := NewPatternParser("(%{name}) [%{code:int}]")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	fields := make(map[string]event.Value)
	err = p.Parse("(web-01) [200]", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["name"].String() != "web-01" {
		t.Errorf("name: got %q, want %q", fields["name"].String(), "web-01")
	}
	if fields["code"].AsInt() != 200 {
		t.Errorf("code: got %v, want 200", fields["code"])
	}
}

func TestPatternParser_EmptyPattern(t *testing.T) {
	_, err := NewPatternParser("")
	if err == nil {
		t.Fatal("expected error for empty pattern")
	}
}

func TestPatternParser_UnclosedPlaceholder(t *testing.T) {
	_, err := NewPatternParser("hello %{name")
	if err == nil {
		t.Fatal("expected error for unclosed placeholder")
	}
}

func TestPatternParser_UnknownType(t *testing.T) {
	_, err := NewPatternParser("%{name:badtype}")
	if err == nil {
		t.Fatal("expected error for unknown type")
	}
}

func TestPatternParser_ShortCircuit(t *testing.T) {
	p, err := NewPatternParser("%{a} %{b} %{c}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	count := 0
	err = p.Parse("x y z", func(key string, val event.Value) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 emit, got %d", count)
	}
}

func TestPatternParser_Name(t *testing.T) {
	p, err := NewPatternParser("%{x}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}
	if p.Name() != "pattern" {
		t.Errorf("Name: got %q, want %q", p.Name(), "pattern")
	}
}

func TestPatternParser_Empty(t *testing.T) {
	p, err := NewPatternParser("%{x}")
	if err != nil {
		t.Fatalf("NewPatternParser: %v", err)
	}

	err = p.Parse("", func(key string, val event.Value) bool {
		t.Fatal("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}
