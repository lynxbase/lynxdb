package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestW3CParser_Basic(t *testing.T) {
	p := NewW3CParser("date time s-ip cs-method cs-uri-stem sc-status")

	input := `2026-02-14 14:52:01 10.0.0.1 GET /api/health 200`

	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"date":        "2026-02-14",
		"time":        "14:52:01",
		"s_ip":        "10.0.0.1",
		"cs_method":   "GET",
		"cs_uri_stem": "/api/health",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["sc_status"].AsInt() != 200 {
		t.Errorf("sc_status: got %v, want 200", fields["sc_status"])
	}
}

func TestW3CParser_EmbeddedFieldsDirective(t *testing.T) {
	p := &W3CParser{}

	// First line is the fields directive.
	err := p.Parse("#Fields: date time cs-uri-stem sc-status", func(key string, val event.Value) bool {
		t.Fatal("should not emit on directive line")
		return true
	})
	if err != nil {
		t.Fatalf("Parse directive: %v", err)
	}

	// Second line is data.
	fields := make(map[string]event.Value)
	err = p.Parse("2026-01-01 00:00:00 /index.html 200", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse data: %v", err)
	}

	if fields["cs_uri_stem"].String() != "/index.html" {
		t.Errorf("cs_uri_stem: got %q, want %q", fields["cs_uri_stem"].String(), "/index.html")
	}
	if fields["sc_status"].AsInt() != 200 {
		t.Errorf("sc_status: got %v, want 200", fields["sc_status"])
	}
}

func TestW3CParser_MissingValues(t *testing.T) {
	p := NewW3CParser("date time cs-uri-stem sc-status sc-bytes")

	// "-" represents missing value in W3C format.
	fields := make(map[string]event.Value)
	err := p.Parse("2026-01-01 00:00:00 /page 200 -", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if _, ok := fields["sc_bytes"]; ok {
		t.Error("sc_bytes should not be emitted for '-' value")
	}
	if fields["sc_status"].AsInt() != 200 {
		t.Errorf("sc_status: got %v, want 200", fields["sc_status"])
	}
}

func TestW3CParser_CommentLines(t *testing.T) {
	p := NewW3CParser("date time")

	// Comment lines are skipped.
	err := p.Parse("#Version: 1.0", func(key string, val event.Value) bool {
		t.Fatal("should not emit on comment line")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestW3CParser_NoFields(t *testing.T) {
	p := &W3CParser{}

	// No fields configured — should silently skip.
	called := false
	err := p.Parse("some data line", func(key string, val event.Value) bool {
		called = true
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if called {
		t.Error("should not emit when no fields configured")
	}
}

func TestW3CParser_FewerValuesThanFields(t *testing.T) {
	p := NewW3CParser("date time method uri status")

	fields := make(map[string]event.Value)
	err := p.Parse("2026-01-01 00:00:00 GET", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if len(fields) != 3 {
		t.Errorf("expected 3 fields, got %d", len(fields))
	}
}

func TestW3CParser_Empty(t *testing.T) {
	p := NewW3CParser("date time")
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestW3CParser_Name(t *testing.T) {
	p := &W3CParser{}
	if p.Name() != "w3c" {
		t.Errorf("Name: got %q, want %q", p.Name(), "w3c")
	}
}

func TestW3CParser_HeaderWithPrefix(t *testing.T) {
	// NewW3CParser should handle both with and without "#Fields:" prefix.
	p := NewW3CParser("#Fields: date time status")

	fields := make(map[string]event.Value)
	err := p.Parse("2026-01-01 12:00:00 404", func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["status"].AsInt() != 404 {
		t.Errorf("status: got %v, want 404", fields["status"])
	}
}
