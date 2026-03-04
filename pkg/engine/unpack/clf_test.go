package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestCLFParser_Basic(t *testing.T) {
	input := `127.0.0.1 - frank [10/Oct/2025:13:55:36 -0700] "GET /api/health HTTP/1.1" 200 2326`

	p := &CLFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"client_ip": "127.0.0.1",
		"user":      "frank",
		"method":    "GET",
		"uri":       "/api/health",
		"protocol":  "HTTP/1.1",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	// status should be numeric
	if fields["status"].AsInt() != 200 {
		t.Errorf("status: got %v, want 200", fields["status"])
	}
	// bytes should be numeric
	if fields["bytes"].AsInt() != 2326 {
		t.Errorf("bytes: got %v, want 2326", fields["bytes"])
	}
	// ident="-" should be null
	if !fields["ident"].IsNull() {
		t.Errorf("ident: got %v, want null", fields["ident"])
	}
}

func TestCLFParser_MissingBytes(t *testing.T) {
	input := `10.0.0.1 - - [01/Jan/2026:00:00:00 +0000] "POST /submit HTTP/1.1" 404 -`

	p := &CLFParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if !fields["bytes"].IsNull() {
		t.Errorf("bytes (dash): got %v, want null", fields["bytes"])
	}
	if fields["status"].AsInt() != 404 {
		t.Errorf("status: got %v, want 404", fields["status"])
	}
}

func TestCLFParser_Empty(t *testing.T) {
	p := &CLFParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestCLFParser_Name(t *testing.T) {
	p := &CLFParser{}
	if p.Name() != "clf" {
		t.Errorf("Name: got %q, want %q", p.Name(), "clf")
	}
}
