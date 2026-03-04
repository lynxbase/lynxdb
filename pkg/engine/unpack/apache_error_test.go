package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestApacheErrorParser_Basic(t *testing.T) {
	input := `[Fri Feb 14 14:52:01.234567 2026] [ssl:error] [pid 12345:tid 67890] [client 192.168.1.100:52436] AH02032: Hostname provided via SNI not found`

	p := &ApacheErrorParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"timestamp":  "Fri Feb 14 14:52:01.234567 2026",
		"module":     "ssl",
		"severity":   "error",
		"client_ip":  "192.168.1.100",
		"error_code": "AH02032",
		"message":    "Hostname provided via SNI not found",
	}
	for k, want := range expect {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	if fields["pid"].AsInt() != 12345 {
		t.Errorf("pid: got %v, want 12345", fields["pid"])
	}
	if fields["tid"].AsInt() != 67890 {
		t.Errorf("tid: got %v, want 67890", fields["tid"])
	}
	if fields["client_port"].AsInt() != 52436 {
		t.Errorf("client_port: got %v, want 52436", fields["client_port"])
	}
}

func TestApacheErrorParser_NoClient(t *testing.T) {
	input := `[Sat Jan 01 00:00:00.000000 2026] [core:notice] [pid 1] AH00094: Command line`

	p := &ApacheErrorParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["module"].String() != "core" {
		t.Errorf("module: got %q, want %q", fields["module"].String(), "core")
	}
	if fields["severity"].String() != "notice" {
		t.Errorf("severity: got %q, want %q", fields["severity"].String(), "notice")
	}
	if fields["pid"].AsInt() != 1 {
		t.Errorf("pid: got %v, want 1", fields["pid"])
	}
	if _, ok := fields["client_ip"]; ok {
		t.Errorf("client_ip should not be present")
	}
	if fields["error_code"].String() != "AH00094" {
		t.Errorf("error_code: got %q, want %q", fields["error_code"].String(), "AH00094")
	}
}

func TestApacheErrorParser_SeverityOnly(t *testing.T) {
	// Some older Apache configs only emit severity, no module.
	input := `[Mon Mar 01 12:00:00 2026] [error] [pid 100] Some error occurred`

	p := &ApacheErrorParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["severity"].String() != "error" {
		t.Errorf("severity: got %q, want %q", fields["severity"].String(), "error")
	}
	if fields["message"].String() != "Some error occurred" {
		t.Errorf("message: got %q, want %q", fields["message"].String(), "Some error occurred")
	}
}

func TestApacheErrorParser_Empty(t *testing.T) {
	p := &ApacheErrorParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestApacheErrorParser_Name(t *testing.T) {
	p := &ApacheErrorParser{}
	if p.Name() != "apache_error" {
		t.Errorf("Name: got %q, want %q", p.Name(), "apache_error")
	}
}
