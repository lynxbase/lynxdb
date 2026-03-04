package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestNginxErrorParser_Basic(t *testing.T) {
	input := `2026/02/14 14:52:01 [error] 12345#67: *890 upstream timed out, client: 10.0.1.5, server: api.example.com, request: "GET /api HTTP/1.1"`

	p := &NginxErrorParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expect := map[string]string{
		"timestamp": "2026/02/14 14:52:01",
		"severity":  "error",
		"client":    "10.0.1.5",
		"server":    "api.example.com",
		"request":   "GET /api HTTP/1.1",
		"method":    "GET",
		"uri":       "/api",
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
	if fields["tid"].AsInt() != 67 {
		t.Errorf("tid: got %v, want 67", fields["tid"])
	}
	if fields["cid"].AsInt() != 890 {
		t.Errorf("cid: got %v, want 890", fields["cid"])
	}
}

func TestNginxErrorParser_NoKVPairs(t *testing.T) {
	input := `2026/01/01 00:00:00 [warn] 1#1: *0 simple warning message`

	p := &NginxErrorParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["severity"].String() != "warn" {
		t.Errorf("severity: got %q, want %q", fields["severity"].String(), "warn")
	}
	if fields["message"].String() != "simple warning message" {
		t.Errorf("message: got %q, want %q", fields["message"].String(), "simple warning message")
	}
}

func TestNginxErrorParser_Empty(t *testing.T) {
	p := &NginxErrorParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestNginxErrorParser_Name(t *testing.T) {
	p := &NginxErrorParser{}
	if p.Name() != "nginx_error" {
		t.Errorf("Name: got %q, want %q", p.Name(), "nginx_error")
	}
}
