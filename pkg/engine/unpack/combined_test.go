package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestCombinedParser_FullLine(t *testing.T) {
	p := &CombinedParser{}
	fields := make(map[string]event.Value)
	line := `192.168.1.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08"`
	err := p.Parse(line, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "client_ip", "192.168.1.1")

	// ident should be null ("-")
	if v, ok := fields["ident"]; !ok || !v.IsNull() {
		t.Errorf("ident: got %v, want null", v)
	}

	assertStringValue(t, fields, "user", "frank")
	assertStringValue(t, fields, "timestamp", "10/Oct/2000:13:55:36 -0700")
	assertStringValue(t, fields, "request", "GET /apache_pb.gif HTTP/1.0")
	assertStringValue(t, fields, "method", "GET")
	assertStringValue(t, fields, "uri", "/apache_pb.gif")
	assertStringValue(t, fields, "protocol", "HTTP/1.0")
	assertIntValue(t, fields, "status", 200)
	assertIntValue(t, fields, "bytes", 2326)
	assertStringValue(t, fields, "referer", "http://www.example.com/start.html")
	assertStringValue(t, fields, "user_agent", "Mozilla/4.08")
}

func TestCombinedParser_MissingFields(t *testing.T) {
	p := &CombinedParser{}
	fields := make(map[string]event.Value)
	line := `10.0.0.1 - - [14/Feb/2026:14:23:01 +0000] "POST /api HTTP/1.1" 500 - "-" "-"`
	err := p.Parse(line, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "client_ip", "10.0.0.1")

	// user and ident should be null.
	if v, ok := fields["user"]; !ok || !v.IsNull() {
		t.Errorf("user: got %v, want null", v)
	}

	assertStringValue(t, fields, "method", "POST")
	assertStringValue(t, fields, "uri", "/api")
	assertIntValue(t, fields, "status", 500)

	// bytes = "-" → null
	if v, ok := fields["bytes"]; !ok || !v.IsNull() {
		t.Errorf("bytes: got %v, want null", v)
	}

	// referer = "-" → null
	if v, ok := fields["referer"]; !ok || !v.IsNull() {
		t.Errorf("referer: got %v, want null", v)
	}
}

func TestCombinedParser_EmptyInput(t *testing.T) {
	p := &CombinedParser{}
	err := p.Parse(``, func(k string, v event.Value) bool {
		t.Errorf("should not emit for empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
}

func TestCombinedParser_Malformed(t *testing.T) {
	p := &CombinedParser{}
	// Incomplete line should not panic.
	fields := make(map[string]event.Value)
	err := p.Parse(`192.168.1.1`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	// Should at least get client_ip.
	assertStringValue(t, fields, "client_ip", "192.168.1.1")
}

func TestCombinedParser_Name(t *testing.T) {
	p := &CombinedParser{}
	if p.Name() != "combined" {
		t.Errorf("Name: got %q, want %q", p.Name(), "combined")
	}
}
