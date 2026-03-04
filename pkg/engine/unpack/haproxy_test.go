package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestHAProxyParser_Basic(t *testing.T) {
	input := `10.0.0.1:56000 [14/Feb/2026:14:52:01.234] web~ app/srv1 10/0/30/69/109 200 1234 - - ---- 1/1/0/0/0 0/0 "GET /api/health HTTP/1.1"`

	p := &HAProxyParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	expectStr := map[string]string{
		"client_ip":  "10.0.0.1",
		"timestamp":  "14/Feb/2026:14:52:01.234",
		"frontend":   "web",
		"backend":    "app",
		"server":     "srv1",
		"term_state": "----",
		"method":     "GET",
		"uri":        "/api/health",
		"protocol":   "HTTP/1.1",
	}
	for k, want := range expectStr {
		got := fields[k].String()
		if got != want {
			t.Errorf("%s: got %q, want %q", k, got, want)
		}
	}

	expectInt := map[string]int64{
		"client_port": 56000,
		"tq":          10,
		"tw":          0,
		"tc":          30,
		"tr":          69,
		"tt":          109,
		"status":      200,
		"bytes":       1234,
		"actconn":     1,
		"feconn":      1,
		"beconn":      0,
		"srv_conn":    0,
		"retries":     0,
	}
	for k, want := range expectInt {
		got := fields[k].AsInt()
		if got != want {
			t.Errorf("%s: got %d, want %d", k, got, want)
		}
	}
}

func TestHAProxyParser_WithSyslogPrefix(t *testing.T) {
	input := `Feb 14 14:52:01 lb1 haproxy[1234]: 192.168.1.50:12345 [14/Feb/2026:14:52:01.000] frontend backend/server 0/0/1/5/6 200 512 - - ---- 10/5/2/1/0 0/0 "POST /submit HTTP/1.1"`

	p := &HAProxyParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["client_ip"].String() != "192.168.1.50" {
		t.Errorf("client_ip: got %q, want %q", fields["client_ip"].String(), "192.168.1.50")
	}
	if fields["method"].String() != "POST" {
		t.Errorf("method: got %q, want %q", fields["method"].String(), "POST")
	}
	if fields["status"].AsInt() != 200 {
		t.Errorf("status: got %v, want 200", fields["status"])
	}
}

func TestHAProxyParser_NegativeTimings(t *testing.T) {
	// HAProxy uses -1 for aborted connections.
	input := `10.0.0.1:80 [01/Jan/2026:00:00:00.000] fe be/srv -1/0/-1/-1/0 503 0 - - CD-- 0/0/0/0/0 0/0 "GET / HTTP/1.0"`

	p := &HAProxyParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(input, func(key string, val event.Value) bool {
		fields[key] = val
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if fields["tq"].AsInt() != -1 {
		t.Errorf("tq: got %v, want -1", fields["tq"])
	}
	if fields["status"].AsInt() != 503 {
		t.Errorf("status: got %v, want 503", fields["status"])
	}
	if fields["term_state"].String() != "CD--" {
		t.Errorf("term_state: got %q, want %q", fields["term_state"].String(), "CD--")
	}
}

func TestHAProxyParser_Empty(t *testing.T) {
	p := &HAProxyParser{}
	err := p.Parse("", func(key string, val event.Value) bool {
		t.Fatalf("should not emit on empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
}

func TestHAProxyParser_Name(t *testing.T) {
	p := &HAProxyParser{}
	if p.Name() != "haproxy" {
		t.Errorf("Name: got %q, want %q", p.Name(), "haproxy")
	}
}
