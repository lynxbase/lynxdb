package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestSyslogParser_RFC3164(t *testing.T) {
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`<34>Feb 14 14:52:01 web-01 sshd[12345]: Failed password for root`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// PRI=34 → facility=4, severity=2
	assertIntValue(t, fields, "priority", 34)
	assertIntValue(t, fields, "facility", 4)
	assertIntValue(t, fields, "severity", 2)
	assertStringValue(t, fields, "facility_name", "auth")
	assertStringValue(t, fields, "severity_name", "critical")
	assertStringValue(t, fields, "format", "rfc3164")
	assertStringValue(t, fields, "timestamp", "Feb 14 14:52:01")
	assertStringValue(t, fields, "hostname", "web-01")
	assertStringValue(t, fields, "appname", "sshd")
	assertStringValue(t, fields, "procid", "12345")
	assertStringValue(t, fields, "message", "Failed password for root")
}

func TestSyslogParser_RFC3164_NoPID(t *testing.T) {
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`<13>Mar  5 09:00:00 router kernel: TCP connection timeout`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertIntValue(t, fields, "priority", 13)
	// PRI=13 → facility=1 (user), severity=5 (notice)
	assertStringValue(t, fields, "facility_name", "user")
	assertStringValue(t, fields, "severity_name", "notice")
	assertStringValue(t, fields, "format", "rfc3164")
	assertStringValue(t, fields, "hostname", "router")
	assertStringValue(t, fields, "appname", "kernel")
	assertStringValue(t, fields, "message", "TCP connection timeout")

	// No PID, so procid should not be present.
	if _, ok := fields["procid"]; ok {
		t.Error("procid should not be present when no PID in input")
	}
}

func TestSyslogParser_RFC5424(t *testing.T) {
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	input := `<165>1 2026-02-14T14:52:01.123Z web-01 myapp 1234 ID47 [exampleSDID@32473 iut="3"] Hello from RFC 5424`
	err := p.Parse(input, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertIntValue(t, fields, "priority", 165)
	assertIntValue(t, fields, "facility", 20)
	assertIntValue(t, fields, "severity", 5)
	// PRI=165 → facility=20 (local4), severity=5 (notice)
	assertStringValue(t, fields, "facility_name", "local4")
	assertStringValue(t, fields, "severity_name", "notice")
	assertStringValue(t, fields, "format", "rfc5424")
	assertIntValue(t, fields, "version", 1)
	assertStringValue(t, fields, "timestamp", "2026-02-14T14:52:01.123Z")
	assertStringValue(t, fields, "hostname", "web-01")
	assertStringValue(t, fields, "appname", "myapp")
	assertStringValue(t, fields, "procid", "1234")
	assertStringValue(t, fields, "msgid", "ID47")
	assertStringValue(t, fields, "message", "Hello from RFC 5424")
}

func TestSyslogParser_RFC5424_NilValues(t *testing.T) {
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	input := `<14>1 2026-01-01T00:00:00Z host app - - - Some message`
	err := p.Parse(input, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// "-" values in RFC 5424 should be null.
	if v, ok := fields["procid"]; !ok || !v.IsNull() {
		t.Errorf("procid: got %v, want null", v)
	}
	if v, ok := fields["msgid"]; !ok || !v.IsNull() {
		t.Errorf("msgid: got %v, want null", v)
	}
	if v, ok := fields["structured_data"]; !ok || !v.IsNull() {
		t.Errorf("structured_data: got %v, want null", v)
	}
}

func TestSyslogParser_NoPRI(t *testing.T) {
	// Without a PRI header, the parser now falls through to RFC 3164 body parsing.
	// A non-syslog message should still emit at least a "message" field.
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`not a syslog message`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	// Short/non-matching input goes to the message fallback in parseRFC3164.
	if len(fields) == 0 {
		t.Error("expected at least one field emitted")
	}
}

func TestSyslogParser_RFC3164_NoPRI(t *testing.T) {
	// Messages from log shippers often lack the PRI header.
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`Mar 05 01:16:54 busybox cron[9242]: Accepted publickey for user from 10.0.123.206 port 22 ssh2`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "timestamp", "Mar 05 01:16:54")
	assertStringValue(t, fields, "hostname", "busybox")
	assertStringValue(t, fields, "appname", "cron")
	assertStringValue(t, fields, "procid", "9242")
	assertStringValue(t, fields, "message", "Accepted publickey for user from 10.0.123.206 port 22 ssh2")

	// No PRI fields should be present.
	for _, key := range []string{"priority", "facility", "severity", "facility_name", "severity_name", "format"} {
		if _, ok := fields[key]; ok {
			t.Errorf("field %q should not be present without PRI header", key)
		}
	}
}

func TestSyslogParser_EmptyInput(t *testing.T) {
	p := &SyslogParser{}
	err := p.Parse(``, func(k string, v event.Value) bool {
		t.Errorf("should not emit for empty input")
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
}

func TestSyslogParser_RFC5424_NilValues_EnrichedFields(t *testing.T) {
	// PRI=14 → facility=1 (user), severity=6 (info)
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	input := `<14>1 2026-01-01T00:00:00Z host app - - - Some message`
	err := p.Parse(input, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "facility_name", "user")
	assertStringValue(t, fields, "severity_name", "info")
	assertStringValue(t, fields, "format", "rfc5424")
}

func TestSyslogParser_BoundaryPRI_KernEmergency(t *testing.T) {
	// PRI=0 → facility=0 (kern), severity=0 (emergency)
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`<0>Jan  1 00:00:00 host app: kernel panic`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertIntValue(t, fields, "priority", 0)
	assertIntValue(t, fields, "facility", 0)
	assertIntValue(t, fields, "severity", 0)
	assertStringValue(t, fields, "facility_name", "kern")
	assertStringValue(t, fields, "severity_name", "emergency")
	assertStringValue(t, fields, "format", "rfc3164")
}

func TestSyslogParser_BoundaryPRI_Local7Debug(t *testing.T) {
	// PRI=191 → facility=23 (local7), severity=7 (debug)
	p := &SyslogParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`<191>Feb 10 12:00:00 myhost myapp: debug msg`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertIntValue(t, fields, "priority", 191)
	assertIntValue(t, fields, "facility", 23)
	assertIntValue(t, fields, "severity", 7)
	assertStringValue(t, fields, "facility_name", "local7")
	assertStringValue(t, fields, "severity_name", "debug")
	assertStringValue(t, fields, "format", "rfc3164")
}

func TestSyslogParser_Name(t *testing.T) {
	p := &SyslogParser{}
	if p.Name() != "syslog" {
		t.Errorf("Name: got %q, want %q", p.Name(), "syslog")
	}
}
