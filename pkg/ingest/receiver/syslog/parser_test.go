package syslog

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/ingest/syslog"
)

func TestParseRFC5424(t *testing.T) {
	cfg, err := normalizeConfig(config.DefaultConfig().Syslog)
	if err != nil {
		t.Fatal(err)
	}
	line := `<34>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog 8710 ID47 [exampleSDID@32473 iut="3" eventSource="Application"] BOMAn application event log entry`
	e, dialect := newParser(cfg).parse([]byte(line), "tcp://127.0.0.1:1", time.Unix(0, 0))
	if dialect != syslog.RFC5424 {
		t.Fatalf("dialect = %s", dialect)
	}
	if got := e.Host; got != "mymachine.example.com" {
		t.Fatalf("host = %q", got)
	}
	if got := e.GetField("facility").AsInt(); got != 4 {
		t.Fatalf("facility = %d", got)
	}
	if got := e.GetField("severity_label").AsString(); got != "crit" {
		t.Fatalf("severity_label = %q", got)
	}
	if got := e.GetField("sd_exampleSDID_32473_iut").AsString(); got != "3" {
		t.Fatalf("structured data = %q", got)
	}
}

func TestParseRFC3164(t *testing.T) {
	cfg, err := normalizeConfig(config.DefaultConfig().Syslog)
	if err != nil {
		t.Fatal(err)
	}
	received := time.Date(2026, time.May, 2, 12, 0, 0, 0, time.Local)
	line := `<13>Feb  5 17:32:18 host app[42]: hello world`
	e, dialect := newParser(cfg).parse([]byte(line), "udp://127.0.0.1:1", received)
	if dialect != syslog.RFC3164 {
		t.Fatalf("dialect = %s", dialect)
	}
	if got := e.Time.Year(); got != 2026 {
		t.Fatalf("year = %d", got)
	}
	if got := e.Host; got != "host" {
		t.Fatalf("host = %q", got)
	}
	if got := e.GetField("app_name").AsString(); got != "app" {
		t.Fatalf("app_name = %q", got)
	}
	if got := e.GetField("procid").AsString(); got != "42" {
		t.Fatalf("procid = %q", got)
	}
}

func TestParseRawFallback(t *testing.T) {
	cfg, err := normalizeConfig(config.DefaultConfig().Syslog)
	if err != nil {
		t.Fatal(err)
	}
	e, dialect := newParser(cfg).parse([]byte("not syslog"), "udp://peer", time.Unix(10, 0))
	if dialect != syslog.Raw {
		t.Fatalf("dialect = %s", dialect)
	}
	if e.SourceType != "syslog:raw" {
		t.Fatalf("sourcetype = %q", e.SourceType)
	}
}
