package syslog

import (
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/syslog"
)

type parser struct {
	cfg *runtimeConfig
}

func newParser(cfg *runtimeConfig) *parser {
	return &parser{cfg: cfg}
}

func (p *parser) parse(raw []byte, source string, receivedAt time.Time) (*event.Event, string) {
	line := string(raw)
	e := event.NewEvent(receivedAt, line)
	e.Source = source
	e.Index = p.cfg.Index
	e.Host = p.cfg.DefaultHostname
	dialect := strings.ToLower(p.cfg.Parser)
	if dialect == "" || dialect == "auto" {
		dialect = syslog.Detect(line)
	}
	switch dialect {
	case syslog.RFC5424, syslog.RFC3164:
		if err := syslog.Parse(e, line, dialect, p.cfg.Location, receivedAt.In(p.cfg.Location).Year()); err != nil {
			e.ParseError = true
			dialect = syslog.Raw
		}
	default:
		dialect = syslog.Raw
	}
	e.SourceType = p.sourceType(dialect)
	return e, dialect
}

func (p *parser) sourceType(dialect string) string {
	base := p.cfg.SourceType
	if base == "" {
		base = "syslog"
	}
	return base + ":" + dialect
}

func locationForConfig(cfg config.SyslogConfig) (*time.Location, error) {
	if cfg.DefaultTimezone == "" || cfg.DefaultTimezone == "Local" {
		return time.Local, nil
	}
	return time.LoadLocation(cfg.DefaultTimezone)
}
