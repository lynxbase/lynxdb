package ndjson

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
	"github.com/lynxbase/lynxdb/pkg/ingest/syslog"
)

// Text turns lines of log text into NDJSON. A line that is a JSON object
// passes unchanged; any other line becomes an event whose time and fields
// an ingest pipeline takes from its text.
type Text struct {
	pipe *pipeline.Pipeline
	// Source, SourceType and Host describe where the lines come from.
	Source, SourceType, Host string
}

// NewText returns a converter that extracts the fields of format, syslog or
// combined, besides the timestamp and key=value pairs; an empty format
// extracts only those. The fields of the format come first, so a key=value
// pair of the same name does not replace them.
func NewText(format string) (*Text, error) {
	var stages []pipeline.Stage
	switch strings.ToLower(format) {
	case "":
	case "syslog":
		stages = append(stages, syslogMessages{})
	case "combined":
		stages = append(stages, formatFields{&CombinedParser{}})
	default:
		return nil, fmt.Errorf("ndjson: unknown format %q (supported: syslog, combined)", format)
	}

	return &Text{pipe: pipeline.New(append(stages, pipeline.DefaultPipeline())...)}, nil
}

// syslogMessages reads the text of each event as a syslog message, as the
// syslog receiver does, taking the time of an RFC 3164 message in UTC and
// the current year. A line that is not one is left to the rest of the
// pipeline.
type syslogMessages struct{}

func (syslogMessages) Process(events []*event.Event) ([]*event.Event, error) {
	year := time.Now().UTC().Year()
	for _, ev := range events {
		if dialect := syslog.Detect(ev.Raw); dialect != syslog.Raw {
			_ = syslog.Parse(ev, ev.Raw, dialect, time.UTC, year)
		}
	}

	return events, nil
}

// formatFields sets the fields a format parser finds in the text of each
// event, keeping the fields the event already has.
type formatFields struct{ parser FormatParser }

func (f formatFields) Process(events []*event.Event) ([]*event.Event, error) {
	for _, ev := range events {
		if ev.Fields == nil {
			ev.Fields = make(map[string]event.Value)
		}
		_ = f.parser.Parse(ev.Raw, func(key string, val event.Value) bool {
			if _, ok := ev.Fields[key]; !ok {
				ev.Fields[key] = val
			}

			return true
		})
	}

	return events, nil
}

const (
	maxLineBytes = 1 << 20
	textBatch    = 1024
	chunkBytes   = 4 << 20
)

// Read converts the lines of r and calls emit with the NDJSON of whole
// lines, a few megabytes at a time; emit must not keep the slice.
func (t *Text) Read(r io.Reader, emit func(ndjson []byte) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), maxLineBytes)
	var out []byte
	pending := make([]*event.Event, 0, textBatch)
	flushText := func() error {
		if len(pending) == 0 {
			return nil
		}
		evs, err := t.pipe.Process(pending)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			out = append(AppendEvent(out, ev), '\n')
		}
		pending = pending[:0]

		return nil
	}
	for sc.Scan() {
		line := bytes.TrimRight(sc.Bytes(), "\r")
		trimmed := bytes.TrimLeft(line, " \t")
		switch {
		case len(trimmed) == 0:
			continue
		case trimmed[0] == '{':
			out = append(append(out, line...), '\n')
		default:
			ev := event.NewEvent(time.Time{}, string(line))
			ev.Source, ev.SourceType, ev.Host = t.Source, t.SourceType, t.Host
			pending = append(pending, ev)
			if len(pending) == textBatch {
				if err := flushText(); err != nil {
					return err
				}
			}
		}
		if len(out) >= chunkBytes {
			if err := emit(out); err != nil {
				return err
			}
			out = out[:0]
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if err := flushText(); err != nil {
		return err
	}
	if len(out) == 0 {
		return nil
	}

	return emit(out)
}
