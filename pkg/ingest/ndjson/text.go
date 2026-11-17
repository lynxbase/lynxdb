package ndjson

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/pipeline"
)

// Text turns lines of log text into NDJSON. A line that is a JSON object
// passes unchanged; any other line becomes an event whose time and fields
// an ingest pipeline takes from its text.
type Text struct {
	pipe *pipeline.Pipeline
	// Source, SourceType and Host describe where the lines come from.
	Source, SourceType, Host string
}

// NewText returns a converter that extracts the fields of format, a log
// format that NewParser parses, besides the timestamp and key=value pairs;
// an empty format extracts only those.
func NewText(format string) (*Text, error) {
	pipe := pipeline.DefaultPipeline()
	if format != "" {
		p, err := NewParser(format)
		if err != nil {
			return nil, err
		}
		pipe = pipeline.New(pipe, formatFields{p})
	}

	return &Text{pipe: pipe}, nil
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
