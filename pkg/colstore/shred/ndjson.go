package shred

import (
	"bytes"
	"math"
)

// LineError reports a rejected line of an NDJSON chunk.
type LineError struct {
	Line int // zero-based among the non-empty lines
	Err  error
}

// SplitLines returns the prefix of data holding at most maxLines lines and
// what follows it.
func SplitLines(data []byte, maxLines int) (chunk, rest []byte) {
	pos := 0
	for n := 0; n < maxLines && pos < len(data); n++ {
		end := bytes.IndexByte(data[pos:], '\n')
		if end < 0 {
			return data, nil
		}
		pos += end + 1
	}

	return data[:pos], data[pos:]
}

// AddNDJSON appends one row per non-empty line of data and returns the
// number of rows it dropped for being older than in.Since. A line that is
// not a JSON object is kept as text when in.Text is set, and skipped and
// reported in errs otherwise; SplitLines keeps a chunk within the batch's
// remaining rows.
func (b *Batch) AddNDJSON(data []byte, ts int64, in Intake, errs []LineError) ([]LineError, int) {
	since, expired := in.Since, 0
	if since == 0 {
		since = math.MinInt64
	}
	for line := 0; len(data) > 0; {
		end := bytes.IndexByte(data, '\n')
		doc := data
		if end >= 0 {
			doc, data = data[:end], data[end+1:]
		} else {
			data = nil
		}
		if len(bytes.TrimSpace(doc)) == 0 {
			continue
		}
		kept, err := b.addJSON(doc, ts, since)
		switch {
		case err == nil:
		case in.Text:
			kept = b.addText(bytes.TrimSuffix(doc, []byte{'\r'}), ts, since)
		default:
			kept = true
			errs = append(errs, LineError{Line: line, Err: err})
		}
		if !kept {
			expired++
		}
		line++
	}

	return errs, expired
}
