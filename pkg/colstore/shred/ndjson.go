package shred

import "bytes"

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

// AddNDJSON appends one row per non-empty line of data, skipping lines that
// are not JSON objects and reporting them in errs; SplitLines keeps a chunk
// within the batch's remaining rows.
func (b *Batch) AddNDJSON(data []byte, ts int64, errs []LineError) []LineError {
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
		if err := b.AddJSON(doc, ts); err != nil {
			errs = append(errs, LineError{Line: line, Err: err})
		}
		line++
	}

	return errs
}
