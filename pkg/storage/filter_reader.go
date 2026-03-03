package storage

import (
	"bufio"
	"bytes"
	"io"
)

// FilterReader wraps a bufio.Scanner and a line-level pre-filter.
// Only yields lines that pass the pre-filter, avoiding Event allocation
// for lines that will definitely not match the query (predicate pushdown).
type FilterReader struct {
	scanner      *bufio.Scanner
	preFilter    [][]byte // literals that must all appear in line (AND semantics)
	canPushdown  bool     // false if query has OR, NOT, or no search terms
	linesRead    int64
	linesSkipped int64
	lowerBuf     []byte // reusable buffer for ASCII lowercasing (avoids alloc per line)
}

// NewFilterReader creates a FilterReader that pre-filters lines using the given
// literal byte slices. Only lines containing ALL literals are yielded.
// If literals is nil or empty, all non-blank lines are yielded (no filtering).
func NewFilterReader(r io.Reader, literals [][]byte) *FilterReader {
	fr := &FilterReader{
		scanner: bufio.NewScanner(r),
	}
	fr.scanner.Buffer(make([]byte, 256*1024), 10*1024*1024) // 256KB default, 10MB max

	if len(literals) > 0 {
		fr.preFilter = literals
		fr.canPushdown = true
	}

	return fr
}

// Next returns the next line that passes the pre-filter as a string.
// Returns ("", false) when input is exhausted.
func (fr *FilterReader) Next() (string, bool) {
	for fr.scanner.Scan() {
		line := fr.scanner.Bytes() // zero-copy: slice of scanner's internal buffer
		fr.linesRead++

		if len(line) == 0 || isAllBlank(line) {
			continue
		}

		if fr.canPushdown {
			// Case-insensitive check: lower the line bytes into reusable buffer
			lower := fr.toLowerASCII(line)
			skip := false
			for _, lit := range fr.preFilter {
				if !bytes.Contains(lower, lit) {
					skip = true

					break
				}
			}
			if skip {
				fr.linesSkipped++

				continue // NO allocation, NO parsing, NO Event creation
			}
		}

		// Line passed pre-filter — return as string (copies from scanner buffer)
		return string(line), true
	}

	return "", false
}

// Stats returns (linesRead, linesSkipped) for diagnostics.
func (fr *FilterReader) Stats() (int64, int64) {
	return fr.linesRead, fr.linesSkipped
}

// Err returns the scanner error, if any.
func (fr *FilterReader) Err() error {
	return fr.scanner.Err()
}

// toLowerASCII lowercases ASCII bytes in-place into fr.lowerBuf.
// Grows the buffer as needed. Returns the lowered slice (no allocation
// on the fast path when buffer is large enough).
func (fr *FilterReader) toLowerASCII(src []byte) []byte {
	if cap(fr.lowerBuf) < len(src) {
		fr.lowerBuf = make([]byte, len(src)*2)
	}
	buf := fr.lowerBuf[:len(src)]
	for i, c := range src {
		if c >= 'A' && c <= 'Z' {
			buf[i] = c + 32
		} else {
			buf[i] = c
		}
	}

	return buf
}

// isAllBlank returns true if the byte slice contains only whitespace.
func isAllBlank(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			return false
		}
	}

	return true
}
