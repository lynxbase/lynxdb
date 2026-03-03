package pipeline

import (
	"context"
	"regexp"
	"strings"

	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
)

// RexIterator performs regex field extraction per batch.
type RexIterator struct {
	child         Iterator
	field         string // source field (default: _raw)
	pattern       *regexp.Regexp
	groups        []string // named capture group names
	literalPrefix string   // literal prefix for fast skip
}

// NewRexIterator creates a regex extraction operator.
func NewRexIterator(child Iterator, field, pattern string) (*RexIterator, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return &RexIterator{
		child:         child,
		field:         field,
		pattern:       re,
		groups:        re.SubexpNames(),
		literalPrefix: extractLiteralPrefix(pattern),
	}, nil
}

// extractLiteralPrefix returns the leading literal string from a regex pattern.
// E.g. `\/archives\/edgar\/data\/` → `/archives/edgar/data/`.
func extractLiteralPrefix(pattern string) string {
	var buf strings.Builder
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		// Stop at any regex metacharacter
		switch c {
		case '.', '*', '+', '?', '(', ')', '[', '{', '|', '^', '$':
			return buf.String()
		case '\\':
			// Escaped literal character
			if i+1 < len(pattern) {
				next := pattern[i+1]
				// Only treat as literal if it's a punctuation escape
				if next == '/' || next == '.' || next == '-' || next == '_' ||
					next == '\\' || next == '"' || next == '\'' {
					buf.WriteByte(next)
					i += 2

					continue
				}
				// \d, \w, \s etc are not literals
				return buf.String()
			}

			return buf.String()
		default:
			buf.WriteByte(c)
			i++
		}
	}

	return buf.String()
}

func (r *RexIterator) Init(ctx context.Context) error {
	return r.child.Init(ctx)
}

func (r *RexIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := r.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	srcCol := batch.Columns[r.field]
	if srcCol == nil {
		return batch, nil
	}

	// Pre-allocate output columns for named capture groups.
	for _, name := range r.groups {
		if name == "" {
			continue
		}
		if col, exists := batch.Columns[name]; !exists {
			batch.Columns[name] = make([]event.Value, batch.Len)
		} else if len(col) < batch.Len {
			extended := make([]event.Value, batch.Len)
			copy(extended, col)
			batch.Columns[name] = extended
		}
	}

	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			break
		}
		src := srcCol[i]
		if src.IsNull() {
			continue
		}
		s := src.String()
		// Fast skip: if literal prefix is set and not found, skip regex
		if r.literalPrefix != "" && !strings.Contains(s, r.literalPrefix) {
			continue
		}
		// Use FindStringSubmatchIndex to avoid allocating a []string per row.
		// Returns []int indices into the original string — no string copies.
		indices := r.pattern.FindStringSubmatchIndex(s)
		if indices == nil {
			continue
		}
		for j, name := range r.groups {
			if name == "" {
				continue
			}
			startIdx := 2 * j
			endIdx := 2*j + 1
			if startIdx >= len(indices) || endIdx >= len(indices) {
				continue
			}
			start, end := indices[startIdx], indices[endIdx]
			if start < 0 {
				continue // unmatched optional group — leave pre-allocated null
			}
			// strings.Clone: prevent memory retention of full _raw backing array.
			// Without Clone, the substring s[start:end] shares the backing array
			// with s. If s is a 500-byte _raw and the match is 30 bytes, GC cannot
			// free the 470 unused bytes while the substring is alive.
			batch.Columns[name][i] = event.StringValue(strings.Clone(s[start:end]))
		}
	}

	return batch, nil
}

func (r *RexIterator) Close() error { return r.child.Close() }

func (r *RexIterator) Schema() []FieldInfo { return r.child.Schema() }
