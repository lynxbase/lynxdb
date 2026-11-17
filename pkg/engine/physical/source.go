package physical

import (
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// Hints are the storage-level facts a scan offers for skipping data: every
// row the query keeps lies in [MinTime, MaxTime), contains each of Terms as
// a whole token of its raw text, contains at least one term of each AnyTerms
// group, has a token matching each TokenGlobs pattern, and satisfies each of
// FieldEquals. A storage engine may use any subset of them.
type Hints struct {
	Sources     []ir.Source
	MinTime     time.Time
	MaxTime     time.Time
	Terms       []string
	AnyTerms    [][]string
	TokenGlobs  []string
	FieldEquals []FieldEqual
	Columns     []string
	Limit       int64
}

// FieldEqual is a column that must equal a string value.
type FieldEqual struct {
	Field string
	Value string
}

// ScanHints derives storage hints from a scan for reference time now.
func ScanHints(scan *ir.Scan, now time.Time) Hints {
	h := Hints{Sources: scan.Sources, Columns: scan.Columns, Limit: scan.Limit}
	// A scan of several sources keeps the index column, which tells its
	// rows apart from those of the other sources a storage engine returns.
	if h.Columns != nil && (len(scan.Sources) > 1 || (len(scan.Sources) == 1 && scan.Sources[0].Pattern)) &&
		!slices.Contains(h.Columns, "index") {
		h.Columns = append(slices.Clip(h.Columns), "index")
	}
	h.MinTime, h.MaxTime = scan.Time.Resolve(now)
	for _, p := range scan.Prune {
		addPruneHint(&h, p)
	}
	return h
}

func addPruneHint(h *Hints, p ir.Expr) {
	switch x := p.(type) {
	case *ir.Search:
		if x.Field != "" {
			return
		}
		switch x.Mode {
		case ir.SearchWord, ir.SearchPhrase:
			h.Terms = append(h.Terms, Tokenize(x.Pattern)...)
		case ir.SearchPrefix:
			h.TokenGlobs = append(h.TokenGlobs, strings.ToLower(x.Pattern)+"*")
		case ir.SearchGlob:
			h.TokenGlobs = append(h.TokenGlobs, strings.ToLower(x.Pattern))
		}
	case *ir.Binary:
		switch x.Op {
		case ir.OpOr:
			var group []string
			for _, d := range disjuncts(x) {
				s, ok := d.(*ir.Search)
				if !ok || s.Field != "" || s.Mode != ir.SearchWord {
					return
				}
				toks := Tokenize(s.Pattern)
				if len(toks) != 1 {
					return
				}
				group = append(group, toks[0])
			}
			h.AnyTerms = append(h.AnyTerms, group)
		case ir.OpEq:
			f, ok := x.L.(*ir.FieldRef)
			lit, ok2 := x.R.(*ir.Literal)
			if !ok || !ok2 {
				return
			}
			if s, isStr := lit.V.(string); isStr {
				h.FieldEquals = append(h.FieldEquals, FieldEqual{Field: f.Name, Value: s})
			}
		}
	}
}

func disjuncts(e ir.Expr) []ir.Expr {
	if b, ok := e.(*ir.Binary); ok && b.Op == ir.OpOr {
		return append(disjuncts(b.L), disjuncts(b.R)...)
	}
	return []ir.Expr{e}
}

// Tokenize splits text into lower-case index tokens: maximal runs of
// letters and digits.
func Tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
}
