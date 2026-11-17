package physical

import (
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

// EventStore is an in-memory event collection keyed by index name, as the
// CLI's file and pipe modes load it.
type EventStore interface {
	MaterializeEvents(index string) []*event.Event
	// AllEvents returns the events of every index, or nil when unsupported.
	AllEvents() []*event.Event
}

// MapStore adapts an index-to-events map to EventStore.
type MapStore map[string][]*event.Event

func (m MapStore) MaterializeEvents(index string) []*event.Event { return m[index] }

func (m MapStore) AllEvents() []*event.Event {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	var all []*event.Event
	for _, n := range names {
		all = append(all, m[n]...)
	}
	return all
}

// IndexNames lists the indexes of a store that can enumerate them.
type IndexNames interface {
	Indexes() []string
}

// Indexes lists the map's index names.
func (m MapStore) Indexes() []string {
	out := make([]string, 0, len(m))
	for n := range m {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// NewEventSource returns a Source reading scans from an in-memory store. A
// scan without sources reads defaultIndex; a wildcard source reads every
// matching index. The scan's time range is enforced exactly; its pruning
// predicates are left to the operators above.
func NewEventSource(store EventStore, defaultIndex string, now time.Time) func(*ir.Scan) (pipeline.Iterator, error) {
	return func(scan *ir.Scan) (pipeline.Iterator, error) {
		events := resolveEvents(store, scan, defaultIndex)
		minT, maxT := scan.Time.Resolve(now)
		if !minT.IsZero() || !maxT.IsZero() {
			kept := events[:0:0]
			for _, ev := range events {
				if !minT.IsZero() && ev.Time.Before(minT) {
					continue
				}
				if !maxT.IsZero() && !ev.Time.Before(maxT) {
					continue
				}
				kept = append(kept, ev)
			}
			events = kept
		}
		rows := EventsToRows(events)
		if scan.Descending {
			sort.SliceStable(rows, func(i, j int) bool {
				return rows[i][ir.TimeColumn].AsTimestamp().After(rows[j][ir.TimeColumn].AsTimestamp())
			})
		}
		return pipeline.NewRowScanIterator(rows, pipeline.DefaultBatchSize), nil
	}
}

func resolveEvents(store EventStore, scan *ir.Scan, defaultIndex string) []*event.Event {
	if len(scan.Sources) == 0 {
		return store.MaterializeEvents(defaultIndex)
	}
	var names []string
	if lister, ok := store.(IndexNames); ok {
		names = lister.Indexes()
	}
	var out []*event.Event
	seen := map[string]bool{}
	for _, s := range scan.Sources {
		if s.Name == "*" {
			if all := store.AllEvents(); all != nil {
				return all
			}
		}
		if s.Pattern {
			for _, n := range names {
				if ir.MatchName(s.Name, n) && !seen[n] {
					seen[n] = true
					out = append(out, store.MaterializeEvents(n)...)
				}
			}
			continue
		}
		if !seen[s.Name] {
			seen[s.Name] = true
			out = append(out, store.MaterializeEvents(s.Name)...)
		}
	}
	return out
}

// EventsToRows converts events to rows with the event metadata columns.
func EventsToRows(events []*event.Event) []map[string]event.Value {
	if len(events) == 0 {
		return nil
	}
	rows := make([]map[string]event.Value, len(events))
	for i, ev := range events {
		row := make(map[string]event.Value, len(ev.Fields)+6)
		if !ev.Time.IsZero() {
			row[ir.TimeColumn] = event.TimestampValue(ev.Time)
		}
		if ev.Raw != "" {
			row[ir.RawColumn] = event.StringValue(ev.Raw)
		}
		if ev.Source != "" {
			row["_source"] = event.StringValue(ev.Source)
		}
		if ev.SourceType != "" {
			row["_sourcetype"] = event.StringValue(ev.SourceType)
		}
		if ev.Host != "" {
			row["host"] = event.StringValue(ev.Host)
		}
		if ev.Index != "" {
			row["index"] = event.StringValue(ev.Index)
		}
		for k, v := range ev.Fields {
			row[k] = v
		}
		rows[i] = row
	}
	return rows
}

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
	Descending  bool
}

// FieldEqual is a column that must equal a string value.
type FieldEqual struct {
	Field string
	Value string
}

// ScanHints derives storage hints from a scan for reference time now.
func ScanHints(scan *ir.Scan, now time.Time) Hints {
	h := Hints{Sources: scan.Sources, Columns: scan.Columns, Limit: scan.Limit, Descending: scan.Descending}
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
