package exec_test

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// TestCodesFilterAsStringsDo runs filters on the strings of columns that
// parts store as codes of part dictionaries, in every block, in some blocks
// and in some parts, and over the same rows stored block by block: the same
// rows pass, a string is tested by its code, and a pattern that the layout
// of a dictionary rules out reads none of its strings.
func TestCodesFilterAsStringsDo(t *testing.T) {
	docs := accountDocs(60000)
	some, other := accountID(3), accountID(700)
	word := some[len("did:plc:"):]
	regexp := func(expr string) tokenfilter.Pattern {
		p, err := tokenfilter.Regexp(expr)
		if err != nil {
			t.Fatal(err)
		}

		return p
	}
	count := []exec.Agg{{Name: "rows", Func: exec.Count}}
	where := func(cond *exec.Expr) exec.Node { return &exec.Aggregate{Input: &exec.Scan{Filter: cond}, Aggs: count} }
	rows := func(cond *exec.Expr) exec.Node {
		return &exec.Project{Input: &exec.Scan{Filter: cond}, Exprs: []exec.Named{named("id", num("id")), named("acct", str("acct")), named("peer", str("peer"))}}
	}
	// byCode is set for the filters that test strings by their codes, and
	// ruledOut for those that a dictionary spares its strings.
	cases := []struct {
		name             string
		plan             exec.Node
		byCode, ruledOut bool
		empty            bool
	}{
		{name: "a word of the column", plan: where(exec.Match(str("acct"), tokenfilter.Word(word))), byCode: true},
		{name: "a word of the column, the rows", plan: rows(exec.Match(anyOf("acct"), tokenfilter.Word(word))), byCode: true},
		{name: "the start of a word", plan: where(exec.Match(str("peer"), tokenfilter.Prefix(other[8:14]))), byCode: true},
		{name: "a piece of a value", plan: where(exec.Match(str("acct"), tokenfilter.Substring(some[12:20]))), byCode: true},
		{name: "a piece of many values", plan: where(exec.Match(str("acct"), tokenfilter.Substring("ab"))), byCode: true},
		{name: "a piece in capitals, whatever the case", plan: where(exec.Match(str("peer"), tokenfilter.Substring("AB").IgnoreCase())), byCode: true},
		{name: "an expression", plan: where(exec.Match(str("acct"), regexp("plc:[a-f][a-z2-7]+q$"))), byCode: true},
		{name: "an expression of the rows", plan: rows(exec.Match(str("peer"), regexp("^did:plc:(a|b)[2-7]"))), byCode: true},
		{name: "a word that no identifier spells", plan: where(exec.Match(str("acct"), tokenfilter.Word("moon"))), ruledOut: true, empty: true},
		{name: "not a word of the column", plan: where(exec.Not(exec.Match(str("acct"), tokenfilter.Substring("ab")))), byCode: true},
		{name: "not equal", plan: where(exec.Ne(str("acct"), exec.Str(some))), byCode: true},
		{name: "below", plan: where(exec.Lt(str("acct"), exec.Str("did:plc:m"))), byCode: true},
		{name: "at or above, in a column with gaps", plan: rows(exec.Ge(str("peer"), exec.Str(other))), byCode: true},
		{name: "above a number", plan: where(exec.Gt(str("acct"), exec.Int(5))), byCode: true, empty: true},
		{name: "two columns", plan: where(exec.And(exec.Match(str("acct"), tokenfilter.Substring("a2")), exec.Match(str("peer"), tokenfilter.Substring("b")))), byCode: true},
		{name: "either column and a number", plan: rows(exec.And(exec.Or(exec.Match(str("acct"), tokenfilter.Substring("zz")), exec.Lt(str("peer"), exec.Str("did:plc:c"))),
			exec.Lt(num("n"), exec.Int(30)))), byCode: true},
		{name: "a word of any field", plan: where(exec.MatchAny(tokenfilter.Word(word))), byCode: true},
		{name: "the start of a word of any field", plan: rows(exec.MatchAny(tokenfilter.Prefix(other[8:13]))), byCode: true},
		{name: "a piece of any field", plan: where(exec.MatchAny(tokenfilter.Substring(some[10:19]))), byCode: true},
		{name: "another column's word in any field", plan: where(exec.MatchAny(tokenfilter.Word("moon"))), ruledOut: true},
		{name: "another column's words by account", plan: &exec.Aggregate{Input: &exec.Scan{Filter: exec.MatchAny(tokenfilter.Word("rare morning"))},
			Keys: keys("acct"), Aggs: count}, ruledOut: true},
	}
	// The token filter of a column spares a search the strings that lack
	// its words; without the filters, the layout of the dictionary does.
	for _, parts := range [][]codedPart{
		{{shared: true}, {shared: true}},
		{{shared: true, first: 900}, {}, {shared: true}},
		{{shared: true, unfiltered: true}, {shared: true, first: 900, unfiltered: true}},
	} {
		unfiltered := parts[0].unfiltered
		plain := codedTable(t, docs, 4096, make([]codedPart, len(parts)))
		coded := codedTable(t, docs, 4096, parts)
		was, is := plain.Snapshot(), coded.Snapshot()
		defer was.Release()
		defer is.Release()
		if codes, _ := blockForms(t, is, "peer"); codes == 0 {
			t.Fatal("no block stores the column with gaps as codes")
		}
		for _, c := range cases {
			for _, workers := range []int{1, 4} {
				answer := func(snap *colstore.Snapshot) ([]string, *exec.Result) {
					res, err := exec.Run(context.Background(), snap, c.plan, exec.Options{Workers: workers, Forgetful: true})
					if err != nil {
						t.Fatalf("%s: %v", c.name, err)
					}
					out := namedRows(res)
					slices.Sort(out)

					return out, res
				}
				want, _ := answer(was)
				got, res := answer(is)
				if empty := len(want) == 0 || (len(want) == 1 && want[0] == "rows=0"); empty != c.empty {
					t.Fatalf("%s: the rows are %v, so the query does not test what it means to", c.name, first(want))
				}
				if !slices.Equal(got, want) {
					t.Fatalf("%s with %d workers and %d parts: %d rows, want %d; first %v, want %v", c.name, workers, len(parts), len(got), len(want), first(got), first(want))
				}
				// Where a block is too short for a column to have a filter
				// of its own, the dictionary rules the pattern out there too.
				s := res.Stats
				ruledOut := s.ColumnsRuledOutByDictionary > 0
				if c.byCode != (s.ColumnsFilteredByCode > 0) || (ruledOut && !c.ruledOut) || (unfiltered && c.ruledOut && !ruledOut) {
					t.Fatalf("%s with %d workers and %d parts: %d columns filtered by code and %d ruled out by their dictionary",
						c.name, workers, len(parts), s.ColumnsFilteredByCode, s.ColumnsRuledOutByDictionary)
				}
			}
		}
	}
}

// TestWholeRowsOfCodesAskAhead fetches whole rows from parts that store
// columns as codes of part dictionaries and announce what they are about to
// read: the rows are those of the same parts stored block by block, and the
// pages of the dictionaries are asked for.
func TestWholeRowsOfCodesAskAhead(t *testing.T) {
	docs := accountDocs(60000)
	parts := []codedPart{{shared: true}, {shared: true, first: 900}, {}}
	plain := codedTable(t, docs, 4096, make([]codedPart, len(parts)))
	coded := codedTable(t, docs, 4096, parts)
	was, is := plain.Snapshot(), coded.Snapshot()
	defer was.Release()
	defer is.Release()
	var asked atomic.Int64
	for _, h := range is.Parts() {
		h.SetReadAhead(func(_, n int) { asked.Add(int64(n)) })
	}
	whole := func(filter *exec.Expr) exec.Node {
		return &exec.Project{Input: &exec.Scan{Filter: filter}, Exprs: []exec.Named{named("t", exec.Time())}, Rest: true}
	}
	latest := []exec.Order{{Column: "t", Desc: true}}
	for name, plan := range map[string]exec.Node{
		"the latest rows":          &exec.Sort{Input: whole(nil), By: latest, Limit: 100},
		"the first rows":           &exec.Sort{Input: whole(nil), By: []exec.Order{{Column: "t"}}, Limit: 40},
		"the rows of an account":   &exec.Sort{Input: whole(exec.Eq(str("acct"), exec.Str(accountID(3)))), By: latest, Limit: 100},
		"the rows of a peer":       whole(exec.Eq(str("peer"), exec.Str(accountID(700)))),
		"rows with a small number": &exec.Sort{Input: whole(exec.Lt(num("n"), exec.Int(2))), By: []exec.Order{{Column: "t"}}, Limit: 300},
		"any rows, on a goroutine": &exec.Sort{Input: whole(nil), Limit: 50},
		"the latest of two columns": &exec.Sort{Input: &exec.Project{Input: &exec.Scan{}, Exprs: []exec.Named{
			named("acct", str("acct")), named("peer", anyOf("peer")), named("t", exec.Time())}}, By: []exec.Order{{Column: "t", Desc: true}}, Limit: 80},
	} {
		for _, workers := range []int{1, 4} {
			if workers > 1 && strings.HasPrefix(name, "any rows") {
				continue
			}
			answer := func(snap *colstore.Snapshot) []string {
				res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: workers})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				rows := namedRows(res)
				slices.Sort(rows)

				return rows
			}
			for _, h := range is.Parts() {
				h.ForgetReadAhead()
			}
			asked.Store(0)
			want, got := answer(was), answer(is)
			if len(want) == 0 || !slices.Equal(got, want) {
				t.Fatalf("%s with %d workers: %d rows, want %d; first %v, want %v", name, workers, len(got), len(want), first(got), first(want))
			}
			if asked.Load() == 0 {
				t.Fatalf("%s with %d workers: nothing was asked for ahead", name, workers)
			}
		}
	}
}
