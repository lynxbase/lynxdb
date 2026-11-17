package exec_test

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// TestWordOfDigitsMatchesNumbers requires that a search of every field for
// a word of digits finds the rows that hold it as a number as well as those
// that hold it in a string.
func TestWordOfDigitsMatchesNumbers(t *testing.T) {
	word := func(s string) *exec.Expr { return exec.MatchAny(tokenfilter.Word(s).IgnoreCase()) }
	count := []exec.Agg{{Name: "n", Func: exec.Count}}
	cases := map[string]exec.Node{
		"a small number of many columns":  &exec.Aggregate{Input: &exec.Scan{Filter: word("42")}, Aggs: count},
		"a number that is a row's own":    &exec.Project{Input: &exec.Scan{Filter: word("2971")}, Exprs: []exec.Named{named("id", num("id")), named("n", anyOf("n"))}},
		"zero":                            &exec.Aggregate{Input: &exec.Scan{Filter: word("0")}, Keys: keys("kind"), Aggs: count},
		"a number and another condition":  &exec.Aggregate{Input: &exec.Scan{Filter: exec.And(word("7"), exec.Eq(anyOf("kind"), exec.Str("commit")))}, Aggs: count},
		"not a number":                    &exec.Aggregate{Input: &exec.Scan{Filter: exec.Not(word("13"))}, Aggs: count},
		"either of two numbers":           &exec.Aggregate{Input: &exec.Scan{Filter: exec.Or(word("99"), word("1000000"))}, Aggs: count},
		"a number no row holds":           &exec.Aggregate{Input: &exec.Scan{Filter: word("123456789012")}, Aggs: count},
		"digits with letters stay a word": &exec.Aggregate{Input: &exec.Scan{Filter: word("s3")}, Aggs: count},
	}
	eachLayout(t, 3000, func(t *testing.T, snap *colstore.Snapshot, rows []refRow, workers int) {
		for name, plan := range cases {
			t.Run(name, func(t *testing.T) {
				res := check(t, snap, rows, plan, false, workers)
				if name == "a small number of many columns" && (res.Rows != 1 || res.Value(0, 0).Int < 5) {
					t.Fatalf("%d rows hold 42 as a number or a word", res.Value(0, 0).Int)
				}
			})
		}
	})
	if n, ok := tokenfilter.Word("0042").Number(); !ok || n != 42 {
		t.Errorf("0042 spells %d, %v", n, ok)
	}
	for _, p := range []tokenfilter.Pattern{tokenfilter.Prefix("42"), tokenfilter.Substring("42"), tokenfilter.Word("4 2"), tokenfilter.Word("4.2"), tokenfilter.Word("1234567890123456789")} {
		if _, ok := p.Number(); ok {
			t.Errorf("%v spells a number", p)
		}
	}
}
