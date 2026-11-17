package exec

import (
	"math"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// wordOp says how one 64-bit word of a group's state is fed and merged.
type wordOp uint8

const (
	// wordRows counts the rows of the group.
	wordRows wordOp = iota
	// wordCount counts the values of an argument, of every kind it reads.
	wordCount
	wordCountInt
	wordCountFloat
	wordSumInt
	wordMinInt
	wordMaxInt
	wordSumFloat
	wordMinFloat
	wordMaxFloat
	// wordGroup holds the id that every worker shares for the group.
	wordGroup
)

type wordSpec struct {
	op  wordOp
	arg int
}

func kindBit(k vec.Kind) uint8 { return 1 << k }

const numericKinds = 1<<vec.KindInt | 1<<vec.KindFloat

// aggArg is an expression that one or more aggregates read.
type aggArg struct {
	expr *sexpr
	// kinds is the set of kinds the argument reads, a bit per kind.
	kinds uint8
	// words and distinct list the state words and the distinct counts that
	// the argument feeds.
	words    []int
	distinct []int
	// counted is set when only the number of values is asked for.
	counted bool
}

// aggOut describes how an aggregate's column comes from the state words;
// an index is -1 when the aggregate has no such word.
type aggOut struct {
	fn         AggFunc
	arg        int
	rows       int
	count      int
	countInt   int
	countFloat int
	ints       int
	floats     int
	distinct   int
}

// aggPlan is an aggregation bound to the table: a group's state is a row of
// words, fed per block and merged word by word.
type aggPlan struct {
	keys  []*sexpr
	names []string
	args  []aggArg
	words []wordSpec
	init  []int64
	outs  []aggOut
	// distinct is the number of distinct counts.
	distinct int
	// rows and group index the words of those kinds, -1 without one.
	rows  int
	group int
	// rowsOnly is set when the groups only count their rows.
	rowsOnly bool
}

func sameExpr(a, b *sexpr) bool {
	if a.op != b.op || a.path != b.path || a.kind != b.kind || len(a.args) != len(b.args) {
		return false
	}
	if a.op == OpLiteral && (a.lit.Kind != b.lit.Kind || a.lit.Int != b.lit.Int || a.lit.Float != b.lit.Float ||
		a.lit.Bool != b.lit.Bool || string(a.lit.Bytes) != string(b.lit.Bytes)) {
		return false
	}
	// Patterns are not compared.
	if a.op == OpMatch {
		return false
	}
	for i := range a.args {
		if !sameExpr(a.args[i], b.args[i]) {
			return false
		}
	}

	return true
}

func (ap *aggPlan) arg(e *sexpr) int {
	for i := range ap.args {
		if sameExpr(ap.args[i].expr, e) {
			return i
		}
	}
	ap.args = append(ap.args, aggArg{expr: e, kinds: e.kinds &^ nullBit})

	return len(ap.args) - 1
}

func (ap *aggPlan) word(op wordOp, arg int) int {
	for i, w := range ap.words {
		if w.op == op && w.arg == arg {
			return i
		}
	}
	ap.words = append(ap.words, wordSpec{op: op, arg: arg})
	i := len(ap.words) - 1
	if arg >= 0 {
		ap.args[arg].words = append(ap.args[arg].words, i)
	}

	return i
}

func (q *query) aggregate(a *Aggregate) error {
	ap := &aggPlan{rows: -1, group: -1}
	if len(a.Keys) == 0 && len(a.Aggs) == 0 {
		return unsupported("an aggregate needs a key or an aggregate function")
	}
	for _, k := range a.Keys {
		e, err := q.scanExpr(k.Expr, 0)
		if err != nil {
			return err
		}
		if e.op == OpLiteral {
			return unsupported("a group key must not be a literal")
		}
		ap.keys = append(ap.keys, e)
		ap.names = append(ap.names, k.Name)
	}
	for _, g := range a.Aggs {
		out := aggOut{fn: g.Func, arg: -1, rows: -1, count: -1, countInt: -1, countFloat: -1, ints: -1, floats: -1, distinct: -1}
		if g.Arg == nil {
			if g.Func != Count {
				return unsupported("aggregate %q needs an argument", g.Name)
			}
			out.rows = ap.word(wordRows, -1)
			ap.outs = append(ap.outs, out)
			ap.names = append(ap.names, g.Name)

			continue
		}
		e, err := q.scanExpr(g.Arg, 0)
		if err != nil {
			return err
		}
		if e.op == OpLiteral {
			return unsupported("an aggregate of a literal")
		}
		out.arg = ap.arg(e)
		kinds := ap.args[out.arg].kinds
		var ints, floats wordOp
		switch g.Func {
		case Count:
			out.count = ap.word(wordCount, out.arg)
		case CountDistinct:
			out.distinct = ap.distinct
			ap.args[out.arg].distinct = append(ap.args[out.arg].distinct, ap.distinct)
			ap.distinct++
		case Sum, Avg:
			ints, floats = wordSumInt, wordSumFloat
		case Min:
			ints, floats = wordMinInt, wordMinFloat
		case Max:
			ints, floats = wordMaxInt, wordMaxFloat
		default:
			return unsupported("unknown aggregate function")
		}
		if ints != 0 {
			if kinds&numericKinds == 0 {
				return unsupported("aggregate %q takes numbers", g.Name)
			}
			if kinds&kindBit(vec.KindInt) != 0 {
				out.countInt, out.ints = ap.word(wordCountInt, out.arg), ap.word(ints, out.arg)
			}
			if kinds&kindBit(vec.KindFloat) != 0 {
				out.countFloat, out.floats = ap.word(wordCountFloat, out.arg), ap.word(floats, out.arg)
			}
		}
		ap.outs = append(ap.outs, out)
		ap.names = append(ap.names, g.Name)
	}
	if ap.distinct > 0 && len(ap.keys) > 0 {
		ap.group = ap.word(wordGroup, -1)
	}
	// A state has at least one word, so that groups without aggregates exist.
	ap.rows = ap.word(wordRows, -1)
	ap.rowsOnly = len(ap.words) == 1 && ap.distinct == 0
	for i := range ap.args {
		arg := &ap.args[i]
		arg.counted = len(arg.distinct) == 0
		for _, w := range arg.words {
			arg.counted = arg.counted && ap.words[w].op == wordCount
		}
	}
	for _, w := range ap.words {
		v := int64(0)
		switch w.op {
		case wordMinInt:
			v = math.MaxInt64
		case wordMaxInt:
			v = math.MinInt64
		case wordMinFloat:
			v = int64(math.Float64bits(math.Inf(1)))
		case wordMaxFloat:
			v = int64(math.Float64bits(math.Inf(-1)))
		}
		ap.init = append(ap.init, v)
	}
	q.agg = ap

	return nil
}

func wordFloat(w int64) float64 { return math.Float64frombits(uint64(w)) }

func floatWord(f float64) int64 { return int64(math.Float64bits(f)) }

// mergeWords folds the state src into dst.
func (ap *aggPlan) mergeWords(dst, src []int64) {
	for i, w := range ap.words {
		switch w.op {
		case wordMinInt:
			dst[i] = min(dst[i], src[i])
		case wordMaxInt:
			dst[i] = max(dst[i], src[i])
		case wordSumFloat:
			dst[i] = floatWord(wordFloat(dst[i]) + wordFloat(src[i]))
		case wordMinFloat:
			dst[i] = floatWord(min(wordFloat(dst[i]), wordFloat(src[i])))
		case wordMaxFloat:
			dst[i] = floatWord(max(wordFloat(dst[i]), wordFloat(src[i])))
		case wordGroup:
			dst[i] = src[i]
		default:
			dst[i] += src[i]
		}
	}
}

// column computes the column of aggregate o for n groups whose states are in
// words; floats says which arguments read a float anywhere, and distinct
// holds the distinct counts by shared group id.
func (ap *aggPlan) column(o *aggOut, words []int64, n int, floats []bool, distinct [][]int64, dst *colVec) {
	stride := len(ap.words)
	word := func(g, i int) int64 {
		if i < 0 {
			return 0
		}

		return words[g*stride+i]
	}
	switch o.fn {
	case Count:
		out := dst.bulkInts(n)
		i := o.rows
		if i < 0 {
			i = o.count
		}
		for g := range out {
			out[g] = words[g*stride+i]
		}

		return
	case CountDistinct:
		out := dst.bulkInts(n)
		counts := distinct[o.distinct]
		for g := range out {
			id := 0
			if ap.group >= 0 {
				id = int(words[g*stride+ap.group])
			}
			out[g] = counts[id]
		}

		return
	}
	if o.fn != Avg && !floats[o.arg] {
		out := dst.bulkInts(n)
		for g := range out {
			out[g] = word(g, o.ints)
			if word(g, o.countInt) == 0 {
				dst.setNull(g)
			}
		}

		return
	}
	out := dst.bulkFloats(n)
	for g := range out {
		ni, nf := word(g, o.countInt), word(g, o.countFloat)
		if ni+nf == 0 {
			out[g] = 0
			dst.setNull(g)

			continue
		}
		i, f := float64(word(g, o.ints)), 0.0
		if o.floats >= 0 {
			f = wordFloat(words[g*stride+o.floats])
		}
		switch {
		case o.fn == Sum:
			out[g] = i + f
		case o.fn == Avg:
			out[g] = (i + f) / float64(ni+nf)
		case ni == 0:
			out[g] = f
		case nf == 0:
			out[g] = i
		case o.fn == Min:
			out[g] = min(i, f)
		default:
			out[g] = max(i, f)
		}
	}
}
