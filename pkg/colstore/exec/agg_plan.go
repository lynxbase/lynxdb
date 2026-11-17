package exec

import (
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
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
	// wordVarN, wordVarMean and wordVarM2 are three words in a row: the
	// number of values, their mean and the sum of their squared distances
	// from it, fed and merged together at the first.
	wordVarN
	wordVarMean
	wordVarM2
	// wordFirst, wordLast, wordEarliest and wordLatest hold the key of the
	// row a group picked, its place in the table or its time, and are each
	// followed by a wordPickLoc with the place of that row: part, block and
	// row. The two are fed and merged together.
	wordFirst
	wordLast
	wordEarliest
	wordLatest
	// wordArgMin and wordArgMax pick by the key that an order of the plan
	// computes, and wordFirstRow by place among all rows of the group.
	wordArgMin
	wordArgMax
	wordFirstRow
	wordPickLoc
	// wordState holds the stateRef of a state of variable size, which is
	// fed and merged apart from the words.
	wordState
)

type wordSpec struct {
	op  wordOp
	arg int
	// by indexes the order of a wordArgMin or a wordArgMax.
	by int
}

// pickOrder is an expression whose numbers order the rows of a pick.
type pickOrder struct {
	expr *sexpr
	// floats is set when the keys compare as floats.
	floats bool
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
	states   []int
	// stateKinds is the set of kinds whose values the states read.
	stateKinds uint8
	// counted is set when only the number of values is asked for.
	counted bool
	// picks is set when a group picks a row of the argument, and placed
	// when the argument's words need which rows hold a value but no value.
	picks  bool
	placed bool
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
	// spread is the first of the three words of a variance.
	spread int
	// pick is the word with the place of the row a group picked, and
	// located the column whose value in that row is the aggregate.
	pick    int
	located *sexpr
	// state is the state of an aggregate that has one, such as the extreme
	// string of a Min or a Max whose argument reads strings.
	state int
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
	// states are the states of variable size.
	states []stateSpec
	// distinct is the number of distinct counts.
	distinct int
	// rows and group index the words of those kinds, -1 without one.
	rows  int
	group int
	// rowsOnly is set when the groups only count their rows.
	rowsOnly bool
	// picks is set when an aggregate picks a row per group, and timePath is
	// the path of the row time when one picks by it, else -1.
	picks    bool
	timePath int
	orders   []pickOrder
	// firstRow is the word of the pick among all rows, -1 without one.
	firstRow int
}

func (ap *aggPlan) order(by *sexpr) int {
	for i := range ap.orders {
		if sameExpr(ap.orders[i].expr, by) {
			return i
		}
	}
	ap.orders = append(ap.orders, pickOrder{expr: by, floats: by.kinds&kindBit(vec.KindFloat) != 0})

	return len(ap.orders) - 1
}

// pickWord returns the word with the place of the row that op picks for an
// argument, adding the pair of words when it is not there.
func (ap *aggPlan) pickWord(op wordOp, arg, by int) int {
	for i, w := range ap.words {
		if w.op == op && w.arg == arg && w.by == by {
			return i + 1
		}
	}
	ap.words = append(ap.words, wordSpec{op: op, arg: arg, by: by}, wordSpec{op: wordPickLoc, arg: -1})
	ap.picks = true
	if arg >= 0 {
		ap.args[arg].words = append(ap.args[arg].words, len(ap.words)-2)
		ap.args[arg].picks = true
	}

	return len(ap.words) - 1
}

// least reports whether a pick prefers the smaller key.
func (op wordOp) least() bool {
	return op == wordFirst || op == wordEarliest || op == wordArgMin || op == wordFirstRow
}

// orderedFloat maps a float to an integer that orders as the float does.
func orderedFloat(f float64) int64 {
	b := int64(math.Float64bits(f))

	return b ^ (b >> 63 & math.MaxInt64)
}

// mergePick keeps in dst, a key and the place of its row, the better of it
// and another pick; least prefers the smaller key.
func mergePick(dst []int64, key, loc int64, least bool) {
	switch {
	case loc < 0:
		return
	case dst[1] < 0:
	case least && (key < dst[0] || (key == dst[0] && loc < dst[1])):
	case !least && (key > dst[0] || (key == dst[0] && loc > dst[1])):
	default:
		return
	}
	dst[0], dst[1] = key, loc
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
	ap := &aggPlan{rows: -1, group: -1, timePath: -1, firstRow: -1}
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
		out := aggOut{fn: g.Func, arg: -1, rows: -1, count: -1, countInt: -1, countFloat: -1, ints: -1, floats: -1, distinct: -1, spread: -1, pick: -1, state: -1}
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
		if g.Func == FirstRow {
			if e.op != OpColumn {
				return unsupported("aggregate %q takes a column", g.Name)
			}
			out.pick, out.located = ap.pickWord(wordFirstRow, -1, -1), e
			ap.firstRow = out.pick - 1
			ap.outs = append(ap.outs, out)
			ap.names = append(ap.names, g.Name)

			continue
		}
		out.arg = ap.arg(e)
		kinds := ap.args[out.arg].kinds
		var ints, floats wordOp
		switch g.Func {
		case Count:
			out.count = ap.word(wordCount, out.arg)
		case ApproxDistinct:
			out.state = ap.state(stateHLL, out.arg)
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
		case First, Last, Earliest, Latest, AnyValue:
			if e.op != OpColumn {
				return unsupported("aggregate %q takes a column", g.Name)
			}
			op := wordFirst
			if g.Func != AnyValue {
				op += wordOp(g.Func - First)
			}
			out.pick, out.located = ap.pickWord(op, out.arg, -1), e
			if op >= wordEarliest && ap.timePath < 0 {
				ap.timePath = q.path(part.TimePath)
			}
		case ArgMin, ArgMax:
			if e.op != OpColumn {
				return unsupported("aggregate %q takes a column", g.Name)
			}
			by, err := q.scanExpr(g.By, 0)
			if err != nil {
				return err
			}
			if by.kinds&numericKinds == 0 {
				return unsupported("aggregate %q orders by numbers", g.Name)
			}
			out.pick, out.located = ap.pickWord(wordArgMin+wordOp(g.Func-ArgMin), out.arg, ap.order(by)), e
		case Var, Stdev:
			if kinds&numericKinds == 0 {
				return unsupported("aggregate %q takes numbers", g.Name)
			}
			out.spread = ap.word(wordVarN, out.arg)
			ap.word(wordVarMean, out.arg)
			ap.word(wordVarM2, out.arg)
		default:
			return unsupported("unknown aggregate function")
		}
		if ints != 0 {
			extreme := g.Func == Min || g.Func == Max
			switch {
			case extreme && kinds&(numericKinds|kindBit(vec.KindString)) == 0:
				return unsupported("aggregate %q takes numbers or strings", g.Name)
			case !extreme && kinds&numericKinds == 0:
				return unsupported("aggregate %q takes numbers", g.Name)
			case g.Func == Min && kinds&kindBit(vec.KindString) != 0:
				out.state = ap.state(stateMinBytes, out.arg)
			case g.Func == Max && kinds&kindBit(vec.KindString) != 0:
				out.state = ap.state(stateMaxBytes, out.arg)
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
		arg.counted = len(arg.distinct) == 0 && len(arg.states) == 0
		arg.placed = arg.picks && len(arg.distinct) == 0 && len(arg.states) == 0
		for _, w := range arg.words {
			op := ap.words[w].op
			arg.counted = arg.counted && op == wordCount
			arg.placed = arg.placed && (op == wordCount || (op >= wordFirst && op <= wordArgMax))
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
		case wordFirst, wordEarliest, wordArgMin, wordFirstRow:
			v = math.MaxInt64
		case wordLast, wordLatest, wordArgMax:
			v = math.MinInt64
		case wordPickLoc:
			v = -1
		}
		ap.init = append(ap.init, v)
	}
	if ap.picks {
		q.located = make([]*sexpr, len(ap.names))
		for i, o := range ap.outs {
			q.located[len(ap.keys)+i] = o.located
		}
	}
	q.agg = ap

	return nil
}

func wordFloat(w int64) float64 { return math.Float64frombits(uint64(w)) }

// addSpread adds the value x to the three words of a variance.
func addSpread(w []int64, x float64) {
	n := w[0] + 1
	mean := wordFloat(w[1])
	d := x - mean
	mean += d / float64(n)
	w[0], w[1], w[2] = n, floatWord(mean), floatWord(wordFloat(w[2])+d*(x-mean))
}

// mergeSpread folds the three words src of a variance into dst.
func mergeSpread(dst, src []int64) {
	na, nb := float64(dst[0]), float64(src[0])
	switch {
	case nb == 0:
		return
	case na == 0:
		copy(dst[:3], src[:3])

		return
	}
	d := wordFloat(src[1]) - wordFloat(dst[1])
	dst[2] = floatWord(wordFloat(dst[2]) + wordFloat(src[2]) + d*d*na*nb/(na+nb))
	dst[1] = floatWord(wordFloat(dst[1]) + d*nb/(na+nb))
	dst[0] += src[0]
}

func floatWord(f float64) int64 { return int64(math.Float64bits(f)) }

// merge folds the group src, whose states of variable size are in from, into
// dst, whose are in h.
func (ap *aggPlan) merge(h *stateHeap, dst []int64, from *stateHeap, src []int64) {
	ap.mergeWords(dst, src)
	if len(ap.states) != 0 {
		ap.mergeStates(h, dst, &stateSource{heap: from, words: src})
	}
}

// mergeWords folds the words of the state src into dst.
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
		case wordVarN:
			mergeSpread(dst[i:i+3], src[i:i+3])
		case wordFirst, wordLast, wordEarliest, wordLatest, wordArgMin, wordArgMax, wordFirstRow:
			mergePick(dst[i:i+2], src[i], src[i+1], w.op.least())
		case wordVarMean, wordVarM2, wordPickLoc, wordState:
		default:
			dst[i] += src[i]
		}
	}
}

// column computes the column of aggregate o for n groups whose states are in
// words and in h; floats says which arguments read a float anywhere, and
// distinct holds the distinct counts by shared group id.
func (ap *aggPlan) column(o *aggOut, words []int64, h *stateHeap, n int, floats []bool, distinct [][]int64, dst *colVec) {
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
	case First, Last, Earliest, Latest, ArgMin, ArgMax, AnyValue, FirstRow:
		// The place of the row; the value is read once the rows are final.
		out := dst.bulkInts(n)
		for g := range out {
			if out[g] = words[g*stride+o.pick]; out[g] < 0 {
				out[g] = 0
				dst.setNull(g)
			}
		}

		return
	case ApproxDistinct:
		out := dst.bulkInts(n)
		at := ap.states[o.state].word
		for g := range out {
			out[g] = hllCount(h.get(stateRef(words[g*stride+at])))
		}

		return
	case Var, Stdev:
		out := dst.bulkFloats(n)
		for g := range out {
			w := words[g*stride+o.spread:]
			if out[g] = 0; w[0] < 2 {
				dst.setNull(g)

				continue
			}
			if out[g] = wordFloat(w[2]) / float64(w[0]-1); o.fn == Stdev {
				out[g] = math.Sqrt(out[g])
			}
		}

		return
	}
	if o.state >= 0 && ap.extremes(o, words, h, n, floats[o.arg], dst) {
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

// extremes computes the column of a Min or a Max whose argument reads
// strings, and reports false when no group holds one, which leaves the
// column to the numbers.
func (ap *aggPlan) extremes(o *aggOut, words []int64, h *stateHeap, n int, float bool, dst *colVec) bool {
	stride := len(ap.words)
	at := ap.states[o.state].word
	found := false
	for g := 0; g < n && !found; g++ {
		found = words[g*stride+at] != 0
	}
	if !found {
		return false
	}
	word := func(g, i int) int64 {
		if i < 0 {
			return 0
		}

		return words[g*stride+i]
	}
	dst.reset()
	for g := 0; g < n; g++ {
		s := h.get(stateRef(words[g*stride+at]))
		ni, nf := word(g, o.countInt), word(g, o.countFloat)
		// Numbers sort before strings.
		switch {
		case len(s) != 0 && (o.fn == Max || ni+nf == 0):
			dst.appendBytes(vec.KindString, s[1:])
		case ni+nf == 0:
			dst.appendNull()
		case !float:
			dst.appendInt(vec.KindInt, word(g, o.ints))
		default:
			i, f := float64(word(g, o.ints)), wordFloat(word(g, o.floats))
			switch {
			case ni == 0:
				dst.appendFloat(f)
			case nf == 0:
				dst.appendFloat(i)
			case o.fn == Min:
				dst.appendFloat(min(i, f))
			default:
				dst.appendFloat(max(i, f))
			}
		}
	}

	return true
}
