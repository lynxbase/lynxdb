package exec

import (
	"bytes"
	"math"
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// RowAgg is an aggregate of an Accumulator: Func of an argument, or with
// Rows the number of rows; P is the percentage of a Percentile and Size the
// most values of a Take.
type RowAgg struct {
	Func AggFunc
	P    float64
	Size int
	Rows bool
}

// Accumulator computes aggregates of rows that come one at a time, in
// groups that the caller numbers from zero, with the states and the results
// of the executor's aggregates. It is not safe for concurrent use.
type Accumulator struct {
	aggs  []RowAgg
	plan  aggPlan
	arena *mem.Arena
	heap  stateHeap
	// words holds the words of every group, and slots what the aggregates
	// that keep values hold, group by group.
	words  []int64
	slots  []accSlot
	groups int
	// floats tells, per argument, whether a float was read.
	floats []bool
	col    colVec
}

// accSlot is the state of one aggregate of one group that holds values:
// the value of a pick and the time of its row, the values of a list, the
// hashes of the distinct values.
type accSlot struct {
	value    Value
	at       int64
	held     bool
	values   []Value
	distinct map[[2]uint64]struct{}
}

// NewAccumulator returns an Accumulator for aggs, or an error that wraps
// ErrUnsupported for an aggregate it does not compute.
func NewAccumulator(aggs []RowAgg) (*Accumulator, error) {
	a := &Accumulator{aggs: aggs, arena: mem.NewArena(), floats: make([]bool, len(aggs))}
	a.heap.init(a.arena)
	ap := &a.plan
	*ap = aggPlan{rows: -1, group: -1, timePath: -1, firstRow: -1}
	for _, g := range aggs {
		out := aggOut{fn: g.Func, arg: -1, rows: -1, count: -1, countInt: -1, countFloat: -1, ints: -1, floats: -1, distinct: -1, spread: -1, pick: -1, state: -1}
		if g.Rows {
			if g.Func != Count {
				return nil, unsupported("an aggregate of rows other than a count")
			}
			out.rows = ap.word(wordRows, -1)
			ap.outs = append(ap.outs, out)

			continue
		}
		// Every aggregate reads an argument of its own, of any kind.
		ap.args = append(ap.args, aggArg{kinds: allKinds})
		out.arg = len(ap.args) - 1
		switch g.Func {
		case First, Last, Earliest, Latest, List, Take, Values, CountDistinct:
		default:
			if err := ap.allocate(&out, Agg{Func: g.Func, P: g.P}); err != nil {
				return nil, err
			}
		}
		ap.outs = append(ap.outs, out)
	}
	ap.finish()

	return a, nil
}

// Release gives up the memory of the states.
func (a *Accumulator) Release() { a.arena.Free() }

// group returns the words of group g, which it adds with every group below
// it.
func (a *Accumulator) group(g int) []int64 {
	stride := len(a.plan.words)
	for a.groups <= g {
		a.words = append(a.words, a.plan.init...)
		a.slots = append(a.slots, make([]accSlot, len(a.aggs))...)
		a.groups++
	}

	return a.words[g*stride : (g+1)*stride]
}

// Add adds a row of group g whose time is at: args holds the value of each
// aggregate's argument, the zero Value where the row has none.
func (a *Accumulator) Add(g int, args []Value, at int64) {
	w := a.group(g)
	ap := &a.plan
	w[ap.rows]++
	slots := a.slots[g*len(a.aggs) : (g+1)*len(a.aggs)]
	for i := range a.aggs {
		o := &ap.outs[i]
		v := args[i]
		if o.arg < 0 || v.IsNull() {
			continue
		}
		s := &slots[i]
		switch fn := a.aggs[i].Func; fn {
		case First, Earliest, Last, Latest:
			// Of rows that tie, the earliest keeps the first and the
			// latest the last.
			if !s.held || fn == Last || (fn == Earliest && at < s.at) || (fn == Latest && at >= s.at) {
				s.value, s.at, s.held = ownValue(s.value, v), at, true
			}
		case List, Take:
			most := MaxListed
			if fn == Take {
				most = a.aggs[i].Size
			}
			if len(s.values) < most {
				s.values = append(s.values, ownValue(Value{}, v))
			}
		case Values:
			at := sort.Search(len(s.values), func(j int) bool { return compareValues(s.values[j], v) >= 0 })
			if (at < len(s.values) && compareValues(s.values[at], v) == 0) || at >= MaxListed {
				continue
			}
			if len(s.values) == MaxListed {
				s.values = s.values[:MaxListed-1]
			}
			s.values = append(s.values, Value{})
			copy(s.values[at+1:], s.values[at:])
			s.values[at] = ownValue(Value{}, v)
		case CountDistinct:
			if s.distinct == nil {
				s.distinct = map[[2]uint64]struct{}{}
			}
			s.distinct[distinctHash128(v)] = struct{}{}
		default:
			a.feed(o.arg, w, v)
		}
	}
}

// ownValue returns v with bytes of its own, reusing those of old.
func ownValue(old, v Value) Value {
	if v.Bytes != nil {
		v.Bytes = append(old.Bytes[:0], v.Bytes...)
	}

	return v
}

// distinctHash128 identifies a value for a distinct count as the columnar
// aggregate does: kinds apart, and a float that equals an integer as that
// integer.
func distinctHash128(v Value) [2]uint64 {
	switch v.Kind {
	case vec.KindInt:
		return [2]uint64{vec.Mix64(uint64(v.Int)), vec.Mix64(uint64(v.Int) ^ kindSeeds[vec.KindInt])}
	case vec.KindFloat:
		if i, ok := whole(v.Float); ok {
			return [2]uint64{vec.Mix64(uint64(i)), vec.Mix64(uint64(i) ^ kindSeeds[vec.KindInt])}
		}
		bits := math.Float64bits(v.Float)

		return [2]uint64{vec.Mix64(bits), vec.Mix64(bits ^ kindSeeds[vec.KindFloat])}
	case vec.KindBool:
		b := uint64(btoi(v.Bool))

		return [2]uint64{vec.Mix64(b), vec.Mix64(b ^ kindSeeds[vec.KindBool])}
	}
	lo, hi := vec.Hash128(v.Bytes)

	return [2]uint64{lo, hi ^ kindSeeds[v.Kind]}
}

// feed adds a value that is not null to the words and the states of
// argument i in the words w of a group.
func (a *Accumulator) feed(i int, w []int64, v Value) {
	ap := &a.plan
	arg := &ap.args[i]
	var hash uint64
	switch v.Kind {
	case vec.KindInt:
		hash = hashInt(v.Int)
		a.number(arg, w, v, false)
	case vec.KindFloat:
		hash = hashFloat(v.Float)
		a.floats[i] = true
		a.number(arg, w, v, false)
	case vec.KindBool:
		hash = hashBool(v.Bool)
	default:
		hash = hashBytes(v.Kind, v.Bytes)
	}
	if v.Kind != vec.KindInt && v.Kind != vec.KindFloat {
		for _, wi := range arg.words {
			if ap.words[wi].op == wordCount {
				w[wi]++
			}
		}
	}
	text := false
	if v.Kind == vec.KindString && arg.numbers {
		if n, ok := numberText(v.Bytes); ok {
			text = true
			a.floats[i] = a.floats[i] || n.Kind == vec.KindFloat
			a.number(arg, w, n, true)
		}
	}
	for _, si := range arg.states {
		st := &ap.states[si]
		word := &w[st.word]
		switch st.op {
		case stateHLL:
			*word = int64(hllAdd(&a.heap, stateRef(*word), hash))
		case stateMinBytes, stateMaxBytes:
			if v.Kind != vec.KindString || text {
				continue
			}
			if cur := a.heap.get(stateRef(*word)); len(cur) != 0 {
				if c := bytes.Compare(v.Bytes, cur[1:]); c == 0 || (c > 0) != (st.op == stateMaxBytes) {
					continue
				}
			}
			ref, b := a.heap.resize(stateRef(*word), 1+len(v.Bytes))
			b[0] = tagString
			copy(b[1:], v.Bytes)
			*word = int64(ref)
		}
	}
}

// number adds a number to the words of an argument that read numbers and
// to its sketch; text says that the number is the one a string holds, which
// the count of values of every kind has counted.
func (a *Accumulator) number(arg *aggArg, w []int64, v Value, text bool) {
	ap := &a.plan
	x := v.Float
	if v.Kind == vec.KindInt {
		x = float64(v.Int)
	}
	for _, wi := range arg.words {
		word := &w[wi]
		switch op := ap.words[wi].op; {
		case op == wordCount:
			if !text {
				*word++
			}
		case op == wordVarN:
			addSpread(w[wi:], x)
		case v.Kind == vec.KindInt:
			switch op {
			case wordCountInt:
				*word++
			case wordSumInt:
				*word += v.Int
			case wordMinInt:
				*word = min(*word, v.Int)
			case wordMaxInt:
				*word = max(*word, v.Int)
			}
		default:
			switch op {
			case wordCountFloat:
				*word++
			case wordSumFloat:
				*word = floatWord(wordFloat(*word) + x)
			case wordMinFloat:
				*word = floatWord(min(wordFloat(*word), x))
			case wordMaxFloat:
				*word = floatWord(max(wordFloat(*word), x))
			}
		}
	}
	if arg.sketched {
		switch {
		case x != x:
		case x == 0:
			w[arg.sketch.zeros]++
		default:
			arg.sketch.add(&a.heap, w, x, 1)
		}
	}
}

// Reset empties group g.
func (a *Accumulator) Reset(g int) {
	w := a.group(g)
	a.plan.releaseStates(&a.heap, w)
	copy(w, a.plan.init)
	slots := a.slots[g*len(a.aggs) : (g+1)*len(a.aggs)]
	for i := range slots {
		s := &slots[i]
		s.held, s.values = false, s.values[:0]
		clear(s.distinct)
	}
}

// Result appends to dst the value of every aggregate for group g; the bytes
// of a value stay valid until the next call of a method.
func (a *Accumulator) Result(g int, dst []Value) []Value {
	w := a.group(g)
	ap := &a.plan
	slots := a.slots[g*len(a.aggs) : (g+1)*len(a.aggs)]
	for i := range a.aggs {
		o := &ap.outs[i]
		s := &slots[i]
		switch a.aggs[i].Func {
		case First, Last, Earliest, Latest:
			if !s.held {
				dst = append(dst, Value{Kind: vec.KindNull})

				continue
			}
			dst = append(dst, s.value)
		case List, Take, Values:
			b, mark := variant.BeginArray(nil)
			for _, v := range s.values {
				b = appendVariantValue(b, v)
			}
			dst = append(dst, Value{Kind: vec.KindVariant, Bytes: variant.End(b, mark)})
		case CountDistinct:
			dst = append(dst, Value{Kind: vec.KindInt, Int: int64(len(s.distinct))})
		default:
			a.col.reset()
			ap.column(o, w, &a.heap, 1, a.floats, nil, &a.col)
			v := a.col.value(0)
			if v.Kind == KindAny {
				v.Kind = vec.KindNull
			}
			// The column serves the next aggregate.
			if v.Bytes != nil {
				v.Bytes = append([]byte(nil), v.Bytes...)
			}
			dst = append(dst, v)
		}
	}

	return dst
}

// appendVariantValue appends a value in the variant encoding.
func appendVariantValue(dst []byte, v Value) []byte {
	switch v.Kind {
	case vec.KindBool:
		return variant.AppendBool(dst, v.Bool)
	case vec.KindInt:
		return variant.AppendInt(dst, v.Int)
	case vec.KindFloat:
		return variant.AppendFloat(dst, v.Float)
	case vec.KindString:
		return variant.AppendString(dst, v.Bytes)
	case vec.KindVariant:
		return append(dst, v.Bytes...)
	}

	return variant.AppendNull(dst)
}
