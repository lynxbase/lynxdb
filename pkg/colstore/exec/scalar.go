package exec

import (
	"math"
	"math/bits"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// The scalar evaluator computes an expression for the selected rows of a
// block as an operand: one column per kind that the rows hold, like the
// columns of a path. A node whose only input that varies is a dictionary
// column is computed once per dictionary entry; arithmetic and comparisons
// of numbers of one kind run as loops over typed slices; everything else is
// computed row by row from values, which defines what every function means.

const (
	nullBit  = uint8(1) << vec.KindNull
	allKinds = 1<<vec.KindBool | 1<<vec.KindInt | 1<<vec.KindFloat | 1<<vec.KindString | 1<<vec.KindVariant
)

// arity returns how many arguments a scalar function takes, most of them
// unbounded when negative, and zero for an operation that is not one.
func arity(op Op) (least, most int) {
	switch op {
	case OpHourOfDay, OpToInt, OpToFloat, OpToString, OpToBool, OpExists, OpIsNull, OpIsMissing, OpTypeOf,
		OpLength, OpLower, OpUpper, OpNot, OpMatch, OpAbs, OpFloor, OpCeil, OpSqrt, OpLn, OpExp:
		return 1, 1
	case OpRound, OpLog, OpTrim, OpTrimLeft, OpTrimRight:
		return 1, 2
	case OpAdd, OpSub, OpMul, OpDiv, OpMod, OpBin, OpNullIf, OpStartsWith, OpEndsWith,
		OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpPow, OpIndexOf:
		return 2, 2
	case OpIf, OpReplace:
		return 3, 3
	case OpSubstr:
		return 2, 3
	case OpCase:
		return 2, -1
	case OpCoalesce, OpConcat, OpAnd, OpOr:
		return 1, -1
	}

	return 0, 0
}

// resultKinds returns the kinds a function of the given arguments can give.
func resultKinds(op Op, args []*sexpr) uint8 {
	union := func(from, step int) uint8 {
		var k uint8
		for i := from; i < len(args); i += step {
			k |= args[i].kinds
		}

		return k &^ nullBit
	}
	switch op {
	case OpAdd, OpSub, OpMul, OpDiv, OpMod, OpBin:
		a, b := args[0].kinds, args[1].kinds
		var k uint8
		if a&b&kindBit(vec.KindInt) != 0 {
			k |= kindBit(vec.KindInt)
		}
		if (a|b)&kindBit(vec.KindFloat) != 0 && a&numericKinds != 0 && b&numericKinds != 0 {
			k |= kindBit(vec.KindFloat)
		}
		// A string reads as either kind of number.
		text := numericKinds | kindBit(vec.KindString)
		if (a|b)&kindBit(vec.KindString) != 0 && a&text != 0 && b&text != 0 {
			k |= numericKinds
		}
		if op == OpAdd {
			k |= a & b & kindBit(vec.KindString)
		}

		return k
	case OpHourOfDay, OpToInt, OpLength, OpFloor, OpCeil, OpIndexOf:
		return kindBit(vec.KindInt)
	case OpToFloat, OpRound, OpSqrt, OpLn, OpLog, OpExp, OpPow:
		return kindBit(vec.KindFloat)
	case OpAbs:
		return args[0].kinds & numericKinds
	case OpToString, OpTypeOf, OpLower, OpUpper, OpSubstr, OpConcat, OpReplace, OpTrim, OpTrimLeft, OpTrimRight:
		return kindBit(vec.KindString)
	case OpIf:
		return union(1, 1)
	case OpCase:
		return union(1, 2) | args[len(args)-1].kinds&^nullBit
	case OpCoalesce:
		return union(0, 1)
	case OpNullIf:
		return args[0].kinds &^ nullBit
	}

	return kindBit(vec.KindBool)
}

// strict reports whether a function gives no value when an argument has
// none.
func strict(op Op) bool {
	switch op {
	case OpIf, OpCase, OpCoalesce, OpNullIf, OpExists, OpIsNull, OpIsMissing, OpTypeOf, OpAnd, OpOr, OpFunc:
		return false
	}

	return true
}

// typeDict is the dictionary of type names, addressed by kind.
var typeDict = func() (d vec.Column) {
	d.Offsets = []uint32{0}
	for _, name := range typeNames {
		d.Data = append(d.Data, name...)
		d.Offsets = append(d.Offsets, uint32(len(d.Data)))
	}

	return d
}()

// operand is what an expression holds in the selected rows of a block: at
// most one column per kind, a row having a value in at most one of them.
// The columns are indexed by block row, or by selected row when dense is set.
type operand struct {
	cols  [kindSlots + 1]*vec.Column
	dense bool
	// computed is set for the outcome of a function, where a row without a
	// value holds a null instead of nothing.
	computed bool
}

// at returns the value of row r.
func (o *operand) at(r int) Value {
	for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
		c := o.cols[kind]
		if c == nil || !c.IsValid(r) {
			continue
		}
		switch kind {
		case vec.KindNull:
			return Value{Kind: kind}
		case vec.KindBool:
			return Value{Kind: kind, Bool: c.Bool(r)}
		case vec.KindInt:
			return Value{Kind: kind, Int: c.Int(r)}
		case vec.KindFloat:
			return Value{Kind: kind, Float: c.Float(r)}
		}

		return Value{Kind: kind, Bytes: c.Bytes(r)}
	}
	if o.computed {
		return Value{Kind: vec.KindNull}
	}

	return Value{}
}

// empty reports whether no row of the operand has a value.
func (o *operand) empty() bool {
	for _, c := range o.cols {
		if c != nil {
			return false
		}
	}

	return true
}

// only returns the column of an operand that holds values of one kind.
func (o *operand) only() *vec.Column {
	var one *vec.Column
	for _, c := range o.cols {
		if c == nil {
			continue
		}
		if one != nil {
			return nil
		}
		one = c
	}

	return one
}

// builder collects the values of rows, each of any kind, into an operand
// indexed by selected row.
type builder struct {
	cols   [kindSlots + 1]vec.Column
	count  [kindSlots + 1]int
	filled [kindSlots + 1]int
	n      int
}

func (b *builder) begin(n int) {
	b.n, b.count, b.filled = n, [kindSlots + 1]int{}, [kindSlots + 1]int{}
}

func (b *builder) held() int {
	n := 0
	for i := range b.cols {
		n += b.cols[i].Retained()
	}

	return n
}

func (b *builder) put(j int, v Value) {
	k := v.Kind
	if v.IsNull() {
		return
	}
	c := &b.cols[k]
	if b.count[k] == 0 {
		c.Kind, c.Layout, c.Rows = k, vec.Flat, b.n
		c.UseValid(b.n)
		switch k {
		case vec.KindInt:
			c.Ints = sized(c.Ints, b.n)
			clear(c.Ints)
		case vec.KindFloat:
			c.Floats = sized(c.Floats, b.n)
			clear(c.Floats)
		case vec.KindBool:
			c.Bools = sized(c.Bools, vec.BitmapWords(b.n))
			clear(c.Bools)
		case vec.KindString, vec.KindVariant:
			c.Offsets, c.Data = sized(c.Offsets, b.n+1), c.Data[:0]
		}
	}
	b.count[k]++
	c.Valid.Set(j)
	switch k {
	case vec.KindInt:
		c.Ints[j] = v.Int
	case vec.KindFloat:
		c.Floats[j] = v.Float
	case vec.KindBool:
		if v.Bool {
			c.Bools.Set(j)
		}
	case vec.KindString, vec.KindVariant:
		end := uint32(len(c.Data))
		for r := b.filled[k]; r <= j; r++ {
			c.Offsets[r] = end
		}
		b.filled[k] = j + 1
		c.Data = append(c.Data, v.Bytes...)
	}
}

func (b *builder) finish() operand {
	o := operand{dense: true, computed: true}
	for k := vec.KindNull; k <= vec.KindVariant; k++ {
		if b.count[k] == 0 {
			continue
		}
		c := &b.cols[k]
		if k == vec.KindString || k == vec.KindVariant {
			end := uint32(len(c.Data))
			for r := b.filled[k]; r <= b.n; r++ {
				c.Offsets[r] = end
			}
		}
		if b.count[k] == b.n {
			c.Valid = nil
		}
		o.cols[k] = c
	}

	return o
}

// scalarOut is the memory of one computed node of an expression.
type scalarOut struct {
	row, ent  builder
	lit, dict vec.Column
	entries   vec.Column
	// absent holds the bytes of what the node gives for a row without the
	// dictionary column's value.
	absent []byte
	buf    []byte
	// codes holds the kind of each block row and bools the bits of a
	// boolean outcome.
	codes  []uint8
	bools  vec.Bitmap
	ints   [2][]int64
	floats [2][]float64
	valid  [3][]uint8
	lits   [1]uint64
}

func (s *scalarOut) held() int {
	n := s.row.held() + s.ent.held() + cap(s.absent) + cap(s.buf) + cap(s.codes) + 8*cap(s.bools)
	for i := range s.ints {
		n += 8*cap(s.ints[i]) + 8*cap(s.floats[i])
	}
	for i := range s.valid {
		n += cap(s.valid[i])
	}

	return n
}

// literal returns a constant operand.
func (w *worker) literal(e *sexpr, n int) operand {
	o := operand{dense: true, computed: true}
	v := e.lit
	if v.IsNull() {
		return o
	}
	s := &w.outs[e.slot]
	c := &s.lit
	*c = vec.Column{Kind: v.Kind, Layout: vec.Const, Rows: n, Ints: c.Ints[:0], Floats: c.Floats[:0], Offsets: c.Offsets[:0]}
	switch v.Kind {
	case vec.KindInt:
		c.Ints = append(c.Ints, v.Int)
	case vec.KindFloat:
		c.Floats = append(c.Floats, v.Float)
	case vec.KindBool:
		s.lits[0] = uint64(btoi(v.Bool))
		c.Bools = s.lits[:]
	case vec.KindString, vec.KindVariant:
		c.Offsets, c.Data = append(c.Offsets, 0, uint32(len(v.Bytes))), v.Bytes
	}
	o.cols[v.Kind] = c

	return o
}

// operand computes e for the n selected rows of the open block, the rows
// listed in idx or the first n when idx is nil.
func (w *worker) operand(e *sexpr, n int, idx []uint32) (operand, error) {
	switch {
	case e.op == OpColumn:
		var o operand
		for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
			if e.kinds&kindBit(kind) == 0 {
				continue
			}
			c, err := w.column(e.path, kind)
			if err != nil {
				return o, err
			}
			o.cols[kind] = c
		}

		return o, nil
	case e.op == OpLiteral:
		return w.literal(e, n), nil
	case e.ints:
		return w.intOperand(e, n, idx)
	case w.shared == nil && e.args[0].op == OpColumn &&
		(e.op == OpExists || e.op == OpIsNull || e.op == OpIsMissing || e.op == OpTypeOf):
		return w.kindOperand(e)
	}
	base := len(w.opers)
	defer func() { w.opers = w.opers[:base] }()
	for _, a := range e.args {
		o, err := w.operand(a, n, idx)
		if err != nil {
			return operand{}, err
		}
		w.opers = append(w.opers, o)
	}
	args := w.opers[base:]
	if k := loneDictionary(e, args, n); k >= 0 {
		return w.overEntries(e, args, k, n, idx), nil
	}
	if o, ok := w.numeric(e, args, n, idx); ok {
		return o, nil
	}

	return w.byRow(e, args, n, idx, &w.outs[e.slot].row), nil
}

// kindOperand computes a function of what kind of value a column holds in
// each row of the block, from which rows hold one and without their values.
func (w *worker) kindOperand(e *sexpr) (operand, error) {
	var o operand
	arg, s, rows := e.args[0], &w.outs[e.slot], w.rows
	s.codes = sized(s.codes, rows)
	codes := s.codes
	clear(codes)
	// A row that holds values of several kinds counts as the lowest.
	for kind := vec.KindVariant; kind >= vec.KindNull; kind-- {
		if arg.kinds&kindBit(kind) == 0 || !w.has(arg.path, kind) {
			continue
		}
		if w.ref(arg.path, kind).Count == rows {
			for i := range codes {
				codes[i] = uint8(kind)
			}

			continue
		}
		valid, err := w.presence(arg.path, kind)
		if err != nil {
			return o, err
		}
		for wi, word := range valid {
			for ; word != 0; word &= word - 1 {
				if r := wi<<6 + bits.TrailingZeros64(word); r < rows {
					codes[r] = uint8(kind)
				}
			}
		}
	}
	c := &s.dict
	if e.op == OpTypeOf {
		*c = vec.Column{Kind: vec.KindString, Layout: vec.Dict, Rows: rows, Offsets: typeDict.Offsets, Data: typeDict.Data, Codes8: codes}
		o.cols[vec.KindString], o.computed = c, true

		return o, nil
	}
	mask := sized(s.valid[2], rows)
	s.valid[2] = mask
	want := uint8(KindAny)
	if e.op == OpIsNull {
		want = uint8(vec.KindNull)
	}
	vec.MaskEq8(mask, codes, want)
	if e.op == OpExists {
		vec.MaskNot(mask, mask)
	}
	s.bools = sized(s.bools, vec.BitmapWords(rows))
	vec.BitmapFromMask(s.bools, mask)
	*c = vec.Column{Kind: vec.KindBool, Rows: rows, Bools: s.bools}
	o.cols[vec.KindBool], o.computed = c, true

	return o, nil
}

// intOperand wraps what evalInt computes for a tree of integers.
func (w *worker) intOperand(e *sexpr, n int, idx []uint32) (operand, error) {
	vals, valid, err := w.evalInt(e, n, idx, e.depth)
	if err != nil {
		return operand{}, err
	}
	b := &w.outs[e.slot].row
	c := &b.cols[vec.KindInt]
	c.Kind, c.Layout, c.Rows, c.Valid = vec.KindInt, vec.Flat, n, nil
	c.Ints = append(c.Ints[:0], vals[:n]...)
	if valid != nil {
		vec.BitmapFromMask(c.UseValid(n), valid[:n])
	}
	o := operand{dense: true, computed: true}
	o.cols[vec.KindInt] = c

	return o, nil
}

// loneDictionary returns the argument that is a dictionary column of strings
// with fewer entries than rows while every other argument is a constant, or
// -1.
func loneDictionary(e *sexpr, args []operand, n int) int {
	at := -1
	for i := range args {
		if e.args[i].op == OpLiteral {
			continue
		}
		c := args[i].cols[vec.KindString]
		if at >= 0 || c == nil || args[i].only() != c || c.Layout == vec.Flat || c.DictLen() >= n {
			return -1
		}
		at = i
	}

	return at
}

// overEntries computes e once per entry of the dictionary of argument k and
// maps the outcome to the rows.
func (w *worker) overEntries(e *sexpr, args []operand, k, n int, idx []uint32) operand {
	s := &w.outs[e.slot]
	src, dense := args[k].cols[vec.KindString], args[k].dense
	entries := src.DictLen()
	s.entries = vec.Column{Kind: vec.KindString, Rows: entries, Offsets: src.Offsets, Data: src.Data}
	at := operand{dense: true, computed: args[k].computed}
	at.cols[vec.KindString] = &s.entries
	args[k] = at
	res := w.byRow(e, args, entries, nil, &s.ent)

	// What the function gives where the column has no value.
	var absent Value
	if src.Valid != nil {
		base := len(w.vals)
		vals := w.values(len(args))
		defer func() { w.vals = w.vals[:base] }()
		for i := range args {
			vals[i] = args[i].at(0)
		}
		if vals[k] = (Value{}); args[k].computed {
			vals[k].Kind = vec.KindNull
		}
		absent, s.buf = w.value(e, vals, s.buf[:0])
		if absent.Bytes != nil {
			s.absent = append(s.absent[:0], absent.Bytes...)
			absent.Bytes = s.absent
		}
	}
	if c := res.cols[vec.KindBool]; c != nil && c.Valid == nil && res.only() == c && absent.IsNull() && src.Layout == vec.Dict {
		// The outcome of each entry goes to the rows through their codes.
		table := sized(s.buf, max(entries, 256))
		s.buf = table
		for code := 0; code < entries; code++ {
			table[code] = uint8(0) - uint8(c.Bools[code>>6]>>(uint(code)&63)&1)
		}
		mask := sized(s.valid[2], src.Rows)
		s.valid[2] = mask
		if len(src.Codes8) != 0 {
			vec.MaskLookup8(mask, src.Codes8, table)
		} else {
			vec.MaskLookup16(mask, src.Codes16, table)
		}
		s.bools = sized(s.bools, vec.BitmapWords(src.Rows))
		vec.BitmapFromMask(s.bools, mask)
		s.dict = vec.Column{Kind: vec.KindBool, Rows: src.Rows, Valid: src.Valid, Bools: s.bools}
		o := operand{dense: dense, computed: true}
		o.cols[vec.KindBool] = &s.dict

		return o
	}
	if c := res.cols[vec.KindString]; c != nil && c.Valid == nil && res.only() == c && absent.IsNull() {
		s.dict = vec.Column{Kind: vec.KindString, Layout: src.Layout, Rows: src.Rows, Valid: src.Valid,
			Offsets: c.Offsets, Data: c.Data, Codes8: src.Codes8, Codes16: src.Codes16}
		o := operand{dense: dense, computed: true}
		o.cols[vec.KindString] = &s.dict

		return o
	}
	b := &s.row
	b.begin(n)
	for j := 0; j < n; j++ {
		r := j
		if !dense && idx != nil {
			r = int(idx[j])
		}
		switch {
		case !src.IsValid(r):
			b.put(j, absent)
		case src.Layout == vec.Const:
			b.put(j, res.at(0))
		default:
			b.put(j, res.at(src.Code(r)))
		}
	}

	return b.finish()
}

// values returns a buffer for the argument values of one row.
func (w *worker) values(n int) []Value {
	w.vals = fit(w.vals, len(w.vals)+n)

	return w.vals[len(w.vals)-n:]
}

// byRow computes e row by row from the values of its arguments.
func (w *worker) byRow(e *sexpr, args []operand, n int, idx []uint32, b *builder) operand {
	s := &w.outs[e.slot]
	base := len(w.vals)
	vals := w.values(len(args))
	b.begin(n)
	// A function that needs every argument skips the rows that an argument
	// with few values does not have.
	var sparse vec.Bitmap
	sparseDense := false
	if strict(e.op) {
		for i := range args {
			if args[i].empty() {
				w.vals = w.vals[:base]

				return b.finish()
			}
			if c := args[i].only(); c != nil && c.Valid != nil && sparse == nil {
				sparse, sparseDense = c.Valid, args[i].dense
			}
		}
	}
	buf := s.buf
	for j := 0; j < n; j++ {
		r := j
		if idx != nil {
			r = int(idx[j])
		}
		if sparse != nil && ((sparseDense && !sparse.Get(j)) || (!sparseDense && !sparse.Get(r))) {
			continue
		}
		for i := range args {
			if args[i].dense {
				vals[i] = args[i].at(j)
			} else {
				vals[i] = args[i].at(r)
			}
		}
		var v Value
		v, buf = w.value(e, vals, buf[:0])
		b.put(j, v)
	}
	s.buf, w.vals = buf, w.vals[:base]

	return b.finish()
}

// value computes the function of e for the values of its arguments in one
// row.
func (w *worker) value(e *sexpr, vals []Value, buf []byte) (Value, []byte) {
	if e.op != OpFunc {
		return scalarValue(e.op, &e.pat, vals, buf)
	}
	if w.fns == nil {
		w.fns = make([]Func, w.q.funcs)
	}
	if w.fns[e.fn] == nil {
		w.fns[e.fn] = e.newFunc()
	}

	return w.fns[e.fn](vals, buf)
}

// numbers is the numbers of an operand for the selected rows: ints or floats,
// one per row, and a mask of the rows that have one, nil when all do.
type numbers struct {
	ints   []int64
	floats []float64
	valid  []uint8
}

// dense reads the numbers of an operand that holds integers or floats but
// not both; ok is false for any other operand.
func (w *worker) dense(o *operand, s *scalarOut, slot, n int, idx []uint32) (num numbers, ok bool) {
	ci, cf := o.cols[vec.KindInt], o.cols[vec.KindFloat]
	if (ci == nil) == (cf == nil) {
		return num, false
	}
	c := ci
	if c == nil {
		c = cf
	}
	if o.dense {
		idx = nil
	}
	if c.Valid != nil {
		num.valid = sized(s.valid[slot], n)
		s.valid[slot] = num.valid
		if idx == nil {
			vec.MaskFromBitmap(num.valid, c.Valid)
		} else {
			for i, r := range idx {
				num.valid[i] = uint8(0) - uint8(c.Valid[r>>6]>>(r&63)&1)
			}
		}
	}
	if ci != nil {
		switch {
		case c.Layout != vec.Const && idx == nil:
			num.ints = c.Ints[:n]
		default:
			num.ints = sized(s.ints[slot], n)
			s.ints[slot] = num.ints
			for i := range num.ints {
				r := i
				if idx != nil {
					r = int(idx[i])
				}
				num.ints[i] = c.Int(r)
			}
		}

		return num, true
	}
	switch {
	case c.Layout != vec.Const && idx == nil:
		num.floats = c.Floats[:n]
	default:
		num.floats = sized(s.floats[slot], n)
		s.floats[slot] = num.floats
		for i := range num.floats {
			r := i
			if idx != nil {
				r = int(idx[i])
			}
			num.floats[i] = c.Float(r)
		}
	}

	return num, true
}

// asFloats returns the numbers as floats.
func (num *numbers) asFloats(s *scalarOut, slot int) []float64 {
	if num.floats != nil {
		return num.floats
	}
	out := sized(s.floats[slot], len(num.ints))
	s.floats[slot] = out
	for i, v := range num.ints {
		out[i] = float64(v)
	}

	return out
}

// numeric computes arithmetic and comparisons of two operands that each hold
// numbers of one kind; ok is false when it does not apply.
func (w *worker) numeric(e *sexpr, args []operand, n int, idx []uint32) (o operand, ok bool) {
	compare := false
	switch e.op {
	case OpAdd, OpSub, OpMul, OpDiv, OpBin:
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		compare = true
	default:
		return o, false
	}
	// Strings are joined, compared as strings or read as numbers value by
	// value.
	if args[0].cols[vec.KindString] != nil || args[1].cols[vec.KindString] != nil {
		return o, false
	}
	s := &w.outs[e.slot]
	a, okA := w.dense(&args[0], s, 0, n, idx)
	b, okB := w.dense(&args[1], s, 1, n, idx)
	if !okA || !okB {
		return o, false
	}
	valid := a.valid
	switch {
	case a.valid != nil && b.valid != nil:
		valid = sized(s.valid[2], n)
		s.valid[2] = valid
		vec.MaskAnd(valid, a.valid, b.valid)
	case a.valid == nil:
		valid = b.valid
	}
	// own returns valid as a mask to clear rows of; every mask an operand
	// came with is a buffer of this node.
	own := func() []uint8 {
		if valid == nil {
			valid = sized(s.valid[2], n)
			s.valid[2] = valid
			for i := range valid {
				valid[i] = 0xFF
			}
		}

		return valid
	}
	bld := &s.row
	bld.begin(n)
	out := func(kind vec.Kind) *vec.Column {
		c := &bld.cols[kind]
		c.Kind, c.Layout, c.Rows, c.Valid = kind, vec.Flat, n, nil

		return c
	}
	var c *vec.Column
	switch {
	case compare:
		c = out(vec.KindBool)
		mask := sized(s.buf, n)
		s.buf = mask
		if a.ints != nil && b.ints != nil {
			compareInts(mask, e.op, a.ints, b.ints)
		} else {
			x, y := a.asFloats(s, 0), b.asFloats(s, 1)
			compareFloats(mask, e.op, x, y)
			// A NaN compares with nothing.
			for i := range mask {
				if x[i] != x[i] || y[i] != y[i] {
					own()[i] = 0
				}
			}
		}
		c.Bools = sized(c.Bools, vec.BitmapWords(n))
		vec.BitmapFromMask(c.Bools, mask)
	case a.ints != nil && b.ints != nil:
		c = out(vec.KindInt)
		c.Ints = sized(c.Ints, n)
		x, y, z := a.ints, b.ints, c.Ints
		switch e.op {
		case OpAdd:
			for i := range z {
				z[i] = x[i] + y[i]
			}
		case OpSub:
			for i := range z {
				z[i] = x[i] - y[i]
			}
		case OpMul:
			for i := range z {
				z[i] = x[i] * y[i]
			}
		case OpDiv:
			for i := range z {
				switch d := y[i]; d {
				case 0:
					z[i], own()[i] = 0, 0
				case -1:
					z[i] = -x[i]
				default:
					z[i] = x[i] / d
				}
			}
		default:
			for i := range z {
				if d := y[i]; d > 0 {
					z[i] = floorTo(x[i], d)
				} else {
					z[i], own()[i] = 0, 0
				}
			}
		}
	default:
		c = out(vec.KindFloat)
		c.Floats = sized(c.Floats, n)
		x, y, z := a.asFloats(s, 0), b.asFloats(s, 1), c.Floats
		switch e.op {
		case OpAdd:
			for i := range z {
				z[i] = x[i] + y[i]
			}
		case OpSub:
			for i := range z {
				z[i] = x[i] - y[i]
			}
		case OpMul:
			for i := range z {
				z[i] = x[i] * y[i]
			}
		case OpDiv:
			for i := range z {
				if y[i] == 0 {
					z[i], own()[i] = 0, 0
				} else {
					z[i] = x[i] / y[i]
				}
			}
		default:
			for i := range z {
				if y[i] > 0 {
					z[i] = math.Floor(x[i]/y[i]) * y[i]
				} else {
					z[i], own()[i] = 0, 0
				}
			}
		}
	}
	if valid != nil {
		vec.BitmapFromMask(c.UseValid(n), valid)
		if c.Valid.Count() == n {
			c.Valid = nil
		}
	}
	o.dense, o.computed = true, true
	if c.Valid == nil || c.Valid.Count() > 0 {
		o.cols[c.Kind] = c
	}

	return o, true
}

// compareInts sets mask where the comparison op holds between x and y, with
// one loop per comparison: a greater-than is the less-than of the swapped
// arguments, and an inequality the opposite of an equality.
func compareInts(mask []uint8, op Op, x, y []int64) {
	switch op {
	case OpGt:
		x, y, op = y, x, OpLt
	case OpGe:
		x, y, op = y, x, OpLe
	}
	x, y = x[:len(mask)], y[:len(mask)]
	switch op {
	case OpLt:
		for i := range mask {
			var m uint8
			if x[i] < y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
	case OpLe:
		for i := range mask {
			var m uint8
			if x[i] <= y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
	default:
		for i := range mask {
			var m uint8
			if x[i] == y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
		if op == OpNe {
			vec.MaskNot(mask, mask)
		}
	}
}

// compareFloats is compareInts for floats; a NaN equals nothing and is
// neither below nor above anything.
func compareFloats(mask []uint8, op Op, x, y []float64) {
	switch op {
	case OpGt:
		x, y, op = y, x, OpLt
	case OpGe:
		x, y, op = y, x, OpLe
	}
	x, y = x[:len(mask)], y[:len(mask)]
	switch op {
	case OpLt:
		for i := range mask {
			var m uint8
			if x[i] < y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
	case OpLe:
		for i := range mask {
			var m uint8
			if x[i] <= y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
	default:
		for i := range mask {
			var m uint8
			if x[i] == y[i] {
				m = 0xFF
			}
			mask[i] = m
		}
		if op == OpNe {
			vec.MaskNot(mask, mask)
		}
	}
}

// compareFloatsTo sets mask where the comparison op holds between each of x
// and y.
func compareFloatsTo(mask []uint8, op Op, x []float64, y float64) {
	x = x[:len(mask)]
	switch op {
	case OpLt:
		for i := range mask {
			var m uint8
			if x[i] < y {
				m = 0xFF
			}
			mask[i] = m
		}
	case OpLe:
		for i := range mask {
			var m uint8
			if x[i] <= y {
				m = 0xFF
			}
			mask[i] = m
		}
	case OpGt:
		for i := range mask {
			var m uint8
			if x[i] > y {
				m = 0xFF
			}
			mask[i] = m
		}
	case OpGe:
		for i := range mask {
			var m uint8
			if x[i] >= y {
				m = 0xFF
			}
			mask[i] = m
		}
	default:
		for i := range mask {
			var m uint8
			if x[i] == y {
				m = 0xFF
			}
			mask[i] = m
		}
		if op == OpNe {
			vec.MaskNot(mask, mask)
		}
	}
}
