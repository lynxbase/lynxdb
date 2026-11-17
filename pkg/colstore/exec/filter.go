package exec

import (
	"bytes"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// verdict is what a filter node selects in a block: no row, every row, or
// the rows of a selection. From decide, some means that the block's
// statistics do not settle it.
type verdict uint8

const (
	some verdict = iota
	none
	all
)

// test applies a string predicate to one value.
func (p *pred) test(v []byte) bool {
	if p.op == OpMatch {
		return p.pat.Match(v)
	}
	c := bytes.Compare(v, p.lit.Bytes)
	switch p.op {
	case OpEq:
		return c == 0
	case OpNe:
		return c != 0
	case OpLt:
		return c < 0
	case OpLe:
		return c <= 0
	case OpGt:
		return c > 0
	}

	return c >= 0
}

// ints is the verdict of an integer comparison over values between lo and hi.
func (p *pred) ints(lo, hi int64) verdict {
	switch {
	case p.none:
		return none
	case p.ne && lo == hi && lo == p.lo:
		return none
	case p.ne && (p.lo < lo || p.lo > hi):
		return all
	case p.ne:
		return some
	case p.hi < lo || p.lo > hi:
		return none
	case p.lo <= lo && hi <= p.hi:
		return all
	}

	return some
}

func compareFloat(op Op, a, b float64) bool {
	switch op {
	case OpEq:
		return a == b
	case OpNe:
		return a != b
	case OpLt:
		return a < b
	case OpLe:
		return a <= b
	case OpGt:
		return a > b
	}

	return a >= b
}

// decide settles a filter node from the block's column directory alone:
// constant values, integer bounds and absent columns.
func (w *worker) decide(p *pred) verdict {
	// The block of a range scan comes without a column directory.
	if w.shared != nil && p.kind >= predStrEq {
		return some
	}
	switch p.kind {
	case predConst:
		if p.lit.Bool {
			return all
		}

		return none
	case predAnd:
		v := all
		for _, a := range p.args {
			switch w.decide(a) {
			case none:
				return none
			case some:
				v = some
			}
		}

		return v
	case predOr:
		v := none
		for _, a := range p.args {
			switch w.decide(a) {
			case all:
				return all
			case some:
				v = some
			}
		}

		return v
	case predNot:
		switch w.decide(p.args[0]) {
		case none:
			return all
		case all:
			return none
		}

		return some
	case predStrEq, predStr:
		ref := w.ref(p.path, vec.KindString)
		switch {
		case ref == nil:
			return none
		case !ref.Const():
			return some
		case !p.test(ref.ConstBytes()):
			return none
		case ref.Count == w.rows:
			return all
		}

		return some
	case predNum:
		v := none
		if p.onInt {
			if ref := w.ref(p.path, vec.KindInt); ref != nil {
				v = p.ints(ref.MinInt, ref.MaxInt)
				if v == all && ref.Count != w.rows {
					v = some
				}
			}
		}
		if v == none && p.onFloat && w.ref(p.path, vec.KindFloat) != nil {
			return some
		}

		return v
	case predBool:
		if w.ref(p.path, vec.KindBool) == nil {
			return none
		}
	case predHas:
		v := none
		for kind := vec.KindNull; kind <= vec.KindVariant; kind++ {
			if p.kinds&kindBit(kind) == 0 {
				continue
			}
			if ref := w.ref(p.path, kind); ref != nil {
				if v = some; ref.Count == w.rows {
					v = all

					break
				}
			}
		}
		switch {
		case v == some:
		case (v == all) != p.ne:
			return all
		default:
			return none
		}
	}

	return some
}

// evalHas selects the rows that hold a value of one of the kinds of p.
func (w *worker) evalHas(p *pred, depth int) (verdict, error) {
	m := w.mask(depth)
	have, every := false, false
	for kind := vec.KindNull; kind <= vec.KindVariant && !every; kind++ {
		if p.kinds&kindBit(kind) == 0 || !w.has(p.path, kind) {
			continue
		}
		var valid vec.Bitmap
		switch {
		case w.shared != nil:
			valid = w.sharedColumn(p.path, kind).Valid
		case w.ref(p.path, kind).Count < w.rows:
			var err error
			if valid, err = w.presence(p.path, kind); err != nil {
				return none, err
			}
		}
		switch {
		case valid == nil:
			every = true
		case !have:
			vec.MaskFromBitmap(m, valid)
			have = true
		default:
			other := w.mask(depth + 1)
			vec.MaskFromBitmap(other, valid)
			vec.MaskOr(m, m, other)
		}
	}
	switch {
	case every || !have:
		if every != p.ne {
			return all, nil
		}

		return none, nil
	case p.ne:
		vec.MaskNot(m, m)
	}

	return some, nil
}

// evalExpr selects the rows where a scalar expression gives true.
func (w *worker) evalExpr(p *pred, depth int) (verdict, error) {
	o, err := w.operand(p.left, w.rows, nil)
	c := o.cols[vec.KindBool]
	if err != nil || c == nil {
		return none, err
	}
	m := w.mask(depth)
	if c.Layout == vec.Const {
		if !c.Bools.Get(0) {
			return none, nil
		}

		return w.present(c, depth), nil
	}
	vec.MaskFromBitmap(m, c.Bools)
	if c.Valid != nil {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some, nil
}

// eval computes a filter node for the block; when it returns some, the
// selection of depth holds the rows.
func (w *worker) eval(p *pred, depth int) (verdict, error) {
	switch p.kind {
	case predAnd:
		return w.evalAnd(p, depth)
	case predOr:
		return w.evalOr(p, depth)
	case predNot:
		v, err := w.eval(p.args[0], depth)
		switch {
		case err != nil:
			return none, err
		case v == none:
			return all, nil
		case v == all:
			return none, nil
		}
		s := &w.sels[depth]
		w.toMask(s)
		vec.MaskNot(s.mask, s.mask)

		return some, nil
	case predStrEq:
		return w.evalStrEq(p, depth)
	case predStr:
		return w.evalStr(p, depth)
	case predNum:
		return w.evalNum(p, depth)
	case predBool:
		return w.evalBool(p, depth)
	case predInts:
		return w.evalInts(p, depth)
	case predExpr:
		return w.evalExpr(p, depth)
	case predHas:
		return w.evalHas(p, depth)
	}
	if p.lit.Bool {
		return all, nil
	}

	return none, nil
}

func (w *worker) evalAnd(p *pred, depth int) (verdict, error) {
	// A block that any argument rules out is left before a column is decoded.
	for _, a := range p.args {
		if w.decide(a) == none {
			return none, nil
		}
	}
	acc, next := &w.sels[depth], &w.sels[depth+1]
	have := false
	for _, a := range p.args {
		if w.decide(a) == all {
			continue
		}
		v, err := w.eval(a, depth+1)
		switch {
		case err != nil || v == none:
			return none, err
		case v == all:
			continue
		case !have:
			*acc, *next = *next, *acc
			have = true
		default:
			w.intersect(acc, next)
		}
		if w.empty(acc) {
			return none, nil
		}
	}
	if !have {
		return all, nil
	}

	return some, nil
}

// intersect keeps in acc the rows that other selects too.
func (w *worker) intersect(acc, other *selection) {
	switch {
	case !acc.listed && !other.listed:
		vec.MaskAnd(acc.mask, acc.mask, other.mask)
	case acc.listed && !other.listed:
		acc.list = vec.IndexesAndMask(acc.list, other.mask)
	case !acc.listed:
		other.list = vec.IndexesAndMask(other.list, acc.mask)
		*acc, *other = *other, *acc
	default:
		n, j := 0, 0
		for _, r := range acc.list {
			for j < len(other.list) && other.list[j] < r {
				j++
			}
			if j < len(other.list) && other.list[j] == r {
				acc.list[n] = r
				n++
			}
		}
		acc.list = acc.list[:n]
	}
}

func (w *worker) empty(s *selection) bool {
	if s.listed {
		return len(s.list) == 0
	}

	return vec.MaskCount(s.mask) == 0
}

func (w *worker) evalOr(p *pred, depth int) (verdict, error) {
	for _, a := range p.args {
		if w.decide(a) == all {
			return all, nil
		}
	}
	acc, next := &w.sels[depth], &w.sels[depth+1]
	have := false
	for _, a := range p.args {
		if w.decide(a) == none {
			continue
		}
		v, err := w.eval(a, depth+1)
		switch {
		case err != nil:
			return none, err
		case v == all:
			return all, nil
		case v == none:
			continue
		case !have:
			*acc, *next = *next, *acc
			have = true
		default:
			w.unite(acc, next)
		}
	}
	if !have {
		return none, nil
	}

	return some, nil
}

// unite adds to acc the rows that other selects.
func (w *worker) unite(acc, other *selection) {
	w.toMask(acc)
	w.toMask(other)
	vec.MaskOr(acc.mask, acc.mask, other.mask)
}

// present selects the rows of c that hold a value.
func (w *worker) present(c *vec.Column, depth int) verdict {
	if c.Valid == nil {
		return all
	}
	vec.MaskFromBitmap(w.mask(depth), c.Valid)

	return some
}

func (w *worker) evalStrEq(p *pred, depth int) (verdict, error) {
	c, err := w.column(p.path, vec.KindString)
	if err != nil || c == nil {
		return none, err
	}
	want := p.lit.Bytes
	switch c.Layout {
	case vec.Const:
		if string(c.DictValue(0)) != string(want) {
			return none, nil
		}

		return w.present(c, depth), nil
	case vec.Dict:
		code, ok := c.DictFind(want)
		if !ok {
			return none, nil
		}
		m := w.mask(depth)
		if len(c.Codes8) != 0 {
			vec.MaskEq8(m, c.Codes8, uint8(code))
		} else {
			vec.MaskEq16(m, c.Codes16, uint16(code))
		}
		// Rows without a value are decoded as code 0.
		if c.Valid != nil && code == 0 {
			vec.MaskAndBitmap(m, c.Valid)
		}

		return some, nil
	}
	vec.MaskEqBytes(w.mask(depth), c, want)

	return some, nil
}

func (w *worker) evalStr(p *pred, depth int) (verdict, error) {
	c, err := w.column(p.path, vec.KindString)
	if err != nil || c == nil {
		return none, err
	}
	switch c.Layout {
	case vec.Const:
		if !p.test(c.DictValue(0)) {
			return none, nil
		}

		return w.present(c, depth), nil
	case vec.Dict:
		// A window shorter than the dictionary has fewer rows than entries.
		if c.DictLen() <= w.rows {
			return w.evalDict(p, c, depth), nil
		}
		m := w.mask(depth)
		for i := range m {
			var b uint8
			if p.test(c.DictValue(c.Code(i))) {
				b = 0xFF
			}
			m[i] = b
		}
		if c.Valid != nil {
			vec.MaskAndBitmap(m, c.Valid)
		}

		return some, nil
	}
	if p.op == OpMatch {
		s := &w.sels[depth]
		s.list = p.pat.AppendMatches(s.list[:0], c.Offsets[:c.Rows+1], c.Data)
		if c.Valid != nil {
			s.list = vec.IndexesAndBitmap(s.list, c.Valid)
		}
		if len(s.list) == 0 {
			return none, nil
		}
		s.listed = true

		return some, nil
	}
	m := w.mask(depth)
	for i := range m {
		var b uint8
		if p.test(c.Data[c.Offsets[i]:c.Offsets[i+1]]) {
			b = 0xFF
		}
		m[i] = b
	}
	if c.Valid != nil {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some, nil
}

// evalDict applies a string predicate to each dictionary entry once and
// maps the outcome through the codes.
func (w *worker) evalDict(p *pred, c *vec.Column, depth int) verdict {
	n := c.DictLen()
	w.table = sized(w.table, max(n, 256))
	table := w.table
	clear(table)
	matched := 0
	if p.op == OpMatch {
		w.idx = p.pat.AppendMatches(w.idx[:0], c.Offsets[:n+1], c.Data)
		for _, code := range w.idx {
			table[code] = 0xFF
		}
		matched = len(w.idx)
	} else {
		for code := 0; code < n; code++ {
			if p.test(c.DictValue(code)) {
				table[code] = 0xFF
				matched++
			}
		}
	}
	switch {
	case matched == 0:
		return none
	case matched == n && c.Valid == nil:
		return all
	}
	m := w.mask(depth)
	if len(c.Codes8) != 0 {
		vec.MaskLookup8(m, c.Codes8, table)
	} else {
		vec.MaskLookup16(m, c.Codes16, table)
	}
	if c.Valid != nil && table[0] != 0 {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some
}

func (w *worker) evalNum(p *pred, depth int) (verdict, error) {
	v := none
	if p.onInt {
		var err error
		if v, err = w.evalIntColumn(p, depth); err != nil || v == all {
			return v, err
		}
	}
	if !p.onFloat {
		return v, nil
	}
	f, err := w.evalFloatColumn(p, depth+1)
	switch {
	case err != nil || f == none:
		return v, err
	case f == all:
		return all, nil
	case v == none:
		w.sels[depth], w.sels[depth+1] = w.sels[depth+1], w.sels[depth]

		return some, nil
	}
	w.unite(&w.sels[depth], &w.sels[depth+1])

	return some, nil
}

func (w *worker) evalIntColumn(p *pred, depth int) (verdict, error) {
	v := some
	switch {
	case p.none:
		return none, nil
	case w.shared == nil:
		ref := w.ref(p.path, vec.KindInt)
		if ref == nil {
			return none, nil
		}
		v = p.ints(ref.MinInt, ref.MaxInt)
		if v == none || (v == all && ref.Count == w.rows) {
			return v, nil
		}
	}
	c, err := w.column(p.path, vec.KindInt)
	if err != nil || c == nil {
		return none, err
	}
	if c.Layout == vec.Const {
		v = p.ints(c.Ints[0], c.Ints[0])
	}
	switch v {
	case none:
		return none, nil
	case all:
		return w.present(c, depth), nil
	}
	m := w.mask(depth)
	vec.MaskRangeInt64(m, c.Ints[:w.rows], p.lo, p.hi)
	if p.ne {
		vec.MaskNot(m, m)
	}
	if c.Valid != nil {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some, nil
}

func (w *worker) evalFloatColumn(p *pred, depth int) (verdict, error) {
	c, err := w.column(p.path, vec.KindFloat)
	if err != nil || c == nil {
		return none, err
	}
	if c.Layout == vec.Const {
		if !compareFloat(p.op, c.Floats[0], p.f) {
			return none, nil
		}

		return w.present(c, depth), nil
	}
	m := w.mask(depth)
	for i, x := range c.Floats[:w.rows] {
		var b uint8
		if compareFloat(p.op, x, p.f) {
			b = 0xFF
		}
		m[i] = b
	}
	if c.Valid != nil {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some, nil
}

func (w *worker) evalBool(p *pred, depth int) (verdict, error) {
	c, err := w.column(p.path, vec.KindBool)
	if err != nil || c == nil {
		return none, err
	}
	want := p.lit.Bool == (p.op == OpEq)
	if c.Layout == vec.Const {
		if c.Bools.Get(0) != want {
			return none, nil
		}

		return w.present(c, depth), nil
	}
	m := w.mask(depth)
	vec.MaskFromBitmap(m, c.Bools)
	if !want {
		vec.MaskNot(m, m)
	}
	if c.Valid != nil {
		vec.MaskAndBitmap(m, c.Valid)
	}

	return some, nil
}

func (w *worker) evalInts(p *pred, depth int) (verdict, error) {
	a, va, err := w.evalInt(p.left, w.rows, nil, depth+1)
	if err != nil {
		return none, err
	}
	b, vb, err := w.evalInt(p.right, w.rows, nil, depth+2)
	if err != nil {
		return none, err
	}
	m := w.mask(depth)
	a, b = a[:len(m)], b[:len(m)]
	for i := range m {
		var bit uint8
		x, y := a[i], b[i]
		var hit bool
		switch p.op {
		case OpEq:
			hit = x == y
		case OpNe:
			hit = x != y
		case OpLt:
			hit = x < y
		case OpLe:
			hit = x <= y
		case OpGt:
			hit = x > y
		default:
			hit = x >= y
		}
		if hit {
			bit = 0xFF
		}
		m[i] = bit
	}
	if va != nil {
		vec.MaskAnd(m, m, va)
	}
	if vb != nil {
		vec.MaskAnd(m, m, vb)
	}

	return some, nil
}
