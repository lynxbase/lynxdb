package exec

import "github.com/lynxbase/lynxdb/pkg/vec"

// hpred is a filter over the columns of an aggregate: And, Or and Not of
// comparisons between scalar expressions.
type hpred struct {
	op          Op
	args        []*hpred
	left, right *pexpr
	// id numbers the comparison among those of the filter.
	id int
}

func (q *query) filterAbove(f *Filter) error {
	slots, ids := 0, 0
	h, err := havingPred(f.Cond, q.names, &slots, &ids)
	if err != nil {
		return err
	}
	q.having, q.havingIDs = h, ids

	return nil
}

func havingPred(e *Expr, in []string, slots, ids *int) (*hpred, error) {
	if e == nil {
		return nil, unsupported("missing filter expression")
	}
	switch e.Op {
	case OpAnd, OpOr, OpNot:
		if e.Op == OpNot && len(e.Args) != 1 {
			return nil, unsupported("not takes one argument")
		}
		h := &hpred{op: e.Op}
		for _, a := range e.Args {
			arg, err := havingPred(a, in, slots, ids)
			if err != nil {
				return nil, err
			}
			h.args = append(h.args, arg)
		}

		return h, nil
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		if len(e.Args) != 2 {
			return nil, unsupported("a comparison takes two arguments")
		}
		left, err := postExpr(e.Args[0], in, slots)
		if err != nil {
			return nil, err
		}
		right, err := postExpr(e.Args[1], in, slots)
		if err != nil {
			return nil, err
		}
		*ids++

		return &hpred{op: e.Op, left: left, right: right, id: *ids - 1}, nil
	}

	return nil, unsupported("filter over an aggregate")
}

// havingEval applies a filter to batches of aggregate rows.
type havingEval struct {
	post        postEval
	left, right []*colVec
	out         []colVec
}

func (h *havingEval) held() int {
	n := h.post.held()
	for i := range h.out {
		n += h.out[i].held()
	}

	return n
}

// run returns the rows of in, n of them, that pass the query's filter; the
// result aliases in or the evaluator's buffers until the next run.
func (h *havingEval) run(q *query, in []colVec, n int) []colVec {
	h.left, h.right = fit(h.left, q.havingIDs), fit(h.right, q.havingIDs)
	h.load(q.having, in, n)
	h.out = fit(h.out, len(in))
	for c := range h.out {
		h.out[c].reset()
	}
	for i := 0; i < n; i++ {
		if !h.test(q.having, i) {
			continue
		}
		for c := range in {
			h.out[c].appendFrom(&in[c], i)
		}
	}

	return h.out
}

// load computes both sides of every comparison for the batch.
func (h *havingEval) load(e *hpred, in []colVec, n int) {
	for _, a := range e.args {
		h.load(a, in, n)
	}
	if e.left != nil {
		h.left[e.id], h.right[e.id] = h.post.eval(e.left, in, n), h.post.eval(e.right, in, n)
	}
}

func (h *havingEval) test(e *hpred, i int) bool {
	switch e.op {
	case OpAnd:
		for _, a := range e.args {
			if !h.test(a, i) {
				return false
			}
		}

		return true
	case OpOr:
		for _, a := range e.args {
			if h.test(a, i) {
				return true
			}
		}

		return false
	case OpNot:
		return !h.test(e.args[0], i)
	}
	a, b := h.left[e.id].value(i), h.right[e.id].value(i)
	// Values compare within one class of kinds, as in a scan filter.
	if a.IsNull() || b.IsNull() || kindRank(a.Kind) != kindRank(b.Kind) || a.Kind == vec.KindVariant {
		return false
	}
	c := compareValues(a, b)
	switch e.op {
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
