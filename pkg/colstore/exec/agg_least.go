package exec

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// leastPlan is set for an aggregate whose one value is each group's earliest
// or latest time, sorted by it first under a limit: the scan takes blocks by
// time and leaves out those beyond the limit-th group found so far, which no
// group of the result has a row beyond.
type leastPlan struct {
	// path is the query path of the row time.
	path   int
	latest bool
	limit  int
}

// planLeast returns the leastPlan of the query, or nil when its shape is
// not one.
func (q *query) planLeast() *leastPlan {
	ap := q.agg
	if ap == nil || len(ap.keys) == 0 || len(ap.outs) != 1 || q.having != nil || q.located != nil || q.limit <= 0 || len(q.order) == 0 {
		return nil
	}
	out := ap.outs[0]
	if out.fn != Min && out.fn != Max {
		return nil
	}
	e := ap.args[out.arg].expr
	if e.op != OpColumn || q.paths[e.path] != part.TimePath || (e.kind != vec.KindInt && e.kind != KindAny) {
		return nil
	}
	// Every row has a time, so no group's time is null wherever nulls sort.
	by := q.order[0]
	if by.desc != (out.fn == Max) {
		return nil
	}
	col := len(ap.keys)
	if q.post != nil {
		for _, pe := range q.post {
			if postUses(pe, OpTotal) {
				return nil
			}
		}
		if pe := q.post[by.col]; pe.op != OpColumn || pe.col != col {
			return nil
		}
	} else if by.col != col {
		return nil
	}

	return &leastPlan{path: e.path, latest: out.fn == Max, limit: q.limit}
}

func postUses(e *pexpr, op Op) bool {
	for _, a := range e.args {
		if postUses(a, op) {
			return true
		}
	}

	return e.op == op
}

// leastScan is the state of a leastPlan while the scan runs: the groups with
// the least times found so far, which the goroutines share, and the bound
// that the limit-th of them gives the scan.
type leastScan struct {
	plan  *leastPlan
	bound atomic.Int64
	mu    sync.Mutex
	kept  leastKept
	// blocks holds, per goroutine, the groups of its open block that may
	// join kept.
	blocks []leastKept
	keys   [][]byte
	rows   [][]uint32
}

func newLeastScan(plan *leastPlan, workers int) *leastScan {
	l := &leastScan{plan: plan, blocks: make([]leastKept, workers), keys: make([][]byte, workers), rows: make([][]uint32, workers)}
	l.bound.Store(math.MaxInt64)
	if plan.latest {
		l.bound.Store(math.MinInt64)
	}

	return l
}

// byTime is the order of the scan, as ScanOptions.ByTime takes it.
func (l *leastScan) byTime() int {
	if l.plan.latest {
		return -1
	}

	return 1
}

// flip is what a time is xored with to order the kept times least first: the
// latest times are kept as the least of their complements.
func (l *leastScan) flip() int64 {
	if l.plan.latest {
		return -1
	}

	return 0
}

// narrow returns, of the n rows idx of worker w's open block, those whose
// time the bound leaves in: a row beyond it can only change a group that a
// row within it puts after the limit-th.
func (l *leastScan) narrow(w *worker, slot, n int, idx []uint32) (int, []uint32, error) {
	flip := l.flip()
	bound := l.bound.Load() ^ flip
	if bound == math.MaxInt64 {
		return n, idx, nil
	}
	c, err := w.column(l.plan.path, vec.KindInt)
	if err != nil || c == nil {
		return 0, nil, err
	}
	out := l.rows[slot][:0]
	for j := 0; j < n; j++ {
		r := uint32(j)
		if idx != nil {
			r = idx[j]
		}
		if c.IsValid(int(r)) && c.Int(int(r))^flip <= bound {
			out = append(out, r)
		}
	}
	l.rows[slot] = out
	if len(out) == n {
		return n, idx, nil
	}

	return len(out), out, nil
}

// offer takes the times of rows idx of worker w's open block, whose keys
// the aggregate has presented.
func (l *leastScan) offer(w *worker, slot, n int, idx []uint32) error {
	c, err := w.column(l.plan.path, vec.KindInt)
	if err != nil || c == nil {
		return err
	}
	flip := l.flip()
	limit := l.plan.limit
	worst := l.bound.Load() ^ flip
	k := &l.blocks[slot]
	k.reset()
	parts := w.scratch.agg.parts
	for j := 0; j < n; j++ {
		r := j
		if idx != nil {
			r = int(idx[j])
		}
		if !c.IsValid(r) {
			continue
		}
		if at := c.Int(r) ^ flip; at < worst && (len(k.heap) < limit || at < k.heap[0].at) {
			key := l.keys[slot][:0]
			for i := range parts {
				key = appendRowKey(key, &parts[i], j, r, i == len(parts)-1)
			}
			l.keys[slot] = key
			k.offer(string(key), at, limit)
		}
	}
	if len(k.heap) == 0 {
		return nil
	}
	l.mu.Lock()
	for _, g := range k.heap {
		l.kept.offer(g.key, g.at, limit)
	}
	full := len(l.kept.heap) == limit
	if full {
		worst = l.kept.heap[0].at ^ flip
	}
	l.mu.Unlock()
	for full {
		bound := l.bound.Load()
		if (flip != 0 && worst <= bound) || (flip == 0 && worst >= bound) || l.bound.CompareAndSwap(bound, worst) {
			break
		}
	}

	return nil
}

// leastKept holds groups with the least times, as a heap whose root has the
// greatest of them.
type leastKept struct {
	heap []leastGroup
	at   map[string]int
}

type leastGroup struct {
	key string
	at  int64
}

// offer lowers the time of the group with key to at, keeping the limit
// groups with the least times.
func (k *leastKept) offer(key string, at int64, limit int) {
	if k.at == nil {
		k.at = make(map[string]int, limit)
	}
	if i, ok := k.at[key]; ok {
		if at < k.heap[i].at {
			k.heap[i].at = at
			k.down(i)
		}

		return
	}
	if len(k.heap) < limit {
		k.heap = append(k.heap, leastGroup{key: key, at: at})
		k.at[key] = len(k.heap) - 1
		k.up(len(k.heap) - 1)

		return
	}
	delete(k.at, k.heap[0].key)
	k.heap[0] = leastGroup{key: key, at: at}
	k.at[k.heap[0].key] = 0
	k.down(0)
}

func (k *leastKept) reset() {
	k.heap = k.heap[:0]
	clear(k.at)
}

func (k *leastKept) swap(i, j int) {
	k.heap[i], k.heap[j] = k.heap[j], k.heap[i]
	k.at[k.heap[i].key], k.at[k.heap[j].key] = i, j
}

func (k *leastKept) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if k.heap[i].at <= k.heap[parent].at {
			return
		}
		k.swap(i, parent)
		i = parent
	}
}

func (k *leastKept) down(i int) {
	for {
		child := 2*i + 1
		if child >= len(k.heap) {
			return
		}
		if r := child + 1; r < len(k.heap) && k.heap[r].at > k.heap[child].at {
			child = r
		}
		if k.heap[child].at <= k.heap[i].at {
			return
		}
		k.swap(i, child)
		i = child
	}
}
