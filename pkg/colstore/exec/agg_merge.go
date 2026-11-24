package exec

import (
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// serialGroups is the number of groups up to which the tables of the
// workers are merged on one goroutine.
var serialGroups = 4096

// merger is the state one goroutine owns while it merges what the workers
// aggregated.
type merger struct {
	*mergeScratch
	arena *mem.Arena
	set   *vec.HashSet128
	// own holds the groups that the merger is merging: those of one
	// partition after another that no worker merged groups into.
	own groupPartition
	// lookup serves the merges of the groups that workers left staged.
	lookup lookup
	// users counts the distinct values per distinct count and shared group.
	users [][]int64
	sink  sink
}

// mergeScratch is the memory a merger keeps between queries.
type mergeScratch struct {
	batch  []colVec
	post   postEval
	having havingEval
}

var mergeScratchPool mem.Pool[mergeScratch]

func (m *merger) held() int {
	if m.arena == nil {
		return 0
	}
	n := m.post.held() + m.having.held() + m.sink.held() + m.arena.Size() + m.own.held() + m.lookup.held()
	for i := range m.users {
		n += 8 * cap(m.users[i])
	}
	for i := range m.batch {
		n += m.batch[i].held()
	}

	return n
}

// merger returns the state of a merge goroutine, set up on first use.
func (x *execution) merger(slot int) *merger {
	m := &x.mergers[slot]
	if m.arena == nil {
		m.mergeScratch = mergeScratchPool.Get()
		for i := range m.batch {
			m.batch[i].reset()
		}
		for i := range m.post.tmp {
			m.post.tmp[i].reset()
		}
		m.post.rows = x.rows
		m.arena = mem.NewArena()
		m.own.heap.init(m.arena)
		m.own.table = vec.NewByteTable(m.arena)
		m.sink.init(x.q)
	}

	return m
}

// finishAggregate merges the states of the workers, computes the columns of
// the groups and returns the sinks that received them.
func (x *execution) finishAggregate(ws []*worker) ([]*sink, error) {
	plan := x.q.agg
	stride := len(plan.words)
	slots := len(x.workers)
	x.mergers = make([]merger, slots)
	floats := make([]bool, len(plan.args))
	staged, total := false, 0
	for _, w := range ws {
		x.rows += w.matched
		for i, f := range w.agg.floats {
			floats[i] = floats[i] || f
		}
		staged = staged || w.agg.staged != nil
		total += w.agg.groups
	}
	var distinct [][]int64
	if plan.distinct > 0 {
		var err error
		if distinct, err = x.mergeDistinct(ws); err != nil {
			return nil, err
		}
	}

	m := x.merger(0)
	own := &m.own
	switch {
	case len(plan.keys) == 0:
		words := append([]int64(nil), plan.init...)
		for _, w := range ws {
			plan.merge(&own.heap, words, &w.agg.heap, w.agg.words[:stride])
		}
		m.emit(x, nil, words, &own.heap, 1, floats, distinct)
	case !staged && total <= serialGroups && x.shared != nil:
		// The shared ids number the groups of every worker alike.
		groups := x.shared.table.Len()
		words := make([]int64, 0, groups*stride)
		for g := 0; g < groups; g++ {
			words = append(words, plan.init...)
		}
		for _, w := range ws {
			for g := 0; g < w.agg.groups; g++ {
				id := int(w.agg.global[g])
				plan.merge(&own.heap, words[id*stride:(id+1)*stride], &w.agg.heap, w.agg.words[g*stride:(g+1)*stride])
			}
		}
		if groups > 0 {
			m.emit(x, x.shared.table, words, &own.heap, groups, floats, distinct)
		}
	case !staged && total <= serialGroups:
		for _, w := range ws {
			for g := 0; g < w.agg.groups; g++ {
				state := w.agg.words[g*stride : (g+1)*stride]
				id := own.table.IDHashed(w.agg.table.Key(g), w.agg.table.KeyHash(g))
				if id == own.n {
					own.n++
					own.words = append(wordPool.Grow(own.words, stride), state...)
					if len(plan.states) != 0 {
						plan.adoptStates(&own.heap, own.words[id*stride:], &stateSource{heap: &w.agg.heap, words: state})
					}

					continue
				}
				plan.merge(&own.heap, own.words[id*stride:(id+1)*stride], &w.agg.heap, state)
			}
		}
		if own.n > 0 {
			m.emit(x, own.table, own.words, &own.heap, own.n, floats, distinct)
		}
	default:
		if err := forEach(len(ws), slots, func(_, i int) { ws[i].agg.spill() }); err != nil {
			return nil, err
		}
		// A key lives in one partition, so a partition is complete once it
		// has what the workers left staged for it. The groups of a partition
		// that no worker merged any into while it scanned live no longer
		// than it takes to hand them on.
		err := forEach(1<<x.partBits, slots, func(slot, p int) {
			m := x.merger(slot)
			gp, cold := &m.own, false
			if x.parts != nil && x.parts[p].table != nil {
				gp, cold = &x.parts[p], true
			} else {
				gp.reset()
			}
			for _, w := range ws {
				if w.agg.staged != nil {
					w.agg.staged.Chunks(p, func(chunk []byte) { gp.merge(plan, chunk, m.arena, &m.lookup, cold) })
				}
			}
			if gp.n > 0 {
				m.emit(x, gp.table, gp.words, &gp.heap, gp.n, floats, distinct)
			}
		})
		if err != nil {
			return nil, err
		}
	}
	var sinks []*sink
	for i := range x.mergers {
		if x.mergers[i].arena != nil {
			sinks = append(sinks, &x.mergers[i].sink)
		}
	}

	return sinks, nil
}

// mergeRecord folds the state of a staged group into dst.
func (ap *aggPlan) mergeRecord(dst []int64, state []byte) {
	for i, w := range ap.words {
		v := int64(binary.LittleEndian.Uint64(state[8*i:]))
		switch w.op {
		case wordMinInt:
			dst[i] = min(dst[i], v)
		case wordMaxInt:
			dst[i] = max(dst[i], v)
		case wordSumFloat:
			dst[i] = floatWord(wordFloat(dst[i]) + wordFloat(v))
		case wordMinFloat:
			dst[i] = floatWord(min(wordFloat(dst[i]), wordFloat(v)))
		case wordMaxFloat:
			dst[i] = floatWord(max(wordFloat(dst[i]), wordFloat(v)))
		case wordGroup:
			dst[i] = v
		case wordVarN:
			src := [3]int64{v, int64(binary.LittleEndian.Uint64(state[8*i+8:])), int64(binary.LittleEndian.Uint64(state[8*i+16:]))}
			mergeSpread(dst[i:i+3], src[:])
		case wordFirst, wordLast, wordEarliest, wordLatest, wordArgMin, wordArgMax, wordFirstRow:
			mergePick(dst[i:i+2], v, int64(binary.LittleEndian.Uint64(state[8*i+8:])), w.op.least())
		case wordVarMean, wordVarM2, wordPickLoc, wordState:
		default:
			dst[i] += v
		}
	}
}

// emit computes the plan's columns for n groups, whose keys table numbers
// and whose states are in words and in h, and hands them to the merger's
// sink.
func (m *merger) emit(x *execution, table *vec.ByteTable, words []int64, h *stateHeap, n int, floats []bool, distinct [][]int64) {
	plan := x.q.agg
	keys := len(plan.keys)
	m.batch = fit(m.batch, len(plan.names))
	for i := range m.batch[:keys] {
		// A sink that keeps few rows reads the keys of those alone.
		if m.sink.top.k > 0 {
			m.batch[i].view(table, i, keys, n)
		} else {
			m.batch[i].reset()
		}
	}
	if keys > 0 && m.sink.top.k == 0 {
		for g := 0; g < n; g++ {
			decodeKey(table.Key(g), m.batch[:keys])
		}
	}
	for i := range plan.outs {
		plan.column(&plan.outs[i], words, h, n, floats, distinct, &m.batch[keys+i])
	}
	out := m.batch
	if x.q.having != nil && !x.q.early {
		out = m.having.run(x.q, out, n)
		n = out[0].len()
	}
	if x.q.post != nil && !x.q.early {
		out = m.post.run(x.q, out, n)
	}
	m.sink.add(out)
}

// mergeDistinct counts the distinct values that the workers logged, per
// distinct count and shared group id.
func (x *execution) mergeDistinct(ws []*worker) ([][]int64, error) {
	plan := x.q.agg
	groups := 1
	if x.shared != nil {
		groups = x.shared.table.Len()
	}
	// Equal values of a group land in the same partition, so partitions
	// count independently and their counts add up.
	err := forEach(plan.distinct<<partitionBits, len(x.mergers), func(slot, i int) {
		m := x.merger(slot)
		if m.set == nil {
			m.set = vec.NewHashSet128(m.arena)
			m.users = make([][]int64, plan.distinct)
			for d := range m.users {
				m.users[d] = make([]int64, groups)
			}
		}
		d, partition := i>>partitionBits, i&(1<<partitionBits-1)
		users := m.users[d]
		m.set.Clear()
		for _, w := range ws {
			w.agg.dlogs[d].Each(partition, func(lo uint64, rec []byte) {
				if m.set.Add(lo, binary.LittleEndian.Uint64(rec)) {
					users[binary.LittleEndian.Uint32(rec[8:])]++
				}
			})
		}
	})
	if err != nil {
		return nil, err
	}
	out := make([][]int64, plan.distinct)
	for d := range out {
		out[d] = make([]int64, groups)
		for i := range x.mergers {
			if x.mergers[i].users == nil {
				continue
			}
			for g, n := range x.mergers[i].users[d] {
				out[d][g] += n
			}
		}
	}

	return out, nil
}
