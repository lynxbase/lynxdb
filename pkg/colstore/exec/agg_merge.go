package exec

import (
	"encoding/binary"
	"slices"

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
	table *vec.ByteTable
	set   *vec.HashSet128
	n     int
	// users counts the distinct values per distinct count and shared group.
	users [][]int64
	sink  sink
}

// mergeScratch is the memory a merger keeps between queries.
type mergeScratch struct {
	words  []int64
	batch  []colVec
	post   postEval
	having havingEval
}

var mergeScratchPool mem.Pool[mergeScratch]

func (m *merger) held() int {
	if m.arena == nil {
		return 0
	}
	n := 8*used(m.words) + m.post.held() + m.having.held() + m.sink.held() + m.arena.Size()
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
		m.words = m.words[:0]
		for i := range m.batch {
			m.batch[i].reset()
		}
		for i := range m.post.tmp {
			m.post.tmp[i].reset()
		}
		m.arena = mem.NewArena()
		m.table = vec.NewByteTable(m.arena)
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
	logged, total := false, 0
	for _, w := range ws {
		for i, f := range w.agg.floats {
			floats[i] = floats[i] || f
		}
		logged = logged || w.agg.log != nil
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
	switch {
	case len(plan.keys) == 0:
		words := append([]int64(nil), plan.init...)
		for _, w := range ws {
			plan.mergeWords(words, w.agg.words[:stride])
		}
		m.emit(x, nil, words, 1, floats, distinct)
	case !logged && total <= serialGroups && x.shared != nil:
		// The shared ids number the groups of every worker alike.
		groups := x.shared.table.Len()
		words := make([]int64, 0, groups*stride)
		for g := 0; g < groups; g++ {
			words = append(words, plan.init...)
		}
		for _, w := range ws {
			for g := 0; g < w.agg.groups; g++ {
				id := int(w.agg.global[g])
				plan.mergeWords(words[id*stride:(id+1)*stride], w.agg.words[g*stride:(g+1)*stride])
			}
		}
		if groups > 0 {
			m.emit(x, x.shared.table, words, groups, floats, distinct)
		}
	case !logged && total <= serialGroups:
		for _, w := range ws {
			for g := 0; g < w.agg.groups; g++ {
				state := w.agg.words[g*stride : (g+1)*stride]
				id := m.table.IDHashed(w.agg.table.Key(g), w.agg.table.KeyHash(g))
				if id == m.n {
					m.n++
					m.words = append(m.words, state...)

					continue
				}
				plan.mergeWords(m.words[id*stride:(id+1)*stride], state)
			}
		}
		if m.n > 0 {
			m.emit(x, m.table, m.words, m.n, floats, distinct)
		}
	default:
		if err := forEach(len(ws), slots, func(_, i int) { ws[i].agg.spill() }); err != nil {
			return nil, err
		}
		// A key lives in one partition, so partitions merge independently.
		err := forEach(1<<partitionBits, slots, func(slot, partition int) {
			m := x.merger(slot)
			m.partition(plan, partition, ws)
			if m.n > 0 {
				m.emit(x, m.table, m.words, m.n, floats, distinct)
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

// partition merges the logged states of one partition of every worker.
func (m *merger) partition(plan *aggPlan, partition int, ws []*worker) {
	stride := len(plan.words)
	m.table.Clear()
	records := 0
	for _, w := range ws {
		if w.agg.log != nil {
			records += w.agg.log.Len(partition)
		}
	}
	m.words, m.n = slices.Grow(m.words[:0], records*stride), 0
	for _, w := range ws {
		if w.agg.log == nil {
			continue
		}
		w.agg.log.Each(partition, func(h uint64, rec []byte) {
			k := len(rec) - 8*stride
			state := rec[k:]
			g := m.table.IDHashed(rec[:k], h)
			if g == m.n {
				m.n++
				for i := 0; i < stride; i++ {
					m.words = append(m.words, int64(binary.LittleEndian.Uint64(state[8*i:])))
				}

				return
			}
			plan.mergeRecord(m.words[g*stride:(g+1)*stride], state)
		})
	}
}

// mergeRecord folds a logged state into dst.
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
		default:
			dst[i] += v
		}
	}
}

// emit computes the plan's columns for n groups, whose keys table numbers
// and whose states are in words, and hands them to the merger's sink.
func (m *merger) emit(x *execution, table *vec.ByteTable, words []int64, n int, floats []bool, distinct [][]int64) {
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
		plan.column(&plan.outs[i], words, n, floats, distinct, &m.batch[keys+i])
	}
	out := m.batch
	if x.q.having != nil {
		out = m.having.run(x.q, out, n)
		n = out[0].len()
	}
	if x.q.post != nil {
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
