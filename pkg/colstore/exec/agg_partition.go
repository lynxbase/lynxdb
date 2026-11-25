package exec

import (
	"encoding/binary"
	"math/bits"
	"sync"

	"github.com/lynxbase/lynxdb/pkg/mem"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

const (
	// mergeBatch is the number of groups whose keys a partition looks up at
	// once.
	mergeBatch = 1024
	// warmGroups is the number of groups up to which a partition that the
	// calling goroutine has been merging into is taken to be in its cache,
	// where keys are found fastest one after another.
	warmGroups = 1 << 14
	// A query has at least 1<<minPartitionBits partitions, and eight to a
	// worker up to 1<<maxPartitionBits, so that a worker rarely finds the
	// partition it has groups for held by another.
	minPartitionBits = 8
	maxPartitionBits = 12
)

// stageBudget is the memory, in bytes, that the workers of a query may hold
// together in groups that wait to be merged, and as much again in the
// values of each distinct count that wait to be counted. A query whose
// groups fit in it merges them when the scan is over, partition by
// partition, each in the cache of the goroutine that merges it. A worker
// whose part of the budget is spent merges what it holds into the
// partitions while it scans, which costs more per group, since the
// partitions are then in no one's cache, and bounds the memory of the query
// by its groups and distinct values instead of its rows.
var stageBudget = 1 << 30

// wordPool holds the states of the groups of partitions between queries.
var wordPool mem.Sized[int64]

// evenRows lists the even numbers: the bounds of the key of record j of a
// batch lie at 2j and 2j+1.
var evenRows = func() (rows [mergeBatch]uint32) {
	for j := range rows {
		rows[j] = uint32(2 * j)
	}

	return rows
}()

// partitionBitsFor returns the number of partitions, as a power of two, of a
// query that runs on the given number of workers.
func partitionBitsFor(workers int) int {
	return min(max(bits.Len(uint(8*workers-1)), minPartitionBits), maxPartitionBits)
}

// groupPartition holds the groups whose keys hash into one partition, for
// all workers of a query: the table numbers the groups, words holds their
// states of fixed size, a row per group, and heap the others. A worker
// takes the partition to merge the groups it has staged for it.
type groupPartition struct {
	mu    sync.Mutex
	table *vec.ByteTable
	words []int64
	heap  stateHeap
	n     int
	// The padding keeps the partitions that two workers hold out of each
	// other's cache lines.
	_ [128]byte
}

// lookup is the buffers of one goroutine for the batches it merges: the
// hashes of the records of a batch, where the key of each starts and ends
// in its chunk, and the groups that the partition gives them.
type lookup struct {
	hashes []uint64
	bounds []uint32
	ids    []uint32
}

func (l *lookup) held() int { return 8*cap(l.hashes) + 4*(cap(l.bounds)+cap(l.ids)) }

func (gp *groupPartition) held() int {
	if gp.table == nil {
		return 0
	}

	return gp.table.Retained() + 8*cap(gp.words)
}

func (gp *groupPartition) release() {
	if gp.table != nil {
		gp.table.Release()
		wordPool.Put(gp.words)
	}
}

// reset drops the groups and keeps the memory for the next ones.
func (gp *groupPartition) reset() {
	gp.table.Clear()
	gp.heap.reset()
	gp.words, gp.n = gp.words[:0], 0
}

// merge folds the records of a chunk into the partition. With cold set the
// partition is not in the cache of the goroutine that calls, and the table
// finds the groups of a batch of records side by side; it does the same for
// a partition too large for the cache. What the partition allocates
// meanwhile comes from a, the arena of that goroutine, which must hold the
// partition unless every worker is done.
func (gp *groupPartition) merge(plan *aggPlan, chunk []byte, a *mem.Arena, l *lookup, cold bool) {
	if gp.table == nil {
		gp.table = vec.NewByteTable(a)
	}
	gp.table.SetArena(a)
	gp.heap.arena = a
	stride := len(plan.words)
	// What follows the key of a record: the words, and for a plan with
	// states of variable size those and their length.
	fixed, states := 8*stride, len(plan.states) != 0
	for at := 0; at < len(chunk); {
		if !cold && gp.n <= warmGroups {
			h, start, end := vec.RecordAt(chunk, at)
			at = end
			if states {
				end -= 4 + int(binary.LittleEndian.Uint32(chunk[end-4:]))
			}
			end -= fixed
			gp.fold(plan, gp.table.IDHashed(chunk[start:end], h), chunk[end:])

			continue
		}
		l.hashes, l.bounds, l.ids = sized(l.hashes, mergeBatch), sized(l.bounds, 2*mergeBatch), sized(l.ids, mergeBatch)
		n := 0
		for ; n < mergeBatch && at < len(chunk); n++ {
			h, start, end := vec.RecordAt(chunk, at)
			at = end
			if states {
				end -= 4 + int(binary.LittleEndian.Uint32(chunk[end-4:]))
			}
			l.hashes[n], l.bounds[2*n], l.bounds[2*n+1] = h, uint32(start), uint32(end-fixed)
		}
		ids := l.ids[:n]
		gp.table.IDs(ids, l.hashes[:n], &vec.Keys{Data: chunk, Offsets: l.bounds, Rows: evenRows[:n]}, gp.words, stride)
		for j, id := range ids {
			gp.fold(plan, int(id), chunk[l.bounds[2*j+1]:])
		}
	}
}

// fold merges a state as a record holds it, the words and then the states
// of variable size, into group g, which is the next group when the
// partition did not hold it.
func (gp *groupPartition) fold(plan *aggPlan, g int, state []byte) {
	stride := len(plan.words)
	if g != gp.n {
		plan.mergeRecord(gp.words[g*stride:(g+1)*stride], state)
		if len(plan.states) != 0 {
			plan.mergeStates(&gp.heap, gp.words[g*stride:(g+1)*stride], &stateSource{tail: state[8*stride:]})
		}

		return
	}
	gp.n++
	gp.words = wordPool.Grow(gp.words, stride)
	for i := 0; i < stride; i++ {
		gp.words = append(gp.words, int64(binary.LittleEndian.Uint64(state[8*i:])))
	}
	if len(plan.states) != 0 {
		plan.adoptStates(&gp.heap, gp.words[g*stride:], &stateSource{tail: state[8*stride:]})
	}
}

// stage keeps the record of a group for the partition of its key: the key,
// after the byte tag when tagged is set, the words of the group's state and
// its states of variable size. Once the worker holds its part of the
// query's budget in such records, it merges them into their partitions.
func (a *aggWorker) stage(h uint64, tag byte, tagged bool, key []byte, words []int64) {
	var tail []byte
	if len(a.plan.states) != 0 {
		a.s.rec = a.plan.appendStates(a.s.rec[:0], &a.heap, words)
		tail = a.s.rec
	}
	x := a.w.x
	if a.staged == nil {
		a.staged = vec.NewKeyPartitions(a.w.arena, x.partBits)
	}
	a.staged.AddKey(h, tag, tagged, key, words, tail)
	// What the worker holds for the codes of a part is of its share too.
	if a.staged.Size()+a.coded.waiting() > stageBudget/len(x.workers) {
		a.unstage()
	}
}

// unstage merges the groups that the worker has staged into their
// partitions.
func (a *aggWorker) unstage() {
	x := a.w.x
	a.drain(a.staged, func(p int) *sync.Mutex { return &x.partition(p).mu }, func(p int) {
		gp := x.partition(p)
		a.staged.Drain(p, func(chunk []byte) { gp.merge(a.plan, chunk, a.w.arena, &a.s.lookup, true) })
	})
}

// drain has take empty every partition of log that holds records, each
// while the worker holds the lock that lock names for it: first those that
// no other worker holds, then the others as they come free.
func (a *aggWorker) drain(log *vec.KeyPartitions, lock func(p int) *sync.Mutex, take func(p int)) {
	x := a.w.x
	parts := log.Partitions()
	a.s.busy = a.s.busy[:0]
	// Workers start at different partitions, so that they meet less.
	if a.order == 0 {
		a.order = int(x.unstaged.Add(1))
	}
	first := (a.order - 1) * parts / len(x.workers)
	for i := 0; i < parts; i++ {
		p := (first + i) % parts
		if log.Len(p) == 0 {
			continue
		}
		if mu := lock(p); mu.TryLock() {
			locked(mu, take, p)
		} else {
			a.s.busy = append(a.s.busy, uint32(p))
		}
	}
	for _, p := range a.s.busy {
		mu := lock(int(p))
		mu.Lock()
		locked(mu, take, int(p))
	}
}

// locked calls take for partition p and gives up the lock, which the caller
// holds, whatever becomes of the call.
func locked(mu *sync.Mutex, take func(p int), p int) {
	defer mu.Unlock()
	take(p)
}

// distinctPartition holds the values of one distinct count that hash into
// one partition, of every group and for all workers of a query: a worker
// takes it to count the values it has logged for it.
type distinctPartition struct {
	mu  sync.Mutex
	set *vec.HashSet128
	_   [128]byte
}

// settle counts the values that the worker has logged for distinct count d
// once they take its part of the query's budget: it adds them to the
// partitions and counts those that are new there.
func (a *aggWorker) settle(d int) {
	x := a.w.x
	log := a.dlogs[d]
	if log.Size() <= stageBudget/len(x.workers) {
		return
	}
	if a.users == nil {
		a.users = make([][]int64, a.plan.distinct)
	}
	a.drain(log, func(p int) *sync.Mutex { return &x.distinct(d, p).mu }, func(p int) {
		dp := x.distinct(d, p)
		if dp.set == nil {
			dp.set = vec.NewHashSet128()
		}
		log.Drain(p, func(chunk []byte) { a.users[d] = countDistinct(dp.set, chunk, a.users[d]) })
	})
}

// distinct returns partition p of distinct count d of the query.
func (x *execution) distinct(d, p int) *distinctPartition {
	x.dpartsOnce.Do(func() { x.dparts = make([]distinctPartition, x.q.agg.distinct<<partitionBits) })

	return &x.dparts[d<<partitionBits|p]
}

// partition returns partition p of the query's groups.
func (x *execution) partition(p int) *groupPartition {
	x.partsOnce.Do(func() { x.parts = make([]groupPartition, 1<<x.partBits) })

	return &x.parts[p]
}
