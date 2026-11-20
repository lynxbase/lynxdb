package shred

import (
	"math/bits"
	"slices"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Run is rows From to To-1 of a batch.
type Run struct {
	Batch    *Batch
	From, To int
}

// interleaveScratch holds what Interleave reuses from call to call.
type interleaveScratch struct {
	// slots numbers the columns of the batch that a call fills, and cols
	// lists for each of them, by run, the column of the run's batch that it
	// takes values from.
	slots map[*Column]int
	outs  []*Column
	cols  []*Column
	// at lists the positions, among the rows appended, of the rows of each
	// run, starts where each run's list begins.
	at     []uint32
	starts []int
	taken  []int
	next   []int
	// present marks the rows appended that a column holds, before counts
	// the marks in the words before each, and lens the lengths of payloads.
	present []uint64
	before  []uint32
	lens    []uint32
}

// Interleave appends to b one row at a time from runs in the order that
// picks names them: pick i takes the next row of run picks[i]. Every run
// gives its rows in order from From on and leaves From past those it gave.
// The rows are accounted the share of the input bytes of their batches
// that they are of.
func (b *Batch) Interleave(runs []Run, picks []uint32) {
	n := len(picks)
	if n == 0 {
		return
	}
	g := b.interleave
	if g == nil {
		g = &interleaveScratch{slots: map[*Column]int{}}
		b.interleave = g
	}
	clear(g.slots)
	g.outs, g.cols = g.outs[:0], g.cols[:0]
	g.taken = append(g.taken[:0], make([]int, len(runs))...)
	for _, src := range picks {
		g.taken[src]++
	}
	g.starts = g.starts[:0]
	at := 0
	for _, k := range g.taken {
		g.starts = append(g.starts, at)
		at += k
	}
	g.at = slices.Grow(g.at[:0], n)[:n]
	g.next = append(g.next[:0], g.starts...)
	for i, src := range picks {
		g.at[g.next[src]] = uint32(i)
		g.next[src]++
	}
	for ri, r := range runs {
		if g.taken[ri] == 0 {
			continue
		}
		for _, c := range r.Batch.cols {
			if c.Count == 0 {
				continue
			}
			out := b.Col(c.Path, c.Kind)
			slot, ok := g.slots[out]
			if !ok {
				slot = len(g.outs)
				g.slots[out] = slot
				g.outs = append(g.outs, out)
				g.cols = append(g.cols, make([]*Column, len(runs))...)
			}
			g.cols[slot*len(runs)+ri] = c
		}
	}
	words := (n + 63) / 64
	base := b.Rows
	for slot, out := range g.outs {
		b.interleaveColumn(out, g.cols[slot*len(runs):(slot+1)*len(runs)], runs, base, n, words)
	}
	g.next = g.next[:0]
	for _, r := range runs {
		g.next = append(g.next, r.From)
	}
	for _, src := range picks {
		b.Times = append(b.Times, runs[src].Batch.Times[g.next[src]])
		g.next[src]++
	}
	for i, r := range runs {
		k := g.taken[i]
		if k == 0 {
			continue
		}
		if r.Batch.Rows > 0 {
			b.Bytes += int(int64(r.Batch.Bytes) * int64(k) / int64(r.Batch.Rows))
		}
		runs[i].From += k
	}
	b.Rows += n
}

// interleaveColumn appends to out the values that cols, by run, hold at the
// rows that the call takes.
func (b *Batch) interleaveColumn(out *Column, cols []*Column, runs []Run, base, n, words int) {
	g := b.interleave
	g.present = append(g.present[:0], make([]uint64, words)...)
	total := 0
	// The values of the rows taken from a run lie side by side in its
	// column; each is marked at the position its row is appended at.
	for ri, c := range cols {
		if c == nil || g.taken[ri] == 0 {
			continue
		}
		from, to := runs[ri].From, runs[ri].From+g.taken[ri]
		first, end := c.valueRange(from, to)
		at := g.at[g.starts[ri]:]
		for v := first; v < end; v++ {
			pos := at[c.Row(v)-from]
			g.present[pos>>6] |= 1 << (pos & 63)
		}
		total += end - first
	}
	if total == 0 {
		return
	}
	g.before = append(g.before[:0], make([]uint32, words)...)
	count := uint32(0)
	for w, word := range g.present {
		g.before[w] = count
		count += uint32(bits.OnesCount64(word))
	}
	rank := func(pos uint32) int {
		word := g.present[pos>>6]

		return int(g.before[pos>>6]) + bits.OnesCount64(word&(1<<(pos&63)-1))
	}
	start := out.Count
	switch out.Kind {
	case vec.KindBool:
		out.Bools = slices.Grow(out.Bools, total)[:start+total]
	case vec.KindInt:
		out.Ints = slices.Grow(out.Ints, total)[:start+total]
	case vec.KindFloat:
		out.Floats = slices.Grow(out.Floats, total)[:start+total]
	case vec.KindString, vec.KindVariant:
		g.lens = append(g.lens[:0], make([]uint32, total)...)
	}
	for ri, c := range cols {
		if c == nil || g.taken[ri] == 0 {
			continue
		}
		from, to := runs[ri].From, runs[ri].From+g.taken[ri]
		first, end := c.valueRange(from, to)
		at := g.at[g.starts[ri]:]
		for v := first; v < end; v++ {
			i := start + rank(at[c.Row(v)-from])
			switch out.Kind {
			case vec.KindBool:
				out.Bools[i] = c.Bools[v]
			case vec.KindInt:
				out.Ints[i] = c.Ints[v]
			case vec.KindFloat:
				out.Floats[i] = c.Floats[v]
			case vec.KindString, vec.KindVariant:
				g.lens[i-start] = c.Ends[v] - c.start(v)
			}
		}
	}
	if out.Kind == vec.KindString || out.Kind == vec.KindVariant {
		end := uint32(len(out.Data))
		out.Ends = slices.Grow(out.Ends, total)
		for _, l := range g.lens {
			end += l
			out.Ends = append(out.Ends, end)
		}
		data := len(out.Data)
		out.Data = slices.Grow(out.Data, int(end)-data)[:end]
		for ri, c := range cols {
			if c == nil || g.taken[ri] == 0 {
				continue
			}
			from, to := runs[ri].From, runs[ri].From+g.taken[ri]
			first, last := c.valueRange(from, to)
			at := g.at[g.starts[ri]:]
			for v := first; v < last; v++ {
				i := start + rank(at[c.Row(v)-from])
				copy(out.Data[out.start(i):out.Ends[i]], c.Bytes(v))
			}
		}
	}
	out.Count = start + total
	out.placeMarked(base, n, total, g.present)
}

// valueRange returns the values of the column whose rows are from to to-1.
func (c *Column) valueRange(from, to int) (first, end int) {
	if !c.Sparse {
		return min(from, c.Count), min(to, c.Count)
	}

	return lowerBound(c.RowIDs, uint32(from)), lowerBound(c.RowIDs, uint32(to))
}

// placeMarked records that the last count values belong to the rows
// base+pos for the positions pos below n marked in present, in order.
func (c *Column) placeMarked(base, n, count int, present []uint64) {
	had := c.Count - count
	if !c.Sparse && had == base && count == n {
		c.lastRow = int32(base + n - 1)

		return
	}
	if !c.Sparse {
		c.Sparse = true
		c.RowIDs = c.RowIDs[:0]
		for i := 0; i < had; i++ {
			c.RowIDs = append(c.RowIDs, uint32(i))
		}
	}
	for w, word := range present {
		for word != 0 {
			c.RowIDs = append(c.RowIDs, uint32(base+w*64+bits.TrailingZeros64(word)))
			word &= word - 1
		}
	}
	c.lastRow = int32(c.RowIDs[len(c.RowIDs)-1])
}
