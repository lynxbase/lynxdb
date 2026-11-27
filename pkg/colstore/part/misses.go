package part

import "sync/atomic"

// Misses holds the blocks of a part that are known to hold no row passing
// one condition, and any tighter upper time bounds established by a
// newest-first scan of the immutable rows.
type Misses struct {
	latest atomic.Pointer[[]atomic.Int64]
	words  []atomic.Uint64
	used   atomic.Uint64
}

// Has reports whether block is known to hold no such row.
func (m *Misses) Has(block int) bool {
	return m.words[block>>6].Load()&(1<<(block&63)) != 0
}

// Latest returns the last matching time learned for block, or its original
// upper bound. Zero represents an unknown bound and simply costs a scan.
func (m *Misses) Latest(block int, upper int64) int64 {
	if times := m.latest.Load(); times != nil {
		if t := (*times)[block].Load(); t != 0 {
			return min(upper, t)
		}
	}
	return upper
}

// SetLatest records the largest timestamp among all matches in block.
func (m *Misses) SetLatest(block int, t int64) {
	times := m.latest.Load()
	if times == nil {
		newTimes := make([]atomic.Int64, len(m.words)*64)
		if m.latest.CompareAndSwap(nil, &newTimes) {
			times = &newTimes
		} else {
			times = m.latest.Load()
		}
	}
	(*times)[block].Store(t)
}

// Add notes that block holds no such row.
func (m *Misses) Add(block int) {
	m.words[block>>6].Or(1 << (block & 63))
}

// keptConditions bounds the conditions that a part remembers blocks for.
const keptConditions = 128

// Misses returns what the part has learned about the condition that key
// names. Whoever asks with one key must mean one condition on rows.
func (p *Part) Misses(key string) *Misses {
	now := p.missClock.Add(1)
	p.missMu.RLock()
	m := p.misses[key]
	p.missMu.RUnlock()
	if m != nil {
		m.used.Store(now)

		return m
	}
	p.missMu.Lock()
	defer p.missMu.Unlock()
	if m := p.misses[key]; m != nil {
		return m
	}
	if p.misses == nil {
		p.misses = map[string]*Misses{}
	}
	if len(p.misses) >= keptConditions {
		// The least recently asked for of a few gives way.
		oldest, seen := "", 0
		for k, other := range p.misses {
			if seen == 0 || other.used.Load() < p.misses[oldest].used.Load() {
				oldest = k
			}
			if seen++; seen == 8 {
				break
			}
		}
		delete(p.misses, oldest)
	}
	m = &Misses{words: make([]atomic.Uint64, (p.NumBlocks()+63)/64)}
	m.used.Store(now)
	p.misses[key] = m

	return m
}
