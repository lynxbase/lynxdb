package part

import "sync/atomic"

// Misses holds the blocks of a part that are known to hold no row passing
// one condition.
type Misses struct {
	words []atomic.Uint64
	used  atomic.Uint64
}

// Has reports whether block is known to hold no such row.
func (m *Misses) Has(block int) bool {
	return m.words[block>>6].Load()&(1<<(block&63)) != 0
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
