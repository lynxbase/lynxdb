package planner

import (
	"hash/fnv"
	"strings"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/optimizer"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// PlanCache caches optimized query plans by normalized query string.
// Uses CLOCK eviction and TTL-based expiration.
type PlanCache struct {
	mu       sync.RWMutex
	entries  map[uint64]*planCacheEntry
	clock    *planClockBuffer
	ttl      time.Duration
	capacity int
}

type planCacheEntry struct {
	plan       *Plan
	hash       uint64
	normalized string // normalized query string for collision detection (P1)
	createdAt  time.Time
}

// NewPlanCache creates a plan cache with the given capacity and TTL.
func NewPlanCache(capacity int, ttl time.Duration) *PlanCache {
	if capacity <= 0 {
		capacity = 1024
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	return &PlanCache{
		entries:  make(map[uint64]*planCacheEntry, capacity),
		clock:    newPlanClockBuffer(capacity),
		ttl:      ttl,
		capacity: capacity,
	}
}

// Get looks up a cached plan by query string. Returns the plan and true on hit.
// Verifies the normalized query string matches to detect hash collisions (P1).
func (c *PlanCache) Get(query string) (*Plan, bool) {
	norm := normalize(query)
	h := hashNormalized(norm)
	c.mu.RLock()
	entry, ok := c.entries[h]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}
	// Collision detection: verify the normalized query matches (P1).
	if entry.normalized != norm {
		return nil, false
	}
	if time.Since(entry.createdAt) > c.ttl {
		c.mu.Lock()
		delete(c.entries, h)
		c.clock.remove(h)
		c.mu.Unlock()

		return nil, false
	}
	c.mu.Lock()
	c.clock.access(h)
	c.mu.Unlock()

	return entry.plan.Clone(), true
}

// Put stores a plan in the cache.
func (c *PlanCache) Put(query string, plan *Plan) {
	norm := normalize(query)
	h := hashNormalized(norm)
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[h] = &planCacheEntry{
		plan:       plan,
		hash:       h,
		normalized: norm,
		createdAt:  time.Now(),
	}
	evicted := c.clock.insert(h)
	if evicted != 0 {
		delete(c.entries, evicted)
	}
}

// normalize lowercases and collapses whitespace in a query string.
func normalize(query string) string {
	return strings.Join(strings.Fields(strings.ToLower(query)), " ")
}

// hashNormalized computes FNV-1a hash of an already-normalized query string.
func hashNormalized(normalized string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(normalized))

	return h.Sum64()
}

// Clone creates a deep copy of a Plan, including the AST.
func (p *Plan) Clone() *Plan {
	if p == nil {
		return nil
	}
	clone := &Plan{
		RawQuery:   p.RawQuery,
		ResultType: p.ResultType,
	}
	if p.Program != nil {
		clone.Program = cloneProgram(p.Program)
	}
	if p.Hints != nil {
		h := *p.Hints
		if p.Hints.TimeBounds != nil {
			tb := *p.Hints.TimeBounds
			h.TimeBounds = &tb
		}
		h.SearchTerms = append([]string(nil), p.Hints.SearchTerms...)
		h.RequiredCols = append([]string(nil), p.Hints.RequiredCols...)
		h.FieldPredicates = append([]spl2.FieldPredicate(nil), p.Hints.FieldPredicates...)
		clone.Hints = &h
	}
	if p.OptimizerStats != nil {
		clone.OptimizerStats = make(map[string]int, len(p.OptimizerStats))
		for k, v := range p.OptimizerStats {
			clone.OptimizerStats[k] = v
		}
	}
	if p.ExternalTimeBounds != nil {
		tb := *p.ExternalTimeBounds
		clone.ExternalTimeBounds = &tb
	}

	// Clone profiling fields (shallow copy of RuleDetails slice is fine —
	// optimizer.RuleDetail is a value type with no pointers).
	clone.ParseDuration = p.ParseDuration
	clone.OptimizeDuration = p.OptimizeDuration
	clone.TotalRules = p.TotalRules
	if len(p.RuleDetails) > 0 {
		clone.RuleDetails = append([]optimizer.RuleDetail(nil), p.RuleDetails...)
	}

	return clone
}

// cloneProgram creates a shallow-ish clone of Program.
// Commands are shared (they're treated as immutable after optimization).
func cloneProgram(prog *spl2.Program) *spl2.Program {
	clone := &spl2.Program{}
	if prog.Main != nil {
		clone.Main = cloneQuery(prog.Main)
	}
	if len(prog.Datasets) > 0 {
		clone.Datasets = make([]spl2.DatasetDef, len(prog.Datasets))
		for i, d := range prog.Datasets {
			clone.Datasets[i] = spl2.DatasetDef{
				Name:  d.Name,
				Query: cloneQuery(d.Query),
			}
		}
	}

	return clone
}

func cloneQuery(q *spl2.Query) *spl2.Query {
	clone := &spl2.Query{
		Source:   q.Source,
		Commands: append([]spl2.Command(nil), q.Commands...),
	}
	if q.Annotations != nil {
		clone.Annotations = make(map[string]interface{}, len(q.Annotations))
		for k, v := range q.Annotations {
			clone.Annotations[k] = v
		}
	}

	return clone
}

type planClockBuffer struct {
	slots    []planClockSlot
	capacity int
	hand     int
	count    int
	keyIndex map[uint64]int
}

type planClockSlot struct {
	key     uint64
	refBit  bool
	present bool
}

func newPlanClockBuffer(capacity int) *planClockBuffer {
	return &planClockBuffer{
		slots:    make([]planClockSlot, capacity),
		capacity: capacity,
		keyIndex: make(map[uint64]int, capacity),
	}
}

func (cb *planClockBuffer) access(key uint64) {
	if idx, ok := cb.keyIndex[key]; ok {
		cb.slots[idx].refBit = true
	}
}

func (cb *planClockBuffer) insert(key uint64) uint64 {
	if _, ok := cb.keyIndex[key]; ok {
		cb.access(key)

		return 0
	}
	var evicted uint64
	if cb.count >= cb.capacity {
		evicted = cb.evict()
	}
	for i := 0; i < cb.capacity; i++ {
		idx := (cb.hand + i) % cb.capacity
		if !cb.slots[idx].present {
			cb.slots[idx] = planClockSlot{key: key, refBit: true, present: true}
			cb.keyIndex[key] = idx
			cb.count++
			cb.hand = (idx + 1) % cb.capacity

			return evicted
		}
	}

	return evicted
}

func (cb *planClockBuffer) evict() uint64 {
	for i := 0; i < 2*cb.capacity; i++ {
		idx := cb.hand
		cb.hand = (cb.hand + 1) % cb.capacity
		if !cb.slots[idx].present {
			continue
		}
		if cb.slots[idx].refBit {
			cb.slots[idx].refBit = false

			continue
		}
		evicted := cb.slots[idx].key
		cb.slots[idx].present = false
		delete(cb.keyIndex, evicted)
		cb.count--

		return evicted
	}
	// Fallback.
	idx := cb.hand
	cb.hand = (cb.hand + 1) % cb.capacity
	if cb.slots[idx].present {
		evicted := cb.slots[idx].key
		cb.slots[idx].present = false
		delete(cb.keyIndex, evicted)
		cb.count--

		return evicted
	}

	return 0
}

func (cb *planClockBuffer) remove(key uint64) {
	idx, ok := cb.keyIndex[key]
	if !ok {
		return
	}
	cb.slots[idx].present = false
	delete(cb.keyIndex, key)
	cb.count--
}
