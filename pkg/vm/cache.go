package vm

import (
	"container/list"
	"hash/fnv"
	"sync"
)

const defaultMaxCacheEntries = 10_000

// ProgramCache caches compiled programs by expression string hash with LRU eviction.
// When the cache exceeds maxEntries, the least recently used entry is evicted.
type ProgramCache struct {
	mu         sync.RWMutex
	cache      map[uint64]*list.Element
	lru        *list.List
	maxEntries int
	evictions  int64
}

type cacheEntry struct {
	key  uint64
	prog *Program
}

// NewProgramCache creates a new thread-safe program cache with default capacity (10K entries).
func NewProgramCache() *ProgramCache {
	return NewProgramCacheWithMax(defaultMaxCacheEntries)
}

// NewProgramCacheWithMax creates a new thread-safe program cache with the given max capacity.
func NewProgramCacheWithMax(maxEntries int) *ProgramCache {
	if maxEntries <= 0 {
		maxEntries = defaultMaxCacheEntries
	}

	return &ProgramCache{
		cache:      make(map[uint64]*list.Element),
		lru:        list.New(),
		maxEntries: maxEntries,
	}
}

// Get retrieves a cached program by expression string.
// Promotes the entry to the front of the LRU list on hit.
func (pc *ProgramCache) Get(expr string) *Program {
	h := hashExpr(expr)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	elem, ok := pc.cache[h]
	if !ok {
		return nil
	}

	// Promote to front (most recently used).
	pc.lru.MoveToFront(elem)

	return elem.Value.(*cacheEntry).prog
}

// Put stores a compiled program in the cache. Evicts the least recently used
// entry if the cache is at capacity.
func (pc *ProgramCache) Put(expr string, prog *Program) {
	h := hashExpr(expr)
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Update existing entry.
	if elem, ok := pc.cache[h]; ok {
		pc.lru.MoveToFront(elem)
		elem.Value.(*cacheEntry).prog = prog

		return
	}

	// Evict LRU entry if at capacity.
	if pc.lru.Len() >= pc.maxEntries {
		back := pc.lru.Back()
		if back != nil {
			evicted := pc.lru.Remove(back).(*cacheEntry)
			delete(pc.cache, evicted.key)
			pc.evictions++
		}
	}

	// Insert new entry at front.
	elem := pc.lru.PushFront(&cacheEntry{key: h, prog: prog})
	pc.cache[h] = elem
}

// Len returns the number of cached programs.
func (pc *ProgramCache) Len() int {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return pc.lru.Len()
}

// Evictions returns the total number of LRU evictions.
func (pc *ProgramCache) Evictions() int64 {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	return pc.evictions
}

func hashExpr(expr string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(expr))

	return h.Sum64()
}
