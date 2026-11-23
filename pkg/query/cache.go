package query

import (
	"container/list"
	"sync"
)

// Compiling depends on the query text and its options alone: relative times
// stay symbolic until execution and source schemas are discovered at read
// time. A compiled plan therefore never goes stale, and plans are cached by
// text and options.
const (
	// planCacheEntries bounds the number of cached plans.
	planCacheEntries = 1024
	// planCacheMaxQuery is the longest query text whose plan is cached; it
	// bounds the memory an entry holds.
	planCacheMaxQuery = 2 << 10
)

// planKey identifies a plan by the text and options it was compiled from.
type planKey struct {
	src  string
	opts Options
}

type cachedPlan struct {
	key planKey
	c   *Compiled
}

type planCache struct {
	mu      sync.Mutex
	entries map[planKey]*list.Element
	// order holds *cachedPlan values, most recently used first.
	order list.List
	max   int
}

var plans = &planCache{entries: map[planKey]*list.Element{}, max: planCacheEntries}

func (c *planCache) get(key planKey) (*Compiled, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*cachedPlan).c, true
}

func (c *planCache) put(key planKey, compiled *Compiled) {
	if len(key.src) > planCacheMaxQuery {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[key]; ok {
		return
	}
	c.entries[key] = c.order.PushFront(&cachedPlan{key: key, c: compiled})
	for c.order.Len() > c.max {
		last := c.order.Back()
		c.order.Remove(last)
		delete(c.entries, last.Value.(*cachedPlan).key)
	}
}
