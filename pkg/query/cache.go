package query

import (
	"container/list"
	"sync"
)

// Compiling depends on the query text alone: relative times stay symbolic
// until execution and source schemas are discovered at read time. A
// compiled plan therefore never goes stale, and plans are cached by text.
const (
	// planCacheEntries bounds the number of cached plans.
	planCacheEntries = 1024
	// planCacheMaxQuery is the longest query text whose plan is cached; it
	// bounds the memory an entry holds.
	planCacheMaxQuery = 2 << 10
)

type planCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	// order holds *Compiled values, most recently used first.
	order list.List
	max   int
}

var plans = &planCache{entries: map[string]*list.Element{}, max: planCacheEntries}

func (c *planCache) get(src string) (*Compiled, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[src]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(e)
	return e.Value.(*Compiled), true
}

func (c *planCache) put(src string, compiled *Compiled) {
	if len(src) > planCacheMaxQuery {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[src]; ok {
		return
	}
	c.entries[src] = c.order.PushFront(compiled)
	for c.order.Len() > c.max {
		last := c.order.Back()
		c.order.Remove(last)
		delete(c.entries, last.Value.(*Compiled).Source)
	}
}
