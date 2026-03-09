package cluster

import (
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/cluster/meta"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
)

// ShardMapCache provides lock-free read access to the current shard map
// and piggybacked distributed subsystem state (field catalog, sources, splits).
// Writers call Update*() which perform atomic pointer swaps; readers
// call Get*() which are single atomic loads with zero allocation.
type ShardMapCache struct {
	current      atomic.Pointer[sharding.ShardMap]
	fieldCatalog atomic.Pointer[[]meta.GlobalFieldInfo]
	sources      atomic.Pointer[[]meta.GlobalSourceInfo]
	splits       atomic.Pointer[sharding.SplitRegistry]
}

// NewShardMapCache creates a new cache initialized with an empty shard map.
func NewShardMapCache() *ShardMapCache {
	c := &ShardMapCache{}
	c.current.Store(sharding.NewShardMap())

	return c
}

// Get returns the current shard map snapshot.
// Safe for concurrent use. Zero allocation, zero lock.
func (c *ShardMapCache) Get() *sharding.ShardMap {
	return c.current.Load()
}

// Update atomically replaces the cached shard map.
func (c *ShardMapCache) Update(sm *sharding.ShardMap) {
	c.current.Store(sm)
}

// GetFieldCatalog returns the cached global field catalog, or nil if
// no catalog has been received yet. Safe for concurrent use.
func (c *ShardMapCache) GetFieldCatalog() []meta.GlobalFieldInfo {
	p := c.fieldCatalog.Load()
	if p == nil {
		return nil
	}

	return *p
}

// UpdateFieldCatalog atomically replaces the cached global field catalog.
func (c *ShardMapCache) UpdateFieldCatalog(fc []meta.GlobalFieldInfo) {
	c.fieldCatalog.Store(&fc)
}

// GetSources returns the cached global source registry, or nil if
// no sources have been received yet. Safe for concurrent use.
func (c *ShardMapCache) GetSources() []meta.GlobalSourceInfo {
	p := c.sources.Load()
	if p == nil {
		return nil
	}

	return *p
}

// UpdateSources atomically replaces the cached global source registry.
func (c *ShardMapCache) UpdateSources(src []meta.GlobalSourceInfo) {
	c.sources.Store(&src)
}

// GetSplitRegistry returns the cached split registry, or nil if none set.
func (c *ShardMapCache) GetSplitRegistry() *sharding.SplitRegistry {
	return c.splits.Load()
}

// UpdateSplitRegistry atomically replaces the cached split registry.
func (c *ShardMapCache) UpdateSplitRegistry(sr *sharding.SplitRegistry) {
	c.splits.Store(sr)
}
