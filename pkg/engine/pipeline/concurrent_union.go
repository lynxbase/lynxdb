package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// UnionOrderMode controls how ConcurrentUnionIterator emits batches from
// multiple child iterators running in parallel.
type UnionOrderMode int

const (
	// OrderPreserved emits all batches from child[0] before child[1], etc.
	// Child goroutines are started lazily — child[i+1] only starts when
	// child[i] is fully drained. This avoids holding segment readers and
	// mmap handles open for the entire duration of earlier children.
	// Used by APPEND when downstream is order-sensitive (e.g., head, tail).
	OrderPreserved UnionOrderMode = iota

	// OrderInterleaved fans in batches from all children into a single
	// merged channel. Whichever child produces a batch first gets emitted.
	// Used by APPEND when downstream is commutative (stats, sort).
	OrderInterleaved

	// OrderConcurrent eagerly starts ALL child goroutines concurrently
	// (like OrderInterleaved) but reads from per-child channels in index
	// order (like OrderPreserved). This gives concurrent execution with
	// deterministic output ordering. Used by MULTISEARCH where branch
	// order must match the query declaration order.
	OrderConcurrent
)

// batchResult is the unit of communication between child goroutines and
// the ConcurrentUnionIterator's Next() method.
type batchResult struct {
	batch *Batch
	err   error
}

// ConcurrentUnionIterator concatenates output from multiple child iterators
// with branch-level parallelism. Each child runs in its own goroutine,
// producing batches into channels that the consumer reads from.
//
// Three modes are supported:
//   - OrderInterleaved: children run concurrently via a worker pool limited
//     to maxParallel goroutines, batches arrive in arbitrary order (fan-in
//     via single merged channel). Each worker drains one child entirely
//     before picking up the next, ensuring child iterators (which are NOT
//     thread-safe) are only accessed by one goroutine.
//   - OrderPreserved: children run sequentially in index order, but each
//     child's iteration is off-loaded to a goroutine with a buffered channel
//     for decoupled producer/consumer pacing. Child[i+1] only starts when
//     child[i] is exhausted (lazy start).
//   - OrderConcurrent: all children start concurrently (eager start) but
//     batches are emitted in child index order. Combines the parallelism
//     of OrderInterleaved with the deterministic output of OrderPreserved.
//
// Error handling: if any child returns an error, the master context is
// canceled (fail-fast), and the error is propagated to the consumer.
type ConcurrentUnionIterator struct {
	children    []Iterator
	mode        UnionOrderMode
	maxParallel int // soft goroutine limit (default: GOMAXPROCS)
	bufSize     int // per-child channel buffer (default: 2)

	// OrderInterleaved state: fan-in via single merged channel.
	mergedCh chan batchResult

	// OrderPreserved state: per-child channels with lazy start.
	childChs    []chan batchResult
	childCancel []context.CancelFunc
	current     int

	// masterCtx is the context derived from the first Next() call's context.
	// It is stored so that OrderPreserved child goroutines are derived from it
	// (not from individual Next(ctx) calls), enabling Close() to cancel them
	// via c.cancel().
	masterCtx context.Context
	cancel    context.CancelFunc // cancels masterCtx
	wg        sync.WaitGroup
	errMu     sync.Mutex // protects firstErr
	firstErr  error
	errOnce   sync.Once
	started   bool
}

// NewConcurrentUnionIterator creates a parallel union operator.
// Cfg provides MaxBranchParallelism and ChannelBufferSize with defaults.
func NewConcurrentUnionIterator(children []Iterator, mode UnionOrderMode, cfg *ParallelConfig) *ConcurrentUnionIterator {
	maxPar := cfg.effectiveMaxParallel()
	bufSz := cfg.effectiveBufferSize()

	return &ConcurrentUnionIterator{
		children:    children,
		mode:        mode,
		maxParallel: maxPar,
		bufSize:     bufSz,
	}
}

// Init initializes all child iterators. For OrderPreserved, only child[0]
// needs to be initialized upfront — others are initialized lazily. However,
// all children are initialized here to catch setup errors early (e.g., missing
// segment files, permission errors) before any goroutines are spawned.
func (c *ConcurrentUnionIterator) Init(ctx context.Context) error {
	for _, child := range c.children {
		if err := child.Init(ctx); err != nil {
			return err
		}
	}

	return nil
}

// Next returns the next batch from the parallel union. Returns (nil, nil)
// when all children are exhausted.
func (c *ConcurrentUnionIterator) Next(ctx context.Context) (*Batch, error) {
	if !c.started {
		c.start(ctx)
	}

	switch c.mode {
	case OrderInterleaved:
		return c.nextInterleaved(ctx)
	case OrderPreserved:
		return c.nextPreserved(ctx)
	case OrderConcurrent:
		return c.nextConcurrent(ctx)
	default:
		return c.nextInterleaved(ctx)
	}
}

// Close cancels all goroutines and waits for them to exit, then closes
// all child iterators. Uses a timeout to prevent goroutine leaks from
// misbehaving child iterators.
func (c *ConcurrentUnionIterator) Close() error {
	// Cancel master context. Since OrderPreserved child contexts are derived
	// from masterCtx (via startPreservedChild), this propagates to all child
	// goroutines. OrderInterleaved workers also use masterCtx directly.
	if c.cancel != nil {
		c.cancel()
	}

	// Wait with timeout to prevent goroutine leak from misbehaving children.
	done := make(chan struct{})
	go func() { c.wg.Wait(); close(done) }()

	const closeTimeout = 5 * time.Second
	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(closeTimeout):
		slog.Warn("ConcurrentUnionIterator: goroutine drain timeout, proceeding with Close")
	}

	for _, child := range c.children {
		child.Close()
	}

	return nil
}

// Schema returns nil — union schema is the superset of all children.
func (c *ConcurrentUnionIterator) Schema() []FieldInfo { return nil }

// setError records the first error from any child goroutine and cancels
// the master context. Thread-safe via errOnce + errMu.
func (c *ConcurrentUnionIterator) setError(err error) {
	c.errOnce.Do(func() {
		c.errMu.Lock()
		c.firstErr = err
		c.errMu.Unlock()
		c.cancel()
	})
}

// getError returns the first recorded error, if any. Thread-safe via errMu.
func (c *ConcurrentUnionIterator) getError() error {
	c.errMu.Lock()
	defer c.errMu.Unlock()

	return c.firstErr
}

// start initializes the goroutine machinery based on the union mode.
func (c *ConcurrentUnionIterator) start(ctx context.Context) {
	c.started = true
	c.masterCtx, c.cancel = context.WithCancel(ctx)

	switch c.mode {
	case OrderInterleaved:
		c.startInterleaved(c.masterCtx)
	case OrderPreserved:
		c.childChs = make([]chan batchResult, len(c.children))
		c.childCancel = make([]context.CancelFunc, len(c.children))
		// Lazy start: don't spawn goroutines yet. nextPreserved does it.
	case OrderConcurrent:
		c.startConcurrent(c.masterCtx)
	}
}

// startInterleaved spawns a worker pool of at most maxParallel goroutines.
// Workers pull children from a work channel, draining each child entirely
// before picking up the next. This ensures:
//  1. Each child iterator is accessed by exactly one goroutine (child iterators
//     are NOT thread-safe).
//  2. At most maxParallel goroutines run concurrently, bounding resource usage
//     when there are many children (e.g., 20 indexes with maxParallel=4).
func (c *ConcurrentUnionIterator) startInterleaved(ctx context.Context) {
	c.mergedCh = make(chan batchResult, c.bufSize*len(c.children))

	// Feed children into a work channel for the worker pool.
	work := make(chan Iterator, len(c.children))
	for _, child := range c.children {
		work <- child
	}
	close(work)

	numWorkers := c.maxParallel
	if numWorkers > len(c.children) {
		numWorkers = len(c.children)
	}

	for i := 0; i < numWorkers; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for child := range work {
				c.drainChild(ctx, child)
				// Check if we should stop (error from another worker or context canceled).
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

	// Close mergedCh when all goroutines complete.
	go func() { c.wg.Wait(); close(c.mergedCh) }()
}

// drainChild reads all batches from a single child and sends them to mergedCh.
// Returns when the child is exhausted, an error occurs, or the context is canceled.
func (c *ConcurrentUnionIterator) drainChild(ctx context.Context, child Iterator) {
	for {
		batch, err := child.Next(ctx)
		if err != nil {
			c.setError(err)
			select {
			case c.mergedCh <- batchResult{err: err}:
			case <-ctx.Done():
			}

			return
		}
		if batch == nil {
			return // child exhausted
		}
		select {
		case c.mergedCh <- batchResult{batch: batch}:
		case <-ctx.Done():
			return
		}
	}
}

// nextInterleaved reads from the single merged channel.
func (c *ConcurrentUnionIterator) nextInterleaved(_ context.Context) (*Batch, error) {
	// Fail-fast: check firstErr BEFORE blocking on channel read.
	if err := c.getError(); err != nil {
		return nil, err
	}

	// Select on both mergedCh and masterCtx so that Close() (which cancels
	// masterCtx) can interrupt a blocked channel read.
	select {
	case res, ok := <-c.mergedCh:
		if !ok {
			// Channel closed — all children exhausted. Safe to read firstErr
			// because all goroutines have completed (wg.Wait closed the channel).
			if err := c.getError(); err != nil {
				return nil, err
			}

			return nil, nil
		}
		if res.err != nil {
			return nil, res.err
		}

		return res.batch, nil
	case <-c.masterCtx.Done():
		// Prefer the original error (from setError) over context.Canceled,
		// since setError cancels masterCtx as a side effect.
		if err := c.getError(); err != nil {
			return nil, err
		}

		return nil, c.masterCtx.Err()
	}
}

// startPreservedChild spawns a goroutine for the child at the given index.
// The goroutine reads batches from the child and sends them to a per-child
// channel. The child context is derived from masterCtx (not the caller's ctx)
// so that Close() can cancel it via c.cancel().
func (c *ConcurrentUnionIterator) startPreservedChild(idx int) {
	ch := make(chan batchResult, c.bufSize)
	c.childChs[idx] = ch

	childCtx, childCancel := context.WithCancel(c.masterCtx)
	c.childCancel[idx] = childCancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(ch)
		child := c.children[idx]
		for {
			batch, err := child.Next(childCtx)
			if err != nil {
				c.setError(err)
				select {
				case ch <- batchResult{err: err}:
				case <-childCtx.Done():
				}

				return
			}
			if batch == nil {
				return // child exhausted
			}
			select {
			case ch <- batchResult{batch: batch}:
			case <-childCtx.Done():
				return
			}
		}
	}()
}

// nextPreserved reads children in order, starting each child's goroutine
// lazily only when the previous child is exhausted.
func (c *ConcurrentUnionIterator) nextPreserved(_ context.Context) (*Batch, error) {
	for c.current < len(c.children) {
		// Lazy start: only start the current child's goroutine if not yet started.
		if c.childChs[c.current] == nil {
			c.startPreservedChild(c.current)
		}

		// Fail-fast error check.
		if err := c.getError(); err != nil {
			return nil, err
		}

		// Select on both the child channel and masterCtx so that Close()
		// (which cancels masterCtx) can interrupt a blocked channel read.
		select {
		case res, ok := <-c.childChs[c.current]:
			if !ok {
				// Current child exhausted — advance to next.
				c.current++

				continue
			}
			if res.err != nil {
				return nil, res.err
			}

			return res.batch, nil
		case <-c.masterCtx.Done():
			// Prefer the original error over context.Canceled.
			if err := c.getError(); err != nil {
				return nil, err
			}

			return nil, c.masterCtx.Err()
		}
	}

	return nil, nil // all children exhausted
}

// startConcurrent eagerly spawns goroutines for ALL children at once.
// Unlike OrderPreserved (lazy start), every child begins producing batches
// immediately. Output is still consumed in index order by nextConcurrent.
func (c *ConcurrentUnionIterator) startConcurrent(ctx context.Context) {
	c.childChs = make([]chan batchResult, len(c.children))
	c.childCancel = make([]context.CancelFunc, len(c.children))
	for i := range c.children {
		c.startPreservedChild(i) // reuse per-child goroutine logic
	}
}

// nextConcurrent reads children in index order (deterministic), but all
// goroutines are already running concurrently (started by startConcurrent).
func (c *ConcurrentUnionIterator) nextConcurrent(_ context.Context) (*Batch, error) {
	for c.current < len(c.children) {
		// No lazy-start check — all goroutines already running.

		// Fail-fast error check.
		if err := c.getError(); err != nil {
			return nil, err
		}

		select {
		case res, ok := <-c.childChs[c.current]:
			if !ok {
				// Current child exhausted — advance to next.
				c.current++

				continue
			}
			if res.err != nil {
				return nil, res.err
			}

			return res.batch, nil
		case <-c.masterCtx.Done():
			if err := c.getError(); err != nil {
				return nil, err
			}

			return nil, c.masterCtx.Err()
		}
	}

	return nil, nil // all children exhausted
}
