package compaction

import (
	"container/heap"
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRateBytesPerSec = 100 << 20 // 100 MB/s
	defaultWorkerCount     = 2
)

// ExecutorFn is a custom execution function for compaction jobs. When set via
// SetExecutor, the scheduler calls this instead of compactor.Execute(), allowing
// the server to inject its own logic (epoch advance, cache invalidation, etc.).
type ExecutorFn func(ctx context.Context, job *Job) error

// compactionKey uniquely identifies an (index, partition) pair for concurrency
// control. Different partitions within the same index can compact in parallel.
type compactionKey struct {
	Index     string
	Partition string
}

// Scheduler manages a priority queue of compaction jobs, a token bucket
// rate limiter, and a pool of compaction workers.
type Scheduler struct {
	mu         sync.Mutex
	queue      jobQueue
	jobReady   *sync.Cond
	activeKeys map[compactionKey]bool // tracks which (index, partition) pairs have in-flight jobs

	compactor    *Compactor
	executor     ExecutorFn // optional: custom execution logic (replaces compactor.Execute)
	adaptiveCtrl *AdaptiveController
	limiter      *TokenBucket
	cpuSem       chan struct{} // CPU semaphore: limits concurrent CPU-heavy merge execution
	workers      int
	logger       *slog.Logger
	onComplete   func(*Job, *SegmentInfo, error) // callback after each job
	onError      func(*Job, error)               // callback for metrics on failure

	running atomic.Bool
	wg      sync.WaitGroup
}

// SchedulerConfig configures the compaction scheduler.
type SchedulerConfig struct {
	Workers         int   // number of concurrent workers (default 2)
	RateBytesPerSec int64 // max compaction IO in bytes/sec (default 100MB/s)
}

// NewScheduler creates a compaction scheduler.
func NewScheduler(c *Compactor, cfg SchedulerConfig, logger *slog.Logger) *Scheduler {
	workers := cfg.Workers
	if workers < 1 {
		workers = defaultWorkerCount
	}
	rate := cfg.RateBytesPerSec
	if rate <= 0 {
		rate = defaultRateBytesPerSec
	}

	// CPU semaphore: limit concurrent CPU-heavy merge execution to
	// GOMAXPROCS/2 (minimum 1) to avoid starving query goroutines.
	cpuSlots := runtime.GOMAXPROCS(0) / 2
	if cpuSlots < 1 {
		cpuSlots = 1
	}

	s := &Scheduler{
		compactor:  c,
		limiter:    NewTokenBucket(rate),
		cpuSem:     make(chan struct{}, cpuSlots),
		workers:    workers,
		logger:     logger,
		activeKeys: make(map[compactionKey]bool),
	}
	s.jobReady = sync.NewCond(&s.mu)
	heap.Init(&s.queue)

	return s
}

// SetExecutor sets a custom execution function. When set, the scheduler calls
// this instead of compactor.Execute(), allowing the server to inject its own
// logic (epoch advance, cache invalidation, metrics). Must be called before Start.
func (s *Scheduler) SetExecutor(fn ExecutorFn) {
	s.mu.Lock()
	s.executor = fn
	s.mu.Unlock()
}

// SetAdaptiveController sets the adaptive controller for pause/resume checks.
// When set, workers check Paused() before executing and requeue if paused.
// Must be called before Start.
func (s *Scheduler) SetAdaptiveController(ac *AdaptiveController) {
	s.mu.Lock()
	s.adaptiveCtrl = ac
	s.mu.Unlock()
}

// SetOnComplete sets the callback invoked after each compaction job finishes.
func (s *Scheduler) SetOnComplete(fn func(*Job, *SegmentInfo, error)) {
	s.mu.Lock()
	s.onComplete = fn
	s.mu.Unlock()
}

// SetOnError sets the callback invoked when a compaction job fails.
// Use this to wire compaction errors into metrics counters.
func (s *Scheduler) SetOnError(fn func(*Job, error)) {
	s.mu.Lock()
	s.onError = fn
	s.mu.Unlock()
}

// Submit adds a compaction job to the priority queue.
func (s *Scheduler) Submit(job *Job) {
	s.mu.Lock()
	heap.Push(&s.queue, job)
	queueLen := s.queue.Len()
	s.jobReady.Signal()
	s.mu.Unlock()

	s.logger.Debug("compaction job submitted",
		"index", job.Index,
		"partition", job.Partition,
		"priority", job.Priority,
		"queue_depth", queueLen,
	)
}

// SubmitAll adds multiple jobs to the queue.
func (s *Scheduler) SubmitAll(jobs []*Job) {
	s.mu.Lock()
	for _, j := range jobs {
		heap.Push(&s.queue, j)
	}
	if len(jobs) > 0 {
		s.jobReady.Broadcast()
		s.logger.Debug("compaction jobs batch submitted",
			"count", len(jobs),
			"queue_depth", s.queue.Len(),
		)
	}
	s.mu.Unlock()
}

// Limiter returns the rate limiter so the adaptive controller can update the rate.
func (s *Scheduler) Limiter() *TokenBucket {
	return s.limiter
}

// QueueLen returns the number of pending jobs.
func (s *Scheduler) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.queue.Len()
}

// Start launches worker goroutines. Call Stop to shut down.
func (s *Scheduler) Start(ctx context.Context) {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go func(id int) {
			defer s.wg.Done()
			s.worker(ctx, id)
		}(i)
	}
}

// Stop signals workers to shut down, wakes blocked waiters, and waits
// for all workers to finish their current job before returning.
//
// Hold s.mu while flipping running and broadcasting: a worker that has
// already evaluated the cond predicate (queue empty && running=true) but
// has not yet entered jobReady.Wait() would otherwise miss the broadcast
// and block forever, deadlocking wg.Wait().
func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.running.Store(false)
	s.jobReady.Broadcast()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *Scheduler) worker(ctx context.Context, id int) {
	// Set idle I/O priority so compaction yields to query I/O on Linux.
	SetCompactionIOPriority()

	for {
		s.mu.Lock()
		for s.queue.Len() == 0 && s.running.Load() {
			s.jobReady.Wait()
		}
		// Stop is terminal for pending work: once shutdown begins, workers exit
		// after their current job instead of draining the remaining queue.
		if !s.running.Load() {
			s.mu.Unlock()

			return
		}
		if s.queue.Len() == 0 {
			s.mu.Unlock()

			continue
		}

		// Find the first job whose (index, partition) is not already being compacted.
		var job *Job
		job = s.popAvailableJob()
		if job == nil {
			// All queued jobs are for (index, partition) pairs currently being compacted.
			// Wait for a signal (job completion or new submission).
			s.jobReady.Wait()
			s.mu.Unlock()

			continue
		}

		key := compactionKey{Index: job.Index, Partition: job.Partition}
		s.activeKeys[key] = true
		executor := s.executor
		adaptiveCtrl := s.adaptiveCtrl
		onComplete := s.onComplete
		onError := s.onError
		s.logger.Debug("compaction worker dequeue",
			"worker", id,
			"index", job.Index,
			"partition", job.Partition,
			"priority", job.Priority,
			"input_count", len(job.Plan.InputSegments),
		)
		s.mu.Unlock()

		// Check if compaction is paused by adaptive controller (C2).
		// Requeue the job and sleep briefly before retrying.
		if adaptiveCtrl != nil && adaptiveCtrl.Paused() {
			s.mu.Lock()
			heap.Push(&s.queue, job)
			delete(s.activeKeys, key)
			s.jobReady.Signal()
			s.logger.Debug("compaction paused, requeueing",
				"index", job.Index,
				"partition", job.Partition,
				"reason", adaptiveCtrl.PausedReason(),
			)
			s.mu.Unlock()

			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return
			}

			continue
		}

		// Acquire CPU semaphore (C3): limits concurrent CPU-heavy merge execution
		// to GOMAXPROCS/2 to avoid starving query goroutines.
		select {
		case s.cpuSem <- struct{}{}:
			s.logger.Debug("compaction cpu semaphore acquired",
				"worker", id,
			)
		case <-ctx.Done():
			// Context canceled — release the key and exit.
			s.mu.Lock()
			delete(s.activeKeys, key)
			heap.Push(&s.queue, job)
			s.jobReady.Signal()
			s.mu.Unlock()

			return
		}

		// Rate limiting is now applied per-batch inside StreamingMerge
		// (via the limiter passed through the executor). No upfront token
		// consumption — this prevents bursty I/O from large jobs.

		// Execute compaction: use custom executor if set, otherwise default.
		var output *SegmentInfo
		var err error
		execStart := time.Now()
		if executor != nil {
			err = executor(ctx, job)
		} else {
			output, err = s.compactor.Execute(ctx, job.Plan)
		}

		// Release CPU semaphore.
		<-s.cpuSem

		s.logger.Debug("compaction job complete",
			"worker", id,
			"index", job.Index,
			"partition", job.Partition,
			"duration_ms", time.Since(execStart).Milliseconds(),
			"error", err,
		)

		if err != nil {
			s.logger.Error("compaction job failed",
				"worker", id,
				"priority", job.Priority,
				"index", job.Index,
				"partition", job.Partition,
				"error", err,
			)
			if onError != nil {
				onError(job, err)
			}
		}

		// Release the (index, partition) lock and wake other workers that may
		// be waiting for this key to become available.
		s.mu.Lock()
		delete(s.activeKeys, key)
		s.jobReady.Broadcast()
		s.mu.Unlock()

		if onComplete != nil {
			onComplete(job, output, err)
		}

		if ctx.Err() != nil {
			return
		}
	}
}

// popAvailableJob removes and returns the highest-priority job whose (index, partition)
// is not currently active. If all queued jobs have active keys, it returns nil.
// Must be called with s.mu held.
func (s *Scheduler) popAvailableJob() *Job {
	// Scan the queue for the first job whose (index, partition) is not active.
	// The queue is a min-heap by priority, so we iterate in priority order
	// by temporarily removing and re-pushing blocked jobs.
	var deferred []*Job

	for s.queue.Len() > 0 {
		candidate := heap.Pop(&s.queue).(*Job)
		key := compactionKey{Index: candidate.Index, Partition: candidate.Partition}
		if !s.activeKeys[key] {
			// Found an available job. Re-push any deferred jobs.
			for _, d := range deferred {
				heap.Push(&s.queue, d)
			}
			return candidate
		}
		deferred = append(deferred, candidate)
		s.logger.Debug("compaction job deferred",
			"index", candidate.Index,
			"partition", candidate.Partition,
			"reason", "active_key",
		)
	}

	// All jobs were blocked. Re-push them all.
	for _, d := range deferred {
		heap.Push(&s.queue, d)
	}

	return nil
}

// Priority queue (min-heap by priority)

type jobQueue []*Job

func (q jobQueue) Len() int { return len(q) }
func (q jobQueue) Less(i, j int) bool {
	return q[i].Priority < q[j].Priority // lower = higher priority
}
func (q jobQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

func (q *jobQueue) Push(x interface{}) {
	*q = append(*q, x.(*Job))
}
func (q *jobQueue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*q = old[:n-1]

	return item
}

// Token Bucket Rate Limiter

// TokenBucket provides a simple token bucket rate limiter.
// Tokens refill at `rate` bytes per second.
type TokenBucket struct {
	mu       sync.Mutex
	tokens   int64
	rate     int64 // bytes per second
	lastFill time.Time
}

// NewTokenBucket creates a rate limiter with the given rate in bytes/sec.
func NewTokenBucket(bytesPerSec int64) *TokenBucket {
	return &TokenBucket{
		tokens:   bytesPerSec, // start with 1 second worth of tokens
		rate:     bytesPerSec,
		lastFill: time.Now(),
	}
}

// SetRate updates the token refill rate. Thread-safe.
func (tb *TokenBucket) SetRate(bytesPerSec int64) {
	tb.mu.Lock()
	tb.refill() // settle tokens at old rate before changing
	tb.rate = bytesPerSec
	tb.mu.Unlock()
}

// Wait blocks until `n` tokens are available or ctx is canceled.
func (tb *TokenBucket) Wait(ctx context.Context, n int64) {
	for {
		if ctx.Err() != nil {
			return
		}
		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= n {
			tb.tokens -= n
			tb.mu.Unlock()

			return
		}
		// Calculate time to wait for enough tokens.
		deficit := n - tb.tokens
		waitDur := time.Duration(float64(deficit) / float64(tb.rate) * float64(time.Second))
		tb.mu.Unlock()

		timer := time.NewTimer(waitDur)
		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

// TryConsume attempts to consume n tokens without blocking.
// Returns true if tokens were consumed.
func (tb *TokenBucket) TryConsume(n int64) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()
	if tb.tokens >= n {
		tb.tokens -= n

		return true
	}

	return false
}

// Available returns the current number of available tokens.
func (tb *TokenBucket) Available() int64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.refill()

	return tb.tokens
}

func (tb *TokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastFill)
	if elapsed <= 0 {
		return
	}
	tb.lastFill = now

	newTokens := int64(float64(tb.rate) * elapsed.Seconds())
	tb.tokens += newTokens

	// Cap at 2x rate (2 seconds of burst).
	maxTokens := tb.rate * 2
	if tb.tokens > maxTokens {
		tb.tokens = maxTokens
	}
}
