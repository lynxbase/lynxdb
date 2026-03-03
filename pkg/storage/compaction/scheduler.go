package compaction

import (
	"container/heap"
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRateBytesPerSec = 100 << 20 // 100 MB/s
	defaultWorkerCount     = 2
)

// Scheduler manages a priority queue of compaction jobs, a token bucket
// rate limiter, and a pool of compaction workers.
type Scheduler struct {
	mu       sync.Mutex
	queue    jobQueue
	jobReady *sync.Cond

	compactor  *Compactor
	limiter    *TokenBucket
	workers    int
	logger     *slog.Logger
	onComplete func(*Job, *SegmentInfo, error) // callback after each job
	onError    func(*Job, error)               // callback for metrics on failure

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

	s := &Scheduler{
		compactor: c,
		limiter:   NewTokenBucket(rate),
		workers:   workers,
		logger:    logger,
	}
	s.jobReady = sync.NewCond(&s.mu)
	heap.Init(&s.queue)

	return s
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
	s.jobReady.Signal()
	s.mu.Unlock()
}

// SubmitAll adds multiple jobs to the queue.
func (s *Scheduler) SubmitAll(jobs []*Job) {
	s.mu.Lock()
	for _, j := range jobs {
		heap.Push(&s.queue, j)
	}
	if len(jobs) > 0 {
		s.jobReady.Broadcast()
	}
	s.mu.Unlock()
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
func (s *Scheduler) Stop() {
	s.running.Store(false)
	s.jobReady.Broadcast()
	s.wg.Wait()
}

func (s *Scheduler) worker(ctx context.Context, id int) {
	for {
		s.mu.Lock()
		for s.queue.Len() == 0 && s.running.Load() {
			s.jobReady.Wait()
		}
		if !s.running.Load() && s.queue.Len() == 0 {
			s.mu.Unlock()

			return
		}
		if s.queue.Len() == 0 {
			s.mu.Unlock()

			continue
		}
		job := heap.Pop(&s.queue).(*Job)
		onComplete := s.onComplete
		onError := s.onError
		s.mu.Unlock()

		// Rate limit: wait for tokens equal to total input size.
		var inputBytes int64
		for _, seg := range job.Plan.InputSegments {
			inputBytes += seg.Meta.SizeBytes
		}
		s.limiter.Wait(ctx, inputBytes)

		// Execute compaction.
		output, err := s.compactor.Execute(ctx, job.Plan)
		if err != nil {
			s.logger.Error("compaction job failed",
				"worker", id,
				"priority", job.Priority,
				"error", err,
			)
			if onError != nil {
				onError(job, err)
			}
		}

		if onComplete != nil {
			onComplete(job, output, err)
		}

		if ctx.Err() != nil {
			return
		}
	}
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
