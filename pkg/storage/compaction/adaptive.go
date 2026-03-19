package compaction

import (
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveController adjusts the compaction rate based on observed query
// latency. When P99 latency exceeds the target, the rate is reduced.
// When latency is well below target, the rate is increased toward the max.
type AdaptiveController struct {
	mu sync.Mutex

	// Configuration.
	maxRate   int64         // maximum compaction rate (bytes/sec)
	minRate   int64         // minimum compaction rate (bytes/sec)
	targetP99 time.Duration // target P99 query latency

	// Current state.
	currentRate int64 // current compaction rate (bytes/sec)

	// Latency window: circular buffer of recent query latencies.
	window    []time.Duration
	windowIdx int
	windowLen int

	// Pause state.
	paused atomic.Bool
}

// AdaptiveConfig configures the adaptive controller.
type AdaptiveConfig struct {
	MaxRate    int64         // max compaction rate in bytes/sec (default: 200 MB/s)
	MinRate    int64         // min compaction rate in bytes/sec (default: 10 MB/s)
	TargetP99  time.Duration // target P99 query latency (default: 500ms)
	WindowSize int           // number of recent queries to track (default: 100)
}

// NewAdaptiveController creates an adaptive compaction throttle.
func NewAdaptiveController(cfg AdaptiveConfig) *AdaptiveController {
	if cfg.MaxRate <= 0 {
		cfg.MaxRate = 200 << 20 // 200 MB/s
	}
	if cfg.MinRate <= 0 {
		cfg.MinRate = 10 << 20 // 10 MB/s
	}
	if cfg.TargetP99 <= 0 {
		cfg.TargetP99 = 500 * time.Millisecond
	}
	windowSize := cfg.WindowSize
	if windowSize <= 0 {
		windowSize = 100
	}

	return &AdaptiveController{
		maxRate:     cfg.MaxRate,
		minRate:     cfg.MinRate,
		targetP99:   cfg.TargetP99,
		currentRate: cfg.MaxRate,
		window:      make([]time.Duration, windowSize),
	}
}

// RecordLatency records a query's execution latency.
// Thread-safe: called from query goroutines.
func (ac *AdaptiveController) RecordLatency(d time.Duration) {
	ac.mu.Lock()
	ac.window[ac.windowIdx] = d
	ac.windowIdx = (ac.windowIdx + 1) % len(ac.window)
	if ac.windowLen < len(ac.window) {
		ac.windowLen++
	}
	ac.mu.Unlock()
}

// Adjust recalculates the compaction rate based on observed latencies.
// Call periodically (e.g., every 5 seconds) from the compaction scheduler.
// Returns the new rate in bytes/sec.
func (ac *AdaptiveController) Adjust() int64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.windowLen == 0 {
		return ac.currentRate
	}

	p99 := ac.computeP99()

	// Auto-pause: when P99 exceeds 2× target, halt compaction entirely.
	// Resume when P99 drops below target.
	if p99 > ac.targetP99*2 {
		ac.paused.Store(true)
	} else if p99 < ac.targetP99 {
		ac.paused.Store(false)
	}

	if p99 > ac.targetP99 {
		// Latency too high: reduce rate by 25%.
		newRate := ac.currentRate * 3 / 4
		if newRate < ac.minRate {
			newRate = ac.minRate
		}
		ac.currentRate = newRate
	} else if p99 < ac.targetP99/2 {
		// Latency well below target: increase rate by 10%.
		newRate := ac.currentRate * 11 / 10
		if newRate > ac.maxRate {
			newRate = ac.maxRate
		}
		ac.currentRate = newRate
	}
	// else: latency in acceptable range, keep current rate.

	return ac.currentRate
}

// Rate returns the current compaction rate in bytes/sec.
func (ac *AdaptiveController) Rate() int64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	return ac.currentRate
}

// Paused returns whether compaction should be paused.
func (ac *AdaptiveController) Paused() bool {
	return ac.paused.Load()
}

// SetPaused sets whether compaction should be paused.
func (ac *AdaptiveController) SetPaused(p bool) {
	ac.paused.Store(p)
}

// computeP99 computes the P99 latency from the window.
// Must be called under ac.mu.Lock().
func (ac *AdaptiveController) computeP99() time.Duration {
	if ac.windowLen == 0 {
		return 0
	}

	// Copy and sort the active portion of the window.
	active := make([]time.Duration, ac.windowLen)
	copy(active, ac.window[:ac.windowLen])

	// Insertion sort for small windows (typically ~100 entries).
	for i := 1; i < len(active); i++ {
		for j := i; j > 0 && active[j] < active[j-1]; j-- {
			active[j], active[j-1] = active[j-1], active[j]
		}
	}

	// P99 index.
	idx := int(float64(len(active)) * 0.99)
	if idx >= len(active) {
		idx = len(active) - 1
	}

	return active[idx]
}
