package compaction

import (
	"runtime/debug"
	"testing"
	"time"
)

func TestAdaptiveController_ReducesRateOnHighLatency(t *testing.T) {
	disableGC := false
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:         200 << 20,
		MinRate:         10 << 20,
		TargetP99:       500 * time.Millisecond,
		WindowSize:      10,
		EnableGCMonitor: &disableGC,
	})

	initialRate := ac.Rate()

	// Record latencies above the P99 target.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(800 * time.Millisecond)
	}

	newRate := ac.Adjust()
	if newRate >= initialRate {
		t.Errorf("expected rate to decrease: got %d, initial %d", newRate, initialRate)
	}

	// 25% reduction.
	expected := initialRate * 3 / 4
	if newRate != expected {
		t.Errorf("expected rate %d, got %d", expected, newRate)
	}
}

func TestAdaptiveController_IncreasesRateOnLowLatency(t *testing.T) {
	disableGC := false
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:         200 << 20,
		MinRate:         10 << 20,
		TargetP99:       500 * time.Millisecond,
		WindowSize:      10,
		EnableGCMonitor: &disableGC,
	})

	// First reduce the rate so there is room to increase.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(800 * time.Millisecond)
	}
	ac.Adjust()
	reducedRate := ac.Rate()

	// Now record latencies well below target/2 = 250ms.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	newRate := ac.Adjust()
	if newRate <= reducedRate {
		t.Errorf("expected rate to increase: got %d, reduced %d", newRate, reducedRate)
	}

	// 10% increase.
	expected := reducedRate * 11 / 10
	if newRate != expected {
		t.Errorf("expected rate %d, got %d", expected, newRate)
	}
}

func TestAdaptiveController_StaysStableInRange(t *testing.T) {
	// Disable the GC monitor: this test asserts that the rate stays stable
	// when latency is in the holding range. With the monitor enabled,
	// real-process GC pressure (e.g., from -race) can push gcFrac above
	// targetGCFraction (0.15) and trigger a 25% rate cut, making the test
	// flaky on busy hosts.
	disableGC := false
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:         200 << 20,
		MinRate:         10 << 20,
		TargetP99:       500 * time.Millisecond,
		WindowSize:      10,
		EnableGCMonitor: &disableGC,
	})

	// Record latencies between target/2 (250ms) and target (500ms).
	for i := 0; i < 10; i++ {
		ac.RecordLatency(350 * time.Millisecond)
	}

	rateBefore := ac.Rate()
	ac.Adjust()
	rateAfter := ac.Rate()

	if rateBefore != rateAfter {
		t.Errorf("expected rate to stay stable: before %d, after %d", rateBefore, rateAfter)
	}
}

func TestAdaptiveController_MinRateFloor(t *testing.T) {
	minRate := int64(10 << 20)
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:    200 << 20,
		MinRate:    minRate,
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
	})

	// Repeatedly adjust with high latency to drive the rate down.
	for round := 0; round < 50; round++ {
		for i := 0; i < 10; i++ {
			ac.RecordLatency(2 * time.Second)
		}
		ac.Adjust()
	}

	rate := ac.Rate()
	if rate < minRate {
		t.Errorf("rate %d fell below min %d", rate, minRate)
	}
	if rate != minRate {
		t.Errorf("expected rate to settle at min %d, got %d", minRate, rate)
	}
}

func TestAdaptiveController_MaxRateCap(t *testing.T) {
	maxRate := int64(200 << 20)
	disableGC := false
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:         maxRate,
		MinRate:         10 << 20,
		TargetP99:       500 * time.Millisecond,
		WindowSize:      10,
		EnableGCMonitor: &disableGC,
	})

	// Record latencies well below target to push rate up.
	for round := 0; round < 50; round++ {
		for i := 0; i < 10; i++ {
			ac.RecordLatency(10 * time.Millisecond)
		}
		ac.Adjust()
	}

	rate := ac.Rate()
	if rate > maxRate {
		t.Errorf("rate %d exceeded max %d", rate, maxRate)
	}
	if rate != maxRate {
		t.Errorf("expected rate to stay at max %d, got %d", maxRate, rate)
	}
}

func TestAdaptiveController_PauseResume(t *testing.T) {
	ac := NewAdaptiveController(AdaptiveConfig{})

	if ac.Paused() {
		t.Error("expected not paused initially")
	}

	ac.SetPaused(true)
	if !ac.Paused() {
		t.Error("expected paused after SetPaused(true)")
	}

	ac.SetPaused(false)
	if ac.Paused() {
		t.Error("expected not paused after SetPaused(false)")
	}
}

// --- GC-aware throttling tests ---

// newTestAdaptiveController creates an AdaptiveController with an injectable
// GC sample function for deterministic testing.
func newTestAdaptiveController(cfg AdaptiveConfig, gcFractionFn func() float64) *AdaptiveController {
	ac := NewAdaptiveController(cfg)
	if ac.gcMonitor != nil && gcFractionFn != nil {
		ac.gcMonitor.sampleFn = func(stats *debug.GCStats) {
			// Simulate GC pressure by advancing PauseTotal proportionally.
			// Reset prevSampleTime to a fixed offset so that wall time in
			// Sample() is ~1s regardless of scheduling jitter. Without this,
			// the time gap between sampleFn's time.Since() and Sample()'s
			// time.Now() can be 20x+ the actual elapsed time on macOS,
			// causing gcFrac to be wildly incorrect.
			ac.gcMonitor.prevSampleTime = time.Now().Add(-1 * time.Second)
			frac := gcFractionFn()
			elapsed := time.Since(ac.gcMonitor.prevSampleTime)
			gcPause := time.Duration(float64(elapsed) * frac)
			stats.PauseTotal = ac.gcMonitor.prevPauseTotal + gcPause
		}
	}
	return ac
}

func TestAdaptiveController_GCHighReducesRate(t *testing.T) {
	gcFraction := 0.20 // above 15% target
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	initialRate := ac.Rate()

	// Record latencies below target so only GC is the trigger.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	newRate := ac.Adjust()
	if newRate >= initialRate {
		t.Errorf("expected rate to decrease due to GC pressure: got %d, initial %d", newRate, initialRate)
	}

	// 25% reduction.
	expected := initialRate * 3 / 4
	if newRate != expected {
		t.Errorf("expected rate %d, got %d", expected, newRate)
	}
}

func TestAdaptiveController_GCCriticalPauses(t *testing.T) {
	gcFraction := 0.35 // above 30% critical threshold
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	// Record latencies below target so only GC triggers pause.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	ac.Adjust()

	if !ac.Paused() {
		t.Error("expected compaction to be paused when GC fraction > critical threshold")
	}

	reason := ac.PausedReason()
	if reason != "gc_cpu_critical" {
		t.Errorf("expected paused reason 'gc_cpu_critical', got %q", reason)
	}
}

func TestAdaptiveController_GCRecoversResume(t *testing.T) {
	gcFraction := 0.35 // start critical
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	// Record low latencies.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	// Trigger pause.
	ac.Adjust()
	if !ac.Paused() {
		t.Fatal("expected paused with high GC fraction")
	}

	// GC pressure drops below target threshold.
	gcFraction = 0.05
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}
	ac.Adjust()

	if ac.Paused() {
		t.Error("expected compaction to resume when GC fraction drops below target")
	}

	reason := ac.PausedReason()
	if reason != "" {
		t.Errorf("expected empty paused reason after resume, got %q", reason)
	}
}

func TestAdaptiveController_MixedSignals(t *testing.T) {
	// Latency OK but GC high → should still reduce rate.
	gcFraction := 0.20 // above 15% target
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	initialRate := ac.Rate()

	// Record latencies well below target — P99 signal says "increase".
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	newRate := ac.Adjust()

	// Despite low latency, GC pressure should reduce rate.
	if newRate >= initialRate {
		t.Errorf("expected rate to decrease due to GC pressure despite low latency: got %d, initial %d", newRate, initialRate)
	}

	// Should NOT be paused (GC is high but not critical).
	if ac.Paused() {
		t.Error("should not be paused when GC is high but not critical")
	}
}

func TestAdaptiveController_BothHealthyIncreasesRate(t *testing.T) {
	gcFraction := 0.03 // well below 7.5% (targetGC/2)
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	// First reduce the rate so there is room to increase.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(800 * time.Millisecond)
	}
	ac.Adjust()
	reducedRate := ac.Rate()

	// Now make both signals healthy: low latency + low GC.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	newRate := ac.Adjust()
	if newRate <= reducedRate {
		t.Errorf("expected rate to increase when both signals healthy: got %d, reduced %d", newRate, reducedRate)
	}

	// 10% increase.
	expected := reducedRate * 11 / 10
	if newRate != expected {
		t.Errorf("expected rate %d, got %d", expected, newRate)
	}
}

func TestAdaptiveController_GCHighPreventsIncrease(t *testing.T) {
	// Even with low latency, high GC should prevent rate increase.
	gcFraction := 0.10 // above targetGC/2 (7.5%) but below targetGC (15%)
	ac := newTestAdaptiveController(AdaptiveConfig{
		MaxRate:            200 << 20,
		MinRate:            10 << 20,
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	// Reduce rate first.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(800 * time.Millisecond)
	}
	ac.Adjust()
	reducedRate := ac.Rate()

	// Low latency but GC fraction is between targetGC/2 and targetGC.
	// Rate should stay stable (not increase, not decrease).
	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}

	newRate := ac.Adjust()
	if newRate != reducedRate {
		t.Errorf("expected rate to stay stable when GC > targetGC/2: got %d, reduced %d", newRate, reducedRate)
	}
}

func TestAdaptiveController_GCStatsReporting(t *testing.T) {
	gcFraction := 0.22
	ac := newTestAdaptiveController(AdaptiveConfig{
		TargetP99:          500 * time.Millisecond,
		WindowSize:         10,
		TargetGCFraction:   0.15,
		CriticalGCFraction: 0.30,
	}, func() float64 { return gcFraction })

	for i := 0; i < 10; i++ {
		ac.RecordLatency(100 * time.Millisecond)
	}
	ac.Adjust()

	frac, reason := ac.GCStats()
	if frac == 0 {
		t.Error("expected non-zero GC fraction after Adjust()")
	}
	// GC is high but not critical, so reason should be empty (not paused).
	if reason != "" {
		t.Errorf("expected empty reason (not paused), got %q", reason)
	}
}

func TestGCMonitor_Sample(t *testing.T) {
	m := NewGCMonitor()

	// Override with deterministic function.
	callCount := 0
	m.sampleFn = func(stats *debug.GCStats) {
		callCount++
		// Reset prevSampleTime so wall time in Sample() is ~1s.
		m.prevSampleTime = time.Now().Add(-1 * time.Second)
		// Simulate 10ms of GC pause added each call.
		stats.PauseTotal = m.prevPauseTotal + 10*time.Millisecond
	}

	frac := m.Sample()
	if callCount != 1 {
		t.Errorf("expected sampleFn called once, got %d", callCount)
	}

	// Fraction should be positive (10ms pause over some wall time).
	if frac <= 0 {
		t.Errorf("expected positive GC fraction, got %f", frac)
	}

	// Fraction() should return cached value without sampling.
	cached := m.Fraction()
	if cached != frac {
		t.Errorf("Fraction() = %f, want %f", cached, frac)
	}
	if callCount != 1 {
		t.Error("Fraction() should not trigger a new sample")
	}
}

func TestAdaptiveController_DisableGCMonitor(t *testing.T) {
	disabled := false
	ac := NewAdaptiveController(AdaptiveConfig{
		EnableGCMonitor: &disabled,
		WindowSize:      10,
	})

	if ac.gcMonitor != nil {
		t.Error("expected gcMonitor to be nil when disabled")
	}

	// Should still work with only P99 signal.
	for i := 0; i < 10; i++ {
		ac.RecordLatency(800 * time.Millisecond)
	}

	initialRate := ac.Rate()
	newRate := ac.Adjust()
	if newRate >= initialRate {
		t.Errorf("expected rate decrease from P99 signal alone: got %d, initial %d", newRate, initialRate)
	}
}
