package compaction

import (
	"testing"
	"time"
)

func TestAdaptiveController_ReducesRateOnHighLatency(t *testing.T) {
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:    200 << 20,
		MinRate:    10 << 20,
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
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
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:    200 << 20,
		MinRate:    10 << 20,
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
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
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:    200 << 20,
		MinRate:    10 << 20,
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
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
	ac := NewAdaptiveController(AdaptiveConfig{
		MaxRate:    maxRate,
		MinRate:    10 << 20,
		TargetP99:  500 * time.Millisecond,
		WindowSize: 10,
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
