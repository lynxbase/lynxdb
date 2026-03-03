package stats

import (
	"errors"
	"sync"
	"testing"
)

func TestRootMonitor_BasicReserveRelease(t *testing.T) {
	rm := NewRootMonitor("test-pool", 1024)

	if err := rm.Reserve(256); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rm.CurAllocated() != 256 {
		t.Fatalf("expected cur=256, got %d", rm.CurAllocated())
	}
	if rm.MaxAllocated() != 256 {
		t.Fatalf("expected max=256, got %d", rm.MaxAllocated())
	}

	rm.Release(100)
	if rm.CurAllocated() != 156 {
		t.Fatalf("expected cur=156, got %d", rm.CurAllocated())
	}
	if rm.MaxAllocated() != 256 {
		t.Fatalf("max should still be 256, got %d", rm.MaxAllocated())
	}

	rm.Release(156)
	if rm.CurAllocated() != 0 {
		t.Fatalf("expected cur=0, got %d", rm.CurAllocated())
	}
}

func TestRootMonitor_PoolExhaustion(t *testing.T) {
	rm := NewRootMonitor("test-pool", 500)

	if err := rm.Reserve(300); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := rm.Reserve(200); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Pool at limit (500). Next reservation should fail.
	err := rm.Reserve(1)
	if err == nil {
		t.Fatal("expected PoolExhaustedError, got nil")
	}

	var poolErr *PoolExhaustedError
	if !errors.As(err, &poolErr) {
		t.Fatalf("expected *PoolExhaustedError, got %T: %v", err, err)
	}
	if poolErr.Pool != "test-pool" {
		t.Fatalf("expected pool=test-pool, got %s", poolErr.Pool)
	}
	if poolErr.Requested != 1 {
		t.Fatalf("expected requested=1, got %d", poolErr.Requested)
	}
	if poolErr.Current != 500 {
		t.Fatalf("expected current=500, got %d", poolErr.Current)
	}
	if poolErr.Limit != 500 {
		t.Fatalf("expected limit=500, got %d", poolErr.Limit)
	}

	// Current should be unchanged after failed reservation.
	if rm.CurAllocated() != 500 {
		t.Fatalf("expected cur=500 after failed reserve, got %d", rm.CurAllocated())
	}
}

func TestRootMonitor_HighWaterMark(t *testing.T) {
	rm := NewRootMonitor("pool", 0) // unlimited

	if err := rm.Reserve(1000); err != nil {
		t.Fatal(err)
	}
	rm.Release(800) // cur=200, max=1000
	if err := rm.Reserve(500); err != nil {
		t.Fatal(err)
	}
	// cur=700, max=1000

	if rm.MaxAllocated() != 1000 {
		t.Fatalf("expected max=1000, got %d", rm.MaxAllocated())
	}

	if err := rm.Reserve(500); err != nil {
		t.Fatal(err)
	}
	// cur=1200, max=1200
	if rm.MaxAllocated() != 1200 {
		t.Fatalf("expected max=1200, got %d", rm.MaxAllocated())
	}
}

func TestRootMonitor_Unlimited(t *testing.T) {
	rm := NewRootMonitor("unlimited", 0)

	// Should never fail with limit=0.
	if err := rm.Reserve(1 << 40); err != nil {
		t.Fatalf("unlimited pool should not fail: %v", err)
	}
	if rm.CurAllocated() != 1<<40 {
		t.Fatalf("expected cur=%d, got %d", int64(1<<40), rm.CurAllocated())
	}
}

func TestRootMonitor_ReleaseClampZero(t *testing.T) {
	rm := NewRootMonitor("pool", 0)

	if err := rm.Reserve(100); err != nil {
		t.Fatal(err)
	}
	// Release more than allocated — should clamp to 0.
	rm.Release(200)
	if rm.CurAllocated() != 0 {
		t.Fatalf("expected cur=0 after over-release, got %d", rm.CurAllocated())
	}
}

func TestRootMonitor_NilSafety(t *testing.T) {
	var rm *RootMonitor

	if err := rm.Reserve(100); err != nil {
		t.Fatalf("nil Reserve should return nil, got: %v", err)
	}
	rm.Release(100) // should not panic
	if rm.CurAllocated() != 0 {
		t.Fatalf("nil CurAllocated should return 0, got %d", rm.CurAllocated())
	}
	if rm.MaxAllocated() != 0 {
		t.Fatalf("nil MaxAllocated should return 0, got %d", rm.MaxAllocated())
	}
	if rm.Limit() != 0 {
		t.Fatalf("nil Limit should return 0, got %d", rm.Limit())
	}
}

func TestRootMonitor_Concurrent(t *testing.T) {
	rm := NewRootMonitor("concurrent", 0) // unlimited for this test

	const goroutines = 100
	const reservePerGoroutine int64 = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if err := rm.Reserve(reservePerGoroutine); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines) * reservePerGoroutine
	if rm.CurAllocated() != expected {
		t.Fatalf("expected cur=%d, got %d", expected, rm.CurAllocated())
	}

	// Release all concurrently.
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rm.Release(reservePerGoroutine)
		}()
	}
	wg.Wait()

	if rm.CurAllocated() != 0 {
		t.Fatalf("expected cur=0 after all releases, got %d", rm.CurAllocated())
	}
	if rm.MaxAllocated() != expected {
		t.Fatalf("expected max=%d, got %d", expected, rm.MaxAllocated())
	}
}

func TestIsPoolExhausted(t *testing.T) {
	err := &PoolExhaustedError{Pool: "p", Requested: 1, Current: 0, Limit: 0}
	if !IsPoolExhausted(err) {
		t.Fatal("expected true for PoolExhaustedError")
	}
	if IsPoolExhausted(errors.New("some other error")) {
		t.Fatal("expected false for non-PoolExhaustedError")
	}
	if IsPoolExhausted(nil) {
		t.Fatal("expected false for nil")
	}
}
