package mem

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestGCPercentForALiveHeap(t *testing.T) {
	const allowance = 512 << 20
	for live, want := range map[uint64]int64{
		0:          100,
		64 << 20:   100,
		512 << 20:  100,
		1024 << 20: 50,
		2048 << 20: 25,
		8192 << 20: 20,
	} {
		if got := gcPercentFor(live, allowance); got != want {
			t.Errorf("a live heap of %d MB: %d%%, want %d%%", live>>20, got, want)
		}
	}
}

// TestBoundGarbageFollowsTheLiveHeap requires the collector to start
// earlier while a large heap is live and at its default again without it.
func TestBoundGarbageFollowsTheLiveHeap(t *testing.T) {
	t.Setenv("GOGC", "")
	current := func() int {
		p := debug.SetGCPercent(defaultGCPercent)
		debug.SetGCPercent(p)

		return p
	}
	stop := BoundGarbage(8 << 20)
	defer stop()
	held := new([]byte)
	*held = make([]byte, 64<<20)
	settles := func(what string, ok func(int) bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !ok(current()) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: the collector runs at %d%%", what, current())
			}
			runtime.GC()
			time.Sleep(time.Millisecond)
		}
	}
	settles("with 64 MB live against 8 MB allowed", func(p int) bool { return p == minGCPercent })
	runtime.KeepAlive(held)
	*held = nil
	settles("with the heap released", func(p int) bool { return p == defaultGCPercent })
	stop()
	if p := current(); p != defaultGCPercent {
		t.Fatalf("after the bound ended the collector runs at %d%%", p)
	}
}
