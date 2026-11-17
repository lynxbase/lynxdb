package mem

import (
	"os"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
)

const (
	defaultGCPercent = 100
	// minGCPercent keeps the collector from running back to back over a
	// heap much larger than the allowance.
	minGCPercent = 20
	// gcPercentStep is the change that is worth the pause of setting it.
	gcPercentStep = 5
)

// BoundGarbage holds the memory that awaits collection to about allowance
// bytes in a process whose live heap is larger than that: by default the
// collector lets the heap double, which for the buffers of a load is as
// much again as they hold. A smaller heap keeps the default. It does
// nothing when GOGC is set, applies to the whole process, and returns the
// function that ends it.
func BoundGarbage(allowance int64) (stop func()) {
	if os.Getenv("GOGC") != "" || allowance <= 0 {
		return func() {}
	}
	b := &garbageBound{allowance: uint64(allowance), sample: []metrics.Sample{{Name: "/gc/heap/live:bytes"}}}
	b.percent.Store(defaultGCPercent)
	b.arm()

	return func() {
		if b.stopped.CompareAndSwap(false, true) {
			debug.SetGCPercent(defaultGCPercent)
		}
	}
}

type garbageBound struct {
	allowance uint64
	sample    []metrics.Sample
	percent   atomic.Int64
	stopped   atomic.Bool
}

type collected struct{ b *garbageBound }

// arm has the bound adjusted after the next collection: the runtime
// finalizes an object that a collection found unreachable.
func (b *garbageBound) arm() {
	runtime.SetFinalizer(&collected{b}, func(c *collected) {
		if c.b.stopped.Load() {
			return
		}
		c.b.adjust()
		c.b.arm()
	})
}

func (b *garbageBound) adjust() {
	metrics.Read(b.sample)
	want := gcPercentFor(b.sample[0].Value.Uint64(), b.allowance)
	have := b.percent.Load()
	if want == have || (want != defaultGCPercent && want-have < gcPercentStep && have-want < gcPercentStep) {
		return
	}
	b.percent.Store(want)
	debug.SetGCPercent(int(want))
}

// gcPercentFor returns the growth of a live heap that amounts to allowance.
func gcPercentFor(live, allowance uint64) int64 {
	if live <= allowance {
		return defaultGCPercent
	}

	return max(int64(allowance*100/live), minGCPercent)
}
