package exec

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

func TestStateHeapKeepsStatesApart(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	rng := rand.New(rand.NewSource(1))
	refs := make([]stateRef, 300)
	want := make([][]byte, len(refs))
	for step := 0; step < 20000; step++ {
		i := rng.Intn(len(refs))
		switch rng.Intn(4) {
		case 0:
			h.release(refs[i])
			refs[i], want[i] = 0, nil
		case 1:
			v := bytes.Repeat([]byte{byte(step)}, rng.Intn(200))
			refs[i], want[i] = h.set(refs[i], v), v
		default:
			n := len(want[i]) + rng.Intn(40)
			ref, b := h.resize(refs[i], n)
			for j := len(want[i]); j < n; j++ {
				b[j] = byte(step)
				want[i] = append(want[i], byte(step))
			}
			refs[i] = ref
		}
		if got := h.get(refs[i]); !bytes.Equal(got, want[i]) {
			t.Fatalf("step %d: state %d holds %d bytes, want %d", step, i, len(got), len(want[i]))
		}
	}
	for i, ref := range refs {
		if got := h.get(ref); !bytes.Equal(got, want[i]) {
			t.Fatalf("state %d changed under the others", i)
		}
	}
}

func TestStateHeapReusesReleasedBlocks(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	var h stateHeap
	h.init(a)
	fill := func() {
		refs := make([]stateRef, 1000)
		for i := range refs {
			refs[i], _ = h.alloc(8 + i%500)
		}
		for i := range refs {
			refs[i], _ = h.resize(refs[i], 2000)
		}
		for _, ref := range refs {
			h.release(ref)
		}
	}
	fill()
	size := a.Size()
	for round := 0; round < 10; round++ {
		fill()
	}
	if a.Size() != size {
		t.Fatalf("heap grew from %d to %d bytes while holding the same states", size, a.Size())
	}
	h.reset()
	for round := 0; round < 10; round++ {
		for i := 0; i < 1000; i++ {
			h.alloc(2000)
		}
		h.reset()
	}
	if a.Size() != size {
		t.Fatalf("heap grew from %d to %d bytes across resets", size, a.Size())
	}
}
