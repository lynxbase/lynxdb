package mem

import "testing"

func TestArenaSlicesAreZeroedAndIndependent(t *testing.T) {
	a := NewArena()
	defer a.Free()

	for round := 0; round < 3; round++ {
		ints := Slice[int64](a, 1000, 1000)
		bytes := Slice[byte](a, 0, 4096)
		for i, v := range ints {
			if v != 0 {
				t.Fatalf("round %d: ints[%d] = %d, want 0", round, i, v)
			}
			ints[i] = int64(i) + 1
		}
		bytes = append(bytes, "payload"...)
		if string(bytes) != "payload" {
			t.Fatalf("bytes = %q", bytes)
		}
		for i, v := range ints {
			if v != int64(i)+1 {
				t.Fatalf("round %d: ints[%d] overwritten: %d", round, i, v)
			}
		}
		a.Reset()
	}
}

func TestArenaLargeAllocation(t *testing.T) {
	a := NewArena()
	defer a.Free()

	big := Slice[uint32](a, 3<<20, 3<<20)
	big[len(big)-1] = 7
	small := Slice[uint16](a, 8, 8)
	small[0] = 9
	if big[len(big)-1] != 7 || small[0] != 9 {
		t.Fatal("allocations overlap")
	}
	if a.Size() < 12<<20 {
		t.Fatalf("Size() = %d", a.Size())
	}
}

func TestGrowKeepsContents(t *testing.T) {
	a := NewArena()
	defer a.Free()

	var s []uint32
	for i := 0; i < 10000; i++ {
		s = Grow(a, s, 1)
		s = append(s, uint32(i))
	}
	for i, v := range s {
		if v != uint32(i) {
			t.Fatalf("s[%d] = %d", i, v)
		}
	}
	if got := a.CopyBytes([]byte("abc")); string(got) != "abc" {
		t.Fatalf("CopyBytes = %q", got)
	}
}

func TestPoolResetControlsRetention(t *testing.T) {
	type buf struct{ b []byte }
	p := Pool[buf]{Reset: func(v *buf) bool {
		v.b = v.b[:0]

		return cap(v.b) <= 1024
	}}

	v := p.Get()
	v.b = append(v.b, "x"...)
	p.Put(v)
	if got := p.Get(); len(got.b) != 0 {
		t.Fatalf("pooled value not reset: %q", got.b)
	}
	p.Put(&buf{b: make([]byte, 0, 4096)})
	p.Put(nil)
}

func BenchmarkArenaSlice(b *testing.B) {
	a := NewArena()
	defer a.Free()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Slice[int64](a, 512, 512)
		if i%1024 == 1023 {
			a.Reset()
		}
	}
}
