package mem

import "testing"

func TestSizedHandsOutPowersOfTwoAndTakesThemBack(t *testing.T) {
	var p Sized[uint32]
	if s := p.Get(0); s != nil {
		t.Fatalf("Get(0) = %v, want nil", s)
	}
	for _, n := range []int{1, 2, 3, 4, 5, 1000, 1024, 1025} {
		s := p.Get(n)
		if len(s) != n || cap(s) < n || cap(s)&(cap(s)-1) != 0 || cap(s) >= 2*n && n > 1 {
			t.Fatalf("Get(%d) has length %d and capacity %d", n, len(s), cap(s))
		}
		for i := range s {
			s[i] = uint32(n)
		}
		p.Put(s)
	}
	// A slice of another capacity is not one of the pool's.
	p.Put(make([]uint32, 3))
	p.Put(nil)
}

func TestSizedGrowKeepsTheElements(t *testing.T) {
	var p Sized[int64]
	var s []int64
	for i := 0; i < 5000; i++ {
		s = append(p.Grow(s, 1), int64(i))
	}
	if len(s) != 5000 || cap(s) != 8192 {
		t.Fatalf("grew to length %d and capacity %d", len(s), cap(s))
	}
	for i, v := range s {
		if v != int64(i) {
			t.Fatalf("element %d is %d", i, v)
		}
	}
	if kept := p.Grow(s, 100); &kept[0] != &s[0] {
		t.Fatal("Grow moved a slice that had the room")
	}
}

func BenchmarkSizedGetPut(b *testing.B) {
	var p Sized[uint64]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Put(p.Get(1 << 12))
	}
}
