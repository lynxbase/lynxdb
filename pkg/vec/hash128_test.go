package vec

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

func TestHash128MatchesMurmur3Vectors(t *testing.T) {
	for _, tc := range []struct {
		in     string
		lo, hi uint64
	}{
		{"", 0, 0},
		{"hello", 0xcbd8a7b341bd9b02, 0x5b1e906a48ae1d19},
		{"hello, world", 0x342fac623a5ebc8e, 0x4cdcbc079642414d},
		{"19 Jan 2038 at 3:14:07 AM", 0xb89e5988b737affc, 0x664fc2950231b2cb},
		{"The quick brown fox jumps over the lazy dog.", 0xcd99481f9ee902c9, 0x695da1a38987b6e7},
	} {
		if lo, hi := Hash128([]byte(tc.in)); lo != tc.lo || hi != tc.hi {
			t.Errorf("Hash128(%q) = %016x %016x, want %016x %016x", tc.in, lo, hi, tc.lo, tc.hi)
		}
	}
}

func TestHash128DependsOnEveryByteAndTheLength(t *testing.T) {
	seen := map[[2]uint64]string{}
	add := func(key []byte) {
		t.Helper()
		lo, hi := Hash128(key)
		if prev, ok := seen[[2]uint64{lo, hi}]; ok {
			t.Fatalf("%q and %q hash alike", prev, key)
		}
		seen[[2]uint64{lo, hi}] = string(key)
	}
	for n := 0; n <= 48; n++ {
		key := make([]byte, n)
		add(key)
		for i := range key {
			key[i] = 1
			add(key)
			key[i] = 0
		}
	}
}

func TestHashSet128CountsDistinctValues(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	s := NewHashSet128(a)
	for round := 0; round < 2; round++ {
		r := rand.New(rand.NewSource(9))
		want := map[[2]uint64]bool{}
		for i := 0; i < 200000; i++ {
			// A small range makes repeats and equal low words common.
			v := [2]uint64{uint64(r.Intn(3000)), uint64(r.Intn(40))}
			if added := s.Add(v[0], v[1]); added == want[v] {
				t.Fatalf("round %d: Add(%v) = %v", round, v, added)
			}
			want[v] = true
		}
		if !want[[2]uint64{}] && !s.Add(0, 0) {
			t.Fatal("the zero value was reported as present")
		}
		want[[2]uint64{}] = true
		if s.Add(0, 0) || s.Len() != len(want) {
			t.Fatalf("round %d: Len = %d, want %d", round, s.Len(), len(want))
		}
		s.Clear()
		if s.Len() != 0 {
			t.Fatalf("Len after Clear = %d", s.Len())
		}
	}
}

func TestRecentHashesDropOnlyValuesSeenBefore(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	w := NewRecentHashes(a, 6)
	r := rand.New(rand.NewSource(12))
	seen := map[[2]uint64]bool{}
	repeats, dropped := 0, 0
	for i := 0; i < 20000; i++ {
		v := [2]uint64{uint64(r.Intn(300)), uint64(r.Intn(3))}
		got := w.Seen(v[0], v[1])
		if got && !seen[v] {
			t.Fatalf("Seen(%v) reported a value that never came", v)
		}
		if seen[v] {
			repeats++
		}
		if got {
			dropped++
		}
		seen[v] = true
	}
	for i := 0; i < 2; i++ {
		if w.Seen(0, 0) {
			t.Fatal("the zero value was reported as seen")
		}
	}
	w.Seen(7, 7)
	if !w.Seen(7, 7) {
		t.Fatal("a value repeated at once was not reported")
	}
	if dropped == 0 || dropped > repeats {
		t.Fatalf("dropped %d of %d repeats", dropped, repeats)
	}
}

func BenchmarkHash128(b *testing.B) {
	key := []byte("did:plc:yj3sjq3blzpynh27cumnp5ks")
	b.SetBytes(int64(len(key)))
	var sum uint64
	for i := 0; i < b.N; i++ {
		lo, hi := Hash128(key)
		sum += lo ^ hi
	}
	_ = sum
}

func BenchmarkHashSet128Add(b *testing.B) {
	for _, distinct := range []int{1 << 12, 1 << 16, 1 << 22} {
		b.Run(fmt.Sprintf("distinct=%d", distinct), func(b *testing.B) {
			a := mem.NewArena()
			defer a.Free()
			s := NewHashSet128(a)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				x := Mix64(uint64(i%distinct) + 1)
				s.Add(x, x^0x5555)
			}
		})
	}
}
