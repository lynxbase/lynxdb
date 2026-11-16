package vec

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

func TestKeyPartitionsKeepRecordsInOrderPerPartition(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	const bits = 4
	p := NewKeyPartitions(a, bits)
	if p.Partitions() != 1<<bits {
		t.Fatalf("Partitions = %d", p.Partitions())
	}

	type record struct {
		h   uint64
		rec []byte
	}
	want := make([][]record, p.Partitions())
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 20000; i++ {
		size := r.Intn(300)
		switch i {
		case 0, 7000:
			size = 0
		case 5000:
			// Larger than any chunk the log allocates by itself.
			size = 3 * maxKeyChunk
		}
		rec := make([]byte, size)
		r.Read(rec)
		h := r.Uint64()
		p.Add(h, rec)
		want[h>>(64-bits)] = append(want[h>>(64-bits)], record{h, rec})
	}
	for i := range want {
		if p.Len(i) != len(want[i]) {
			t.Fatalf("partition %d: Len = %d, want %d", i, p.Len(i), len(want[i]))
		}
		n := 0
		p.Each(i, func(h uint64, rec []byte) {
			if n < len(want[i]) && (h != want[i][n].h || !bytes.Equal(rec, want[i][n].rec)) {
				t.Fatalf("partition %d record %d differs", i, n)
			}
			n++
		})
		if n != len(want[i]) {
			t.Fatalf("partition %d: visited %d records, want %d", i, n, len(want[i]))
		}
	}
}

func TestKeyPartitionsWithOnePartition(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	p := NewKeyPartitions(a, 0)
	for _, h := range []uint64{0, 1, 1 << 63, ^uint64(0)} {
		p.Add(h, []byte("x"))
	}
	if p.Partitions() != 1 || p.Len(0) != 4 {
		t.Fatalf("%d partitions, %d records", p.Partitions(), p.Len(0))
	}
}

func BenchmarkKeyPartitionsAdd(b *testing.B) {
	a := mem.NewArena()
	defer a.Free()
	p := NewKeyPartitions(a, 8)
	rec := make([]byte, 40)
	b.ReportAllocs()
	b.SetBytes(int64(len(rec)))
	h := uint64(0x9E3779B97F4A7C15)
	for i := 0; i < b.N; i++ {
		h = h*0x9E3779B97F4A7C15 + 1
		p.Add(h, rec)
		if i&(1<<20-1) == 1<<20-1 {
			a.Reset()
			p = NewKeyPartitions(a, 8)
		}
	}
}
