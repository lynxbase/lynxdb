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
		case 300, 9000:
			// Long enough for a length of more than one byte.
			size = 20000
		case 5000:
			// Larger than any chunk the log allocates by itself.
			size = 3 * maxKeyChunk
		}
		rec := make([]byte, size)
		r.Read(rec)
		h := r.Uint64()
		p.Add(h, rec)
		part := PartitionOf(h, bits)
		want[part] = append(want[part], record{h, rec})
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

func TestKeyPartitionsRecordsOfKeysAndPairs(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	p := NewKeyPartitions(a, 0)
	long := bytes.Repeat([]byte("k"), 300)
	p.AddKey(7, 5, true, []byte("key"), []int64{-1, 1 << 40}, []byte("tail"))
	p.AddKey(8, 0, false, long, nil, nil)
	p.AddPair(9, 0x1122334455667788, 0xAABBCCDD)
	want := [][]byte{
		append(append([]byte{5}, "key"...), 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0, 0, 1, 0, 0, 't', 'a', 'i', 'l'),
		long,
		{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0xDD, 0xCC, 0xBB, 0xAA},
	}
	n, size := 0, 0
	p.Chunks(0, func(chunk []byte) {
		size += len(chunk)
		for at := 0; at < len(chunk); n++ {
			h, start, end := RecordAt(chunk, at)
			if h != uint64(7+n) || !bytes.Equal(chunk[start:end], want[n]) {
				t.Fatalf("record %d has hash %d and payload %x, want %x", n, h, chunk[start:end], want[n])
			}
			if n == 2 && end-at != PairSize {
				t.Fatalf("a pair takes %d bytes, want %d", end-at, PairSize)
			}
			at = end
		}
	})
	if n != len(want) || p.Size() != size {
		t.Fatalf("read %d records of %d bytes, the log says %d bytes", n, size, p.Size())
	}
}

func TestKeyPartitionsDrainedChunksServeAgain(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	const bits = 3
	p := NewKeyPartitions(a, bits)
	rec := make([]byte, 100)
	fill := func(round int) {
		for i := 0; i < 20000; i++ {
			rec[0], rec[1] = byte(i), byte(round)
			p.Add(uint64(i)<<32, rec)
		}
	}
	held := 0
	for round := 0; round < 4; round++ {
		fill(round)
		if round == 1 {
			held = a.Size()
		}
		for part := 0; part < 1<<bits; part++ {
			n := 0
			p.Drain(part, func(chunk []byte) {
				for at := 0; at < len(chunk); n++ {
					h, start, end := RecordAt(chunk, at)
					if PartitionOf(h, bits) != part || chunk[start] != byte(h>>32) || chunk[start+1] != byte(round) {
						t.Fatalf("round %d: partition %d holds a record of hash %x that starts %x", round, part, h, chunk[start:start+2])
					}
					at = end
				}
			})
			if n != 20000>>bits || p.Len(part) != 0 {
				t.Fatalf("round %d: partition %d gave %d records and holds %d", round, part, n, p.Len(part))
			}
		}
		if p.Size() != 0 {
			t.Fatalf("round %d: a drained log holds %d bytes", round, p.Size())
		}
	}
	// A round takes what the one before left, in chunks of other sizes at
	// first.
	if a.Size() > held+held/4 {
		t.Fatalf("the arena grew from %d to %d bytes over rounds of the same records", held, a.Size())
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
