package vec

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

// batchKeys lays keys out as the values of a flat column.
func batchKeys(keys [][]byte) (data []byte, offsets []uint32) {
	offsets = append(offsets, 0)
	for _, k := range keys {
		data = append(data, k...)
		offsets = append(offsets, uint32(len(data)))
	}

	return data, offsets
}

func TestByteTableIDsAgreeWithID(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	batched, single := NewByteTable(a), NewByteTable(a)
	defer batched.Release()
	defer single.Release()
	r := rand.New(rand.NewSource(7))
	long := bytes.Repeat([]byte("long key "), 20000)
	for round := 0; round < 40; round++ {
		keys := make([][]byte, 1+r.Intn(3000))
		for i := range keys {
			switch r.Intn(50) {
			case 0:
				keys[i] = nil
			case 1:
				keys[i] = long[:100+r.Intn(len(long)-100)]
			case 2:
				keys[i] = bytes.Repeat([]byte{'x'}, 0x7e+r.Intn(4))
			default:
				keys[i] = []byte(fmt.Sprintf("did:plc:%d", r.Intn(20000)))
			}
		}
		data, offsets := batchKeys(keys)
		// Half of the rounds look up a selection of the rows, in any order.
		var rows []uint32
		picked := keys
		if round%2 == 1 {
			picked = nil
			for i := 0; i < len(keys); i += 1 + r.Intn(3) {
				rows = append(rows, uint32(i))
			}
			r.Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
			for _, row := range rows {
				picked = append(picked, keys[row])
			}
		}
		tagged := round%3 == 0
		hashes := make([]uint64, len(picked))
		want := make([]uint32, len(picked))
		for i, k := range picked {
			full := k
			if tagged {
				full = append([]byte{5}, k...)
			}
			hashes[i] = Hash(full)
			want[i] = uint32(single.IDHashed(full, hashes[i]))
		}
		got := make([]uint32, len(picked))
		batched.IDs(got, hashes, &Keys{Data: data, Offsets: offsets, Rows: rows, Tag: 5, Tagged: tagged}, nil, 0)
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("round %d: key %d has id %d, want %d", round, i, got[i], want[i])
			}
		}
		if round == 25 {
			batched.Clear()
			single.Clear()
		}
	}
	if batched.Len() != single.Len() {
		t.Fatalf("Len = %d, want %d", batched.Len(), single.Len())
	}
	for id := 0; id < single.Len(); id++ {
		if !bytes.Equal(batched.Key(id), single.Key(id)) || batched.KeyHash(id) != single.KeyHash(id) {
			t.Fatalf("key %d is %q, want %q", id, batched.Key(id), single.Key(id))
		}
	}
}

func TestByteTablePartitionAndIDsOf(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	src, dst := NewByteTable(a), NewByteTable(a)
	defer src.Release()
	defer dst.Release()
	const n = 30000
	for i := 0; i < n; i++ {
		src.ID([]byte(fmt.Sprintf("key-%d", i*31)))
	}
	// The other table holds every third key already, under other ids.
	for i := n - 1; i >= 0; i -= 3 {
		dst.ID([]byte(fmt.Sprintf("key-%d", i*31)))
	}
	const bits = 5
	order, starts := src.Partition(make([]uint32, n), make([]uint32, 1<<bits+1), bits)
	seen := make([]bool, n)
	for p := 0; p < 1<<bits; p++ {
		ids := order[starts[p]:starts[p+1]]
		if len(ids) < n>>bits/2 {
			t.Fatalf("partition %d holds %d of %d keys", p, len(ids), n)
		}
		for i, id := range ids {
			if seen[id] || (i > 0 && ids[i-1] >= id) {
				t.Fatalf("partition %d lists id %d twice or out of order", p, id)
			}
			seen[id] = true
			if got := src.KeyHash(int(id)) >> 32 & (1<<bits - 1); got != uint64(p) {
				t.Fatalf("id %d of partition %d has the bits of %d", id, p, got)
			}
		}
		got := make([]uint32, len(ids))
		dst.IDsOf(got, src, ids, nil, 0)
		for i, id := range ids {
			if !bytes.Equal(dst.Key(int(got[i])), src.Key(int(id))) {
				t.Fatalf("id %d went to %q, want %q", id, dst.Key(int(got[i])), src.Key(int(id)))
			}
		}
	}
	if starts[1<<bits] != n || dst.Len() != n {
		t.Fatalf("%d ids listed, %d keys merged, want %d", starts[1<<bits], dst.Len(), n)
	}
}

func TestByteTableKeepsItsPagesThroughClear(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	tbl := NewByteTable(a)
	defer tbl.Release()
	long := bytes.Repeat([]byte{'k'}, 3*tablePage)
	fill := func() {
		for i := 0; i < 5000; i++ {
			tbl.ID([]byte(fmt.Sprintf("a key of some length %d", i)))
			if i%1000 == 0 {
				long[0] = byte(i / 1000)
				if id := tbl.ID(long); !bytes.Equal(tbl.Key(id), long) {
					t.Fatalf("long key %d reads back wrong", i)
				}
			}
		}
	}
	fill()
	size := a.Size()
	for round := 0; round < 5; round++ {
		tbl.Clear()
		fill()
	}
	if a.Size() != size {
		t.Fatalf("the arena grew from %d to %d bytes over rounds of the same keys", size, a.Size())
	}
}

// tableOf returns a table of distinct keys shaped like account ids, and
// lookups keys drawn from them, laid out as the values of a flat column.
func tableOf(a *mem.Arena, distinct, lookups int) (tbl *ByteTable, data []byte, offsets []uint32) {
	tbl = NewByteTable(a)
	for i := 0; i < distinct; i++ {
		tbl.ID([]byte(fmt.Sprintf("did:plc:%024d", i)))
	}
	r := rand.New(rand.NewSource(1))
	offsets = append(offsets, 0)
	for i := 0; i < lookups; i++ {
		data = fmt.Appendf(data, "did:plc:%024d", r.Intn(distinct))
		offsets = append(offsets, uint32(len(data)))
	}

	return tbl, data, offsets
}

// BenchmarkByteTableLookups times a lookup of keys that a table holds, one
// by one and by batch, in tables that fit the cache and in one that does
// not. The keys are a million draws, so that those of a large table are
// not the ones just looked up.
func BenchmarkByteTableLookups(b *testing.B) {
	const lookups, batch = 1 << 20, 16384
	for _, distinct := range []int{1 << 12, 1 << 16, 1 << 21} {
		a := mem.NewArena()
		tbl, data, offsets := tableOf(a, distinct, lookups)
		b.Run(fmt.Sprintf("distinct=%d/one by one", distinct), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				j := i & (lookups - 1)
				tbl.ID(data[offsets[j]:offsets[j+1]])
			}
		})
		b.Run(fmt.Sprintf("distinct=%d/by batch", distinct), func(b *testing.B) {
			hashes := make([]uint64, batch)
			ids := make([]uint32, batch)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i += batch {
				from := i & (lookups - 1)
				keys := Keys{Data: data, Offsets: offsets[from : from+batch+1]}
				for j := range hashes {
					hashes[j] = Hash(keys.at(j))
				}
				tbl.IDs(ids, hashes, &keys, nil, 0)
			}
		})
		tbl.Release()
		a.Free()
	}
}

func TestByteTableAssignsStableDenseIDs(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	tbl := NewByteTable(a)

	const n = 50000
	for round := 0; round < 2; round++ {
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("did:plc:%07d", i))
			if id := tbl.ID(key); id != i {
				t.Fatalf("round %d: ID(%s) = %d, want %d", round, key, id, i)
			}
		}
	}
	if tbl.Len() != n {
		t.Fatalf("Len = %d", tbl.Len())
	}
	for _, i := range []int{0, 1, 255, 256, n - 1} {
		want := fmt.Sprintf("did:plc:%07d", i)
		if string(tbl.Key(i)) != want || tbl.KeyHash(i) != Hash([]byte(want)) {
			t.Fatalf("Key(%d) = %q", i, tbl.Key(i))
		}
	}
	if tbl.ID(nil) != n || tbl.ID([]byte{}) != n || tbl.Len() != n+1 {
		t.Fatal("empty key is not a single distinct key")
	}
}

func TestByteTableClearStartsOver(t *testing.T) {
	a := mem.NewArena()
	defer a.Free()
	tbl := NewByteTable(a)
	for round := 0; round < 3; round++ {
		for i := 0; i < 3000; i++ {
			key := []byte(fmt.Sprintf("round%d-%d", round, i))
			if id := tbl.ID(key); id != i {
				t.Fatalf("round %d: ID(%s) = %d, want %d", round, key, id, i)
			}
		}
		if tbl.Len() != 3000 || string(tbl.Key(2999)) != fmt.Sprintf("round%d-2999", round) {
			t.Fatalf("round %d: Len = %d, last key %q", round, tbl.Len(), tbl.Key(2999))
		}
		tbl.Clear()
		if tbl.Len() != 0 {
			t.Fatalf("Len after Clear = %d", tbl.Len())
		}
	}
}

func BenchmarkByteTableID(b *testing.B) {
	keys := make([][]byte, 1<<16)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("did:plc:%024d", i*7919))
	}
	a := mem.NewArena()
	defer a.Free()
	tbl := NewByteTable(a)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.ID(keys[i&(len(keys)-1)])
	}
}
