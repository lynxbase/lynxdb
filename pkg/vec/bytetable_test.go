package vec

import (
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

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
