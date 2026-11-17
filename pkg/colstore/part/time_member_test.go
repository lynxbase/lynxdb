package part

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// TestTimeMemberTakesOneByte requires that the member the row times come
// from is stored as a reference to them, in every unit a time is given in,
// and reads back as it was written.
func TestTimeMemberTakesOneByte(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	for _, unit := range []struct {
		name string
		at   int64
		step int64
	}{{"seconds", 1_700_000_000, 3}, {"milliseconds", 1_700_000_000_000, 700}, {"microseconds", 1_700_000_000_000_000, 90_000}, {"nanoseconds", 1_700_000_000_000_000_000, 5_000_000}} {
		t.Run(unit.name, func(t *testing.T) {
			const rows = 3000
			batch := shred.NewBatch(shred.Options{TimeField: "ts"})
			want := make([]int64, rows)
			for i := range want {
				want[i] = unit.at + r.Int63n(unit.step*rows)
				doc := fmt.Sprintf(`{"ts":%d,"other":%d,"half":%d}`, want[i], want[i]+1, want[i]/2)
				if err := batch.AddJSON([]byte(doc), 0); err != nil {
					t.Fatal(err)
				}
			}
			var buf bytes.Buffer
			w := NewWriter(&buf, Options{})
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			p, err := Open(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			var b Block
			if err := p.OpenBlock(0, &b); err != nil {
				t.Fatal(err)
			}
			var col vec.Column
			for path, size := range map[string]int{"ts": 1, "other": 0, "half": 0} {
				id, _ := p.PathID(path)
				ref, ok := b.Column(id, vec.KindInt)
				if !ok {
					t.Fatalf("no column %s", path)
				}
				if size != 0 && ref.Size() != size {
					t.Errorf("%s takes %d bytes, want %d", path, ref.Size(), size)
				}
				if size == 0 && ref.Size() < rows {
					t.Errorf("%s takes %d bytes: it is not the time of its rows", path, ref.Size())
				}
				if err := b.Decode(ref, &col); err != nil {
					t.Fatal(err)
				}
				for i, v := range want {
					switch path {
					case "other":
						v++
					case "half":
						v /= 2
					}
					if got := col.Int(i); got != v {
						t.Fatalf("%s of row %d is %d, want %d", path, i, got, v)
					}
				}
				if lo, hi := vec.MinMaxInt64(col.Ints); path == "ts" && (ref.MinInt != lo || ref.MaxInt != hi) {
					t.Errorf("ts is bounded by %d and %d, its values by %d and %d", ref.MinInt, ref.MaxInt, lo, hi)
				}
			}
		})
	}
}

// TestTimeMemberOfSomeRowsIsStoredAsItIs requires the plain encodings for a
// time member that a row lacks and for one whose rows hold one time.
func TestTimeMemberOfSomeRowsIsStoredAsItIs(t *testing.T) {
	for name, docs := range map[string][]string{
		"a row without it": {`{"ts":1700000000000001,"n":1}`, `{"n":2}`, `{"ts":1700000000000003,"n":3}`},
		"one time":         {`{"ts":1700000000000001,"n":1}`, `{"ts":1700000000000001,"n":2}`},
	} {
		batch := shred.NewBatch(shred.Options{TimeField: "ts"})
		for _, d := range docs {
			if err := batch.AddJSON([]byte(d), 1_700_000_000_000_002_000); err != nil {
				t.Fatal(err)
			}
		}
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{})
		if err := w.WriteBatch(batch); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		var b Block
		if err := p.OpenBlock(0, &b); err != nil {
			t.Fatal(err)
		}
		id, _ := p.PathID("ts")
		ref, ok := b.Column(id, vec.KindInt)
		if !ok || ref.enc == encTime {
			t.Fatalf("%s: the member is stored as the row times", name)
		}
		var col vec.Column
		if err := b.Decode(ref, &col); err != nil {
			t.Fatal(err)
		}
		for i, d := range docs {
			if has := bytes.Contains([]byte(d), []byte(`"ts"`)); col.IsValid(i) != has || (has && col.Int(i) != 1700000000000001 && col.Int(i) != 1700000000000003) {
				t.Fatalf("%s: row %d reads %v %d", name, i, col.IsValid(i), col.Int(i))
			}
		}
	}
}
