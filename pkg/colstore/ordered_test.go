package colstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// orderedTable builds a table of data ordered by key in blocks of blockRows:
// runs sorted runs, or, with merged set, one sorted run cut into that many
// parts.
func orderedTable(t testing.TB, data []byte, runs, blockRows int, key []string, merged bool) *Table {
	t.Helper()
	parts, _ := orderedRuns(t, data, 2, runs, blockRows, key)
	if merged {
		bufs := make([]bytes.Buffer, runs)
		n := mergeRanges(t, parts, runs, blockRows, key, func(i int) io.Writer { return &bufs[i] })
		parts = parts[:0]
		for i := 0; i < n; i++ {
			p, err := part.Open(bufs[i].Bytes())
			if err != nil {
				t.Fatal(err)
			}
			parts = append(parts, p)
		}
	}
	handles := make([]*Handle, len(parts))
	for i, p := range parts {
		handles[i] = NewHandle(p, nil)
	}
	tbl := NewTable()
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}

	return tbl
}

func TestRangeScanReadsMatchingRowsOnce(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var data bytes.Buffer
	var want []int64
	for i := 0; i < 6000; i++ {
		a := []string{`"a":"x",`, `"a":"x",`, `"a":"y",`, `"a":"w",`, `"a":7,`, ``}[rng.Intn(6)]
		b := fmt.Sprintf(`"b":"u%03d",`, rng.Intn(150))
		switch rng.Intn(12) {
		case 0:
			b = ""
		case 1:
			b = fmt.Sprintf(`"b":%d,`, rng.Intn(5))
		}
		fmt.Fprintf(&data, `{%s%s"n":%d,"time_us":%d}`+"\n", a, b, i, 1000+i)
		if a == `"a":"x",` {
			want = append(want, int64(i))
		}
	}
	key := []string{"a", "b"}
	filter := KeyFilter{Paths: key[:1], Values: [][]byte{[]byte("x")}}
	for _, merged := range []bool{true, false} {
		for _, runs := range []int{1, 5} {
			tbl := orderedTable(t, data.Bytes(), runs, 64, key, merged)
			snap := tbl.Snapshot()
			plan, ok := snap.PlanKeyRanges(filter, key[1:])
			if !ok {
				t.Fatal("ordered parts were not planned")
			}
			if streams := plan.Streams(); merged && streams != 1 || !merged && streams != runs {
				t.Fatalf("merged %v, %d runs: %d streams", merged, runs, streams)
			}
			if _, ok := snap.PlanKeyRanges(filter, []string{"n"}); ok {
				t.Fatal("planned a group the parts are not ordered by")
			}
			for _, n := range []int{1, 7, 200} {
				scan := plan.Ranges(n, ColumnID{Path: "n", Kind: vec.KindInt})
				var mu sync.Mutex
				var got []int64
				rangeOf := map[string]*KeyRange{}
				err := scan.Run(context.Background(), 4, func(_ int, kr *KeyRange, b *part.Block) error {
					var r RangeReader
					var rows []int64
					var values []string
					for s := range kr.Streams {
						r.Reset(scan, kr, s)
						last := ""
						for {
							ok, err := r.Next(b)
							if err != nil {
								return err
							}
							if !ok {
								break
							}
							ids, value := r.Block.Column(0), r.Block.Keys.Column(1, vec.KindString)
							for row := r.From; row < r.To; row++ {
								rows = append(rows, ids.Int(row))
								if value == nil || !value.IsValid(row) {
									continue
								}
								v := string(value.Bytes(row))
								if v < last {
									return fmt.Errorf("stream %d yields %q after %q", s, v, last)
								}
								last = v
								values = append(values, v)
							}
						}
					}
					mu.Lock()
					defer mu.Unlock()
					got = append(got, rows...)
					for _, v := range values {
						if other := rangeOf[v]; other != nil && other != kr {
							return fmt.Errorf("%q is in two ranges", v)
						}
						rangeOf[v] = kr
					}

					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("merged %v, %d runs, %d ranges: read %d rows, want %d", merged, runs, n, len(got), len(want))
				}
				// A block is decoded once, or once more when a range was cut
				// again after its neighbours had finished with the block.
				if decoded := scan.Decoded.Load(); decoded > 2*int64(plan.Blocks) || len(scan.open) != 0 {
					t.Fatalf("decoded %d of %d blocks, %d left open", decoded, plan.Blocks, len(scan.open))
				}
			}
			snap.Release()
			tbl.Close()
		}
	}
}

func TestRangeReaderTellsSharedKeyColumns(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	var data bytes.Buffer
	for i := 0; i < 5000; i++ {
		b := fmt.Sprintf(`"b":"u%02d",`, rng.Intn(40))
		switch rng.Intn(15) {
		case 0:
			b = ""
		case 1:
			b = fmt.Sprintf(`"b":%d,`, rng.Intn(3))
		}
		fmt.Fprintf(&data, `{"a":"x",%s"c":%d,"time_us":%d}`+"\n", b, rng.Intn(4), 1000+i)
	}
	key := []string{"a", "b", "c"}
	tbl := orderedTable(t, data.Bytes(), 6, 32, key, true)
	defer tbl.Close()
	snap := tbl.Snapshot()
	defer snap.Release()
	plan, ok := snap.PlanKeyRanges(KeyFilter{Paths: key[:1], Values: [][]byte{[]byte("x")}}, key[1:2])
	if !ok || plan.Streams() != 1 {
		t.Fatal("the merged parts are not one stream")
	}
	scan := plan.Ranges(1)
	blocks, continued := 0, 0
	err := scan.Run(context.Background(), 1, func(_ int, kr *KeyRange, b *part.Block) error {
		var r RangeReader
		r.Reset(scan, kr, 0)
		var last, first []byte
		for {
			p, i, whole, ok := r.Peek()
			if !ok {
				return nil
			}
			shared := r.Shared()
			if ok, err := r.Read(b); err != nil || !ok {
				return err
			}
			if whole && (r.From != 0 || r.To != r.Block.Rows) {
				return fmt.Errorf("block %d is not whole: rows %d to %d of %d", i, r.From, r.To, r.Block.Rows)
			}
			first = r.Block.Keys.AppendKey(first[:0], 0)
			if want := part.SharedKeyColumns(last, first); blocks > 0 && shared != want {
				return fmt.Errorf("block %d of %d shares %d key columns with the row before it, reader says %d", i, p.NumBlocks(), want, shared)
			}
			if shared >= 2 {
				continued++
			}
			blocks++
			last = r.Block.Keys.AppendKey(last[:0], r.Block.Rows-1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if blocks < 100 || continued < 20 {
		t.Fatalf("%d blocks, %d of them continue a value", blocks, continued)
	}
}

func TestPlanKeyRangesNeedsOrderedParts(t *testing.T) {
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{
		NewHandle(orderedByTag(t, 500, 100, []string{"alpha", "beta"}), nil),
		NewHandle(makePart(t, 0, 300, 100, "beta"), nil),
	}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()
	if _, ok := snap.PlanKeyRanges(KeyFilter{}, []string{"tag"}); ok {
		t.Fatal("planned key ranges over a part in arrival order")
	}
}
