package colstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// filterDocs returns count NDJSON documents numbered from first, of a
// service that only the writer has or of one that all writers share, with
// two texts of words of their own that are large enough in a full block for
// a token filter each, and small columns that share one.
func filterDocs(r *rand.Rand, writer, first, count int) []byte {
	var buf bytes.Buffer
	for i := first; i < first+count; i++ {
		svc := fmt.Sprintf("own%d", writer)
		if i%5 == 0 {
			svc = "shared"
		}
		fmt.Fprintf(&buf, `{"n":%d,"ts":%d,"svc":"%s","level":"l%d","text":"%016x %016x %016x","ref":"%016x %016x %016x","tags":["t%d","x%d"]`,
			i, i, svc, i%4, r.Uint64(), r.Uint64(), r.Uint64(), r.Uint64(), r.Uint64(), r.Uint64(), i%9, i%3)
		if i%50 == 3 {
			fmt.Fprintf(&buf, `,"note":"note %d of writer %d"`, i, writer)
		}
		buf.WriteString("}\n")
	}

	return buf.Bytes()
}

// blockPrint tells the blocks of a table apart by their size and the row
// numbers at their ends.
type blockPrint struct {
	rows        int
	first, last int64
}

// columnFilterAnswers requires the token filters of every block of snap to
// answer for each string and variant column as the column's values do: no
// token of a value is denied, and few absent ones pass. It returns the tier
// of the blocks by their print and how many columns have a filter of their
// own.
func columnFilterAnswers(t testing.TB, snap *Snapshot, r *rand.Rand) (blocks map[blockPrint]int, own int) {
	t.Helper()
	blocks = map[blockPrint]int{}
	var b part.Block
	var col, ns vec.Column
	absent, passed := 0, 0
	for _, h := range snap.Parts() {
		nID, _ := h.PathID("n")
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			if ref, ok := b.Column(nID, vec.KindInt); !ok {
				t.Fatal("no n column")
			} else if err := b.Decode(ref, &ns); err != nil {
				t.Fatal(err)
			}
			blocks[blockPrint{b.Rows, ns.Int(0), ns.Int(b.Rows - 1)}] = h.Tier

			listed := map[[2]uint32]bool{}
			b.FilterColumns(func(path uint32, kind vec.Kind) { listed[[2]uint32{path, uint32(kind)}] = true })
			own += len(listed)
			refs := b.Columns()
			held := make([]map[uint64]bool, len(refs))
			shared := map[uint64]bool{}
			for j := range refs {
				ref := &refs[j]
				if ref.Kind != vec.KindString && ref.Kind != vec.KindVariant {
					continue
				}
				if err := b.DecodeValues(ref, &col); err != nil {
					t.Fatal(err)
				}
				tokens := map[uint64]bool{}
				add := func(s []byte) {
					for _, hash := range tokenfilter.AppendHashes(nil, s) {
						tokens[hash] = true
					}
				}
				for v := 0; v < ref.Count; v++ {
					if ref.Kind == vec.KindVariant {
						variant.ForEachString(col.Bytes(v), add)
					} else {
						add(col.Bytes(v))
					}
				}
				held[j] = tokens
				if isOwn := listed[[2]uint32{ref.PathID, uint32(ref.Kind)}]; isOwn != h.OwnFilter(i, ref.PathID, ref.Kind) {
					t.Fatalf("run %d, block %d: column %s is listed with a filter of its own: %v", h.Run, i, h.Paths()[ref.PathID], isOwn)
				} else if !isOwn {
					for hash := range tokens {
						shared[hash] = true
					}
				}
			}
			for j := range refs {
				ref := &refs[j]
				if held[j] == nil {
					continue
				}
				in := held[j]
				if !h.OwnFilter(i, ref.PathID, ref.Kind) {
					in = shared
				}
				probe := func(hash uint64) {
					may, err := h.MayContainTokenIn(i, ref.PathID, ref.Kind, hash)
					if err != nil || (in[hash] && !may) {
						t.Fatalf("run %d, block %d: column %s holds token %x: %v, and its filter says %v, %v", h.Run, i, h.Paths()[ref.PathID], hash, in[hash], may, err)
					}
					if !in[hash] {
						if absent++; may {
							passed++
						}
					}
				}
				for hash := range held[j] {
					probe(hash)
				}
				// The tokens of the other columns are the likeliest to be asked for.
				for k := range refs {
					n := 0
					for hash := range held[k] {
						if n++; n > 50 {
							break
						}
						probe(hash)
					}
				}
				for n := 0; n < 50; n++ {
					probe(r.Uint64())
				}
			}
		}
	}
	if rate := float64(passed) / float64(max(absent, 1)); absent == 0 || rate > 0.02 {
		t.Fatalf("filters let %d of %d absent tokens pass", passed, absent)
	}

	return blocks, own
}

func TestStoreKeepsColumnFiltersThroughFlushesAndMerges(t *testing.T) {
	skipSyncs(t)
	path := filepath.Join(t.TempDir(), "store")
	// The runs stay as flushed until the store is opened again to merge.
	opts := StoreOptions{SortKey: []string{"svc"}, TimeField: "ts", BlockRows: 1024, FlushBytes: 1 << 20, MergeFanIn: -1, FlushInterval: -1}
	s, err := OpenStore(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	const writers, perWriter = 3, 24000
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			wr := s.NewWriter()
			for i := 0; i < perWriter; i += 500 {
				if err := wr.AddNDJSON(filterDocs(r, w, w*perWriter+i, 500)); err != nil {
					t.Error(err)
				}
			}
			if err := wr.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	r := rand.New(rand.NewSource(9))

	snap := s.Table().Snapshot()
	flushed, own := columnFilterAnswers(t, snap, r)
	runs := len(runShapes(t, snap))
	snap.Release()
	if runs < 6 || own < 2*runs {
		t.Fatalf("%d runs flushed with %d filters of single columns", runs, own)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	opts.MergeFanIn = 3
	if s, err = OpenStore(path, opts); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Settle(); err != nil {
		t.Fatal(err)
	}
	snap = s.Table().Snapshot()
	settled, own := columnFilterAnswers(t, snap, r)
	merged := len(runShapes(t, snap))
	snap.Release()
	// A merge copies the blocks that no other run reaches into and encodes
	// the others again.
	copied, encoded := 0, 0
	for print, tier := range settled {
		if _, kept := flushed[print]; tier == 0 {
			continue
		} else if kept {
			copied++
		} else {
			encoded++
		}
	}
	t.Logf("%d runs merged into %d: %d blocks kept, %d encoded again, %d filters of single columns", runs, merged, copied, encoded, own)
	if merged >= runs || copied < 5 || encoded < 5 || own < 20 {
		t.Fatalf("%d runs merged into %d: %d blocks kept, %d encoded again, %d filters of single columns", runs, merged, copied, encoded, own)
	}

	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	snap = s.Table().Snapshot()
	defer snap.Release()
	if _, own := columnFilterAnswers(t, snap, r); len(runShapes(t, snap)) != 1 || own < 20 {
		t.Fatalf("%d runs after compaction with %d filters of single columns", len(runShapes(t, snap)), own)
	}
}
