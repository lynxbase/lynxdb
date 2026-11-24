package colstore

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/internal/benchdata"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// sharedColumns returns, by path, the blocks of the store's parts that store
// a string column as codes of a part dictionary.
func sharedColumns(t *testing.T, s *Store) map[string]int {
	t.Helper()
	snap := s.Table().Snapshot()
	defer snap.Release()
	out := map[string]int{}
	var b part.Block
	for _, h := range snap.Parts() {
		for i := 0; i < h.NumBlocks(); i++ {
			if err := h.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			refs := b.Columns()
			for j := range refs {
				if refs[j].Kind == vec.KindString && refs[j].Shared() {
					out[h.Paths()[refs[j].PathID]]++
				}
			}
		}
	}

	return out
}

// TestStoreKeepsItsRowsWithPartDictionaries loads events whose accounts and
// posts repeat, in runs that merges of two tiers and a compaction rewrite,
// into a store that gives its parts dictionaries and into one that does
// not: both hold the same rows after every step, and the first stores the
// columns that repeat as codes.
func TestStoreKeepsItsRowsWithPartDictionaries(t *testing.T) {
	for _, tc := range []struct {
		name   string
		key    []string
		shared []string
	}{
		{"time", nil, []string{"did", "commit.record.subject", "commit.record.subject.cid"}},
		{"keyed", []string{"kind", "commit.operation", "commit.collection", "did"}, []string{"commit.record.subject", "commit.record.subject.cid"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := StoreOptions{SortKey: tc.key, TimeFields: []string{"time_us"}, Partition: 24 * time.Hour, MergeFanIn: 3, FlushInterval: -1}
			plain := opts
			plain.Part.NoPacking = true
			both := []StoreOptions{opts, plain}
			dirs := []string{t.TempDir(), t.TempDir()}
			stores := make([]*Store, len(both))
			open := func() {
				t.Helper()
				for i := range stores {
					var err error
					if stores[i], err = OpenStore(dirs[i], both[i]); err != nil {
						t.Fatal(err)
					}
				}
			}
			shut := func() {
				t.Helper()
				for i, s := range stores {
					if s != nil {
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						stores[i] = nil
					}
				}
			}
			open()
			defer shut()
			sums := func(step string) {
				t.Helper()
				var got [2]rowSum
				for i, s := range stores {
					snap := s.Table().Snapshot()
					var parts []*part.Part
					for _, h := range snap.Parts() {
						parts = append(parts, h.Part)
					}
					got[i] = sumRows(t, parts)
					snap.Release()
				}
				if got[0] != got[1] || got[0].rows == 0 {
					t.Fatalf("%s: the store with dictionaries holds %d rows of sum %x, the other %d of sum %x",
						step, got[0].rows, got[0].sum, got[1].rows, got[1].sum)
				}
			}
			g := benchdata.New(3, 8000).Posts(6000)
			var lines []byte
			for run := range 9 {
				lines = g.Append(lines[:0], 45000)
				for _, s := range stores {
					if _, err := s.Append(lines); err != nil {
						t.Fatal(err)
					}
					if err := s.Sync(); err != nil {
						t.Fatal(err)
					}
				}
				if run%3 == 2 {
					for _, s := range stores {
						if err := s.Settle(); err != nil {
							t.Fatal(err)
						}
					}
					sums("after the merges of a tier")
				}
			}
			for _, s := range stores {
				if err := s.Compact(); err != nil {
					t.Fatal(err)
				}
			}
			sums("after the compaction")
			has := sharedColumns(t, stores[0])
			for _, path := range tc.shared {
				if has[path] == 0 {
					t.Errorf("no block stores %s as codes; shared columns: %v", path, has)
				}
			}
			for _, path := range tc.key {
				if has[path] != 0 {
					t.Errorf("%d blocks store the sort key column %s as codes", has[path], path)
				}
			}
			if other := sharedColumns(t, stores[1]); len(other) != 0 {
				t.Errorf("the store that does not pack shares %v", other)
			}
			// The parts read anew from their files hold the same rows.
			shut()
			open()
			sums("after the stores were opened again")
		})
	}
}
