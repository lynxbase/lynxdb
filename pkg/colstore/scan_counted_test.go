package colstore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// TestScanDecidesCountedValuesFromTheFooter requires an equality on a path
// that a part counts to skip the part when it lacks the value, to leave the
// token filters unprobed for a value in every block, and to probe them for
// a value of a few rows.
func TestScanDecidesCountedValuesFromTheFooter(t *testing.T) {
	const blocks, blockRows = 3, 2000
	var buf bytes.Buffer
	w := part.NewWriter(&buf, part.Options{})
	batch := shred.NewBatch(shred.Options{})
	for i := 0; i < blocks*blockRows; i++ {
		kind := "common"
		if i == 2100 {
			kind = "rare"
		}
		doc := fmt.Sprintf(`{"n":%d,"kind":"%s","text":"%016x"}`, i, kind, i*7919)
		if err := batch.AddJSON([]byte(doc), int64(i)); err != nil {
			t.Fatal(err)
		}
		if batch.Rows == blockRows {
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := part.Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if id, _ := p.PathID("kind"); func() bool { _, ok := p.Counts(id); return !ok }() {
		t.Fatal("the part does not count the values of kind")
	}
	probes := 0
	p.SetReadAhead(func(_, _ int) { probes++ })
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{NewHandle(p, nil)}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()

	equal := func(v string) ScanOptions {
		return ScanOptions{Workers: 1, ColumnTokens: []ColumnTokens{{
			Path: "kind", Hashes: tokenfilter.AppendHashesLongestFirst(nil, []byte(v)), Value: []byte(v),
		}}}
	}
	for _, c := range []struct {
		value  string
		blocks int64
		probed bool
	}{
		{"absent", 0, false},
		{"common", blocks, false},
		{"rare", 1, true},
	} {
		p.ForgetReadAhead()
		probes = 0
		stats, err := snap.Scan(context.Background(), equal(c.value), func(int, *part.Part, *part.Block) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		if stats.Blocks != c.blocks || stats.SkippedByToken != blocks-c.blocks {
			t.Errorf("%s: read %d blocks and skipped %d by token, want %d read", c.value, stats.Blocks, stats.SkippedByToken, c.blocks)
		}
		if (probes > 0) != c.probed {
			t.Errorf("%s: the filters were asked for %d times, probed should be %v", c.value, probes, c.probed)
		}
	}
}
