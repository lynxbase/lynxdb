package colstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// makePart builds a part of rows documents {"n": first+i, "tag": tag} in
// blocks of blockRows, timestamped first+i.
func makePart(t testing.TB, first, rows, blockRows int, tag string) *part.Part {
	t.Helper()
	var buf bytes.Buffer
	w := part.NewWriter(&buf, part.Options{})
	batch := shred.NewBatch(shred.Options{})
	for i := 0; i < rows; i++ {
		doc := fmt.Sprintf(`{"n":%d,"tag":"%s"}`, first+i, tag)
		if err := batch.AddJSON([]byte(doc), int64(first+i)); err != nil {
			t.Fatal(err)
		}
		if batch.Rows == blockRows || i == rows-1 {
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

	return p
}

// sumN scans snap and returns the number of rows and the sum of column n.
func sumN(t testing.TB, snap *Snapshot, opts ScanOptions) (rows, sum int64, stats ScanStats) {
	t.Helper()
	workers := 8
	opts.Workers = workers
	cols := make([]vec.Column, workers)
	partRows := make([]int64, workers)
	partSum := make([]int64, workers)
	stats, err := snap.Scan(context.Background(), opts, func(w int, p *part.Part, b *part.Block) error {
		id, _ := p.PathID("n")
		ref, ok := b.Column(id, vec.KindInt)
		if !ok {
			return errors.New("no n column")
		}
		if err := b.Decode(ref, &cols[w]); err != nil {
			return err
		}
		partRows[w] += int64(b.Rows)
		for i := 0; i < b.Rows; i++ {
			partSum[w] += cols[w].Int(i)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for w := range partRows {
		rows += partRows[w]
		sum += partSum[w]
	}

	return rows, sum, stats
}

func TestSnapshotIsUnaffectedByLaterChanges(t *testing.T) {
	tbl := NewTable()
	var released []string
	handle := func(name string, first, rows int) *Handle {
		return NewHandle(makePart(t, first, rows, 100, name), func() { released = append(released, name) })
	}
	a, b := handle("a", 0, 1000), handle("b", 1000, 1000)
	if err := tbl.Apply([]*Handle{a, b}, nil); err != nil {
		t.Fatal(err)
	}

	before := tbl.Snapshot()
	merged := handle("ab", 0, 2000)
	if err := tbl.Apply([]*Handle{merged}, []*Handle{a, b}); err != nil {
		t.Fatal(err)
	}
	after := tbl.Snapshot()

	// Both views hold the same 2000 rows exactly once.
	for name, snap := range map[string]*Snapshot{"before": before, "after": after} {
		if rows, sum, _ := sumN(t, snap, ScanOptions{}); rows != 2000 || sum != 1999*2000/2 {
			t.Fatalf("%s the merge: %d rows, sum %d", name, rows, sum)
		}
	}
	if len(released) != 0 {
		t.Fatalf("parts released while a snapshot uses them: %v", released)
	}
	before.Release()
	before.Release()
	if len(released) != 2 {
		t.Fatalf("replaced parts not released after their last reader: %v", released)
	}
	after.Release()
	tbl.Close()
	if len(released) != 3 {
		t.Fatalf("released = %v", released)
	}
	if err := tbl.Apply(nil, []*Handle{a}); !errors.Is(err, ErrUnknownPart) {
		t.Fatalf("removing an absent part: %v", err)
	}
}

func TestScanPrunesByTimeAndToken(t *testing.T) {
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{
		NewHandle(makePart(t, 0, 1000, 100, "alpha"), nil),
		NewHandle(makePart(t, 1000, 1000, 100, "beta"), nil),
	}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()

	rows, _, stats := sumN(t, snap, ScanOptions{MinTime: 250, MaxTime: 349})
	if rows != 200 || stats.Blocks != 2 || stats.SkippedByTime != 18 {
		t.Fatalf("time range read %d rows from %d blocks, skipped %d", rows, stats.Blocks, stats.SkippedByTime)
	}
	token := tokenfilter.AppendHashes(nil, []byte("BETA"))
	rows, _, stats = sumN(t, snap, ScanOptions{Tokens: token})
	if rows != 1000 || stats.SkippedByToken != 10 {
		t.Fatalf("token search read %d rows, skipped %d blocks", rows, stats.SkippedByToken)
	}
	rows, _, stats = sumN(t, snap, ScanOptions{Tokens: tokenfilter.AppendHashes(nil, []byte("gamma"))})
	if rows != 0 || stats.SkippedByToken != 20 {
		t.Fatalf("absent token read %d rows, skipped %d blocks", rows, stats.SkippedByToken)
	}
}

// wordPart builds a part of blocks of blockRows rows whose documents hold a
// run of words of their own at each of paths, enough of them for every
// column to get a token filter of its own, and extra[row] at extra[row+1]
// of the row numbered row.
func wordPart(t testing.TB, r *rand.Rand, blocks, blockRows int, paths []string, extra map[int][2]string) *part.Part {
	t.Helper()
	var buf bytes.Buffer
	w := part.NewWriter(&buf, part.Options{})
	batch := shred.NewBatch(shred.Options{})
	for i := 0; i < blocks*blockRows; i++ {
		doc := fmt.Sprintf(`{"n":%d`, i)
		for _, path := range paths {
			text := fmt.Sprintf("%016x %016x", r.Uint64(), r.Uint64())
			if e, ok := extra[i]; ok && e[0] == path {
				text += " " + e[1]
			}
			doc += fmt.Sprintf(`,"%s":"%s"`, path, text)
		}
		if err := batch.AddJSON([]byte(doc+"}"), int64(i)); err != nil {
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
	for _, path := range paths {
		id, _ := p.PathID(path)
		for i := 0; i < p.NumBlocks(); i++ {
			if !p.OwnFilter(i, id, vec.KindString) {
				t.Fatalf("column %s of block %d has no filter of its own", path, i)
			}
		}
	}

	return p
}

func TestScanPrunesByTheTokensOfAColumn(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	const blockRows = 2000
	// Row 2100 lies in the second block and row 4500 in the third.
	parts := []*part.Part{
		wordPart(t, r, 3, blockRows, []string{"a", "b"}, map[int][2]string{2100: {"a", "needle in hay"}, 4500: {"b", "straw"}}),
		wordPart(t, r, 3, blockRows, []string{"a", "b"}, map[int][2]string{2100: {"b", "needle"}, 2101: {"a", "hay"}}),
		wordPart(t, r, 2, blockRows, []string{"b"}, map[int][2]string{100: {"b", "needle hay"}}),
	}
	tbl := NewTable()
	defer tbl.Close()
	var handles []*Handle
	for _, p := range parts {
		handles = append(handles, NewHandle(p, nil))
	}
	if err := tbl.Apply(handles, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()

	hashes := func(text string) []uint64 { return tokenfilter.AppendHashes(nil, []byte(text)) }
	in := func(path, text string) ColumnTokens { return ColumnTokens{Path: path, Hashes: hashes(text)} }
	for _, c := range []struct {
		name   string
		opts   ScanOptions
		blocks int64
	}{
		{"any string of a block", ScanOptions{Tokens: hashes("needle")}, 3},
		{"tokens of several columns of a block", ScanOptions{Tokens: hashes("needle hay")}, 3},
		{"a column", ScanOptions{ColumnTokens: []ColumnTokens{in("a", "needle")}}, 1},
		{"a column of every part", ScanOptions{ColumnTokens: []ColumnTokens{in("b", "needle")}}, 2},
		{"a column that holds one token of two", ScanOptions{ColumnTokens: []ColumnTokens{in("b", "needle hay")}}, 1},
		{"any one column", ScanOptions{ColumnTokens: []ColumnTokens{in("", "needle")}}, 3},
		{"any one column for two tokens", ScanOptions{ColumnTokens: []ColumnTokens{in("", "hay needle")}}, 2},
		{"two columns", ScanOptions{ColumnTokens: []ColumnTokens{in("a", "hay"), in("b", "needle")}}, 1},
		{"two columns of blocks apart", ScanOptions{ColumnTokens: []ColumnTokens{in("a", "needle"), in("b", "straw")}}, 0},
		{"a column no part has", ScanOptions{ColumnTokens: []ColumnTokens{in("c", "needle")}}, 0},
		{"a column and a time range", ScanOptions{MinTime: 4000, MaxTime: 5999, ColumnTokens: []ColumnTokens{in("b", "needle")}}, 0},
	} {
		rows, _, stats := sumN(t, snap, c.opts)
		total := int64(8)
		if c.opts.MaxTime != 0 {
			total = 2
		}
		if stats.Blocks != c.blocks || rows != c.blocks*blockRows || stats.SkippedByToken != total-c.blocks {
			t.Errorf("%s: read %d rows of %d blocks and skipped %d by token, want %d blocks of %d", c.name, rows, stats.Blocks, stats.SkippedByToken, c.blocks, total)
		}
		admitted := int64(0)
		for _, p := range parts {
			for i := 0; i < p.NumBlocks(); i++ {
				may, err := BlockMayContain(p, i, c.opts.ColumnTokens)
				if err != nil {
					t.Fatal(err)
				}
				if _, lo, hi := p.BlockStats(i); may && (c.opts.MaxTime == 0 || (hi >= c.opts.MinTime && lo <= c.opts.MaxTime)) {
					admitted++
				}
			}
		}
		if len(c.opts.Tokens) == 0 && admitted != c.blocks {
			t.Errorf("%s: %d blocks may hold the tokens, the scan read %d", c.name, admitted, c.blocks)
		}
	}
}

func TestScanTurnsWorkerPanicsIntoErrors(t *testing.T) {
	tbl := NewTable()
	defer tbl.Close()
	if err := tbl.Apply([]*Handle{NewHandle(makePart(t, 0, 1000, 10, "x"), nil)}, nil); err != nil {
		t.Fatal(err)
	}
	snap := tbl.Snapshot()
	defer snap.Release()

	_, err := snap.Scan(context.Background(), ScanOptions{Workers: 4}, func(int, *part.Part, *part.Block) error {
		panic("boom")
	})
	if err == nil {
		t.Fatal("a panicking worker did not fail the scan")
	}
	want := errors.New("stop")
	if _, err := snap.Scan(context.Background(), ScanOptions{}, func(int, *part.Part, *part.Block) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := snap.Scan(ctx, ScanOptions{}, func(int, *part.Part, *part.Block) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled scan: %v", err)
	}
}

// Readers must see every row exactly once while parts are merged under
// them, and a part must be released only after its last reader.
func TestConcurrentReadersAndMerges(t *testing.T) {
	const partRows, parts = 500, 8
	tbl := NewTable()
	var live atomic.Int64
	var mu sync.Mutex
	inUse := map[*Handle]bool{}
	handle := func(first, rows int) *Handle {
		var h *Handle
		live.Add(1)
		h = NewHandle(makePart(t, first, rows, 100, "t"), func() {
			mu.Lock()
			delete(inUse, h)
			mu.Unlock()
			live.Add(-1)
		})
		mu.Lock()
		inUse[h] = true
		mu.Unlock()

		return h
	}
	// spans stays ordered by first row, whatever order the table keeps.
	type span struct {
		h           *Handle
		first, rows int
	}
	var spans []span
	var initial []*Handle
	for i := 0; i < parts; i++ {
		s := span{h: handle(i*partRows, partRows), first: i * partRows, rows: partRows}
		spans = append(spans, s)
		initial = append(initial, s.h)
	}
	if err := tbl.Apply(initial, nil); err != nil {
		t.Fatal(err)
	}
	const total = partRows * parts
	wantSum := int64(total-1) * total / 2

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 6; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := tbl.Snapshot()
				for _, h := range snap.Parts() {
					mu.Lock()
					ok := inUse[h]
					mu.Unlock()
					if !ok {
						t.Error("snapshot holds a released part")
					}
				}
				if rows, sum, _ := sumN(t, snap, ScanOptions{}); rows != total || sum != wantSum {
					t.Errorf("reader saw %d rows, sum %d", rows, sum)
				}
				snap.Release()
			}
		}()
	}
	// Merge two neighbouring spans, then split the result at another row,
	// so the part set keeps changing while the rows stay the same.
	for round := 0; round < 60; round++ {
		i := round % (len(spans) - 1)
		a, b := spans[i], spans[i+1]
		merged := span{h: handle(a.first, a.rows+b.rows), first: a.first, rows: a.rows + b.rows}
		if err := tbl.Apply([]*Handle{merged.h}, []*Handle{a.h, b.h}); err != nil {
			t.Fatal(err)
		}
		cut := 1 + (round*37)%(merged.rows-1)
		left := span{h: handle(merged.first, cut), first: merged.first, rows: cut}
		right := span{h: handle(merged.first+cut, merged.rows-cut), first: merged.first + cut, rows: merged.rows - cut}
		if err := tbl.Apply([]*Handle{left.h, right.h}, []*Handle{merged.h}); err != nil {
			t.Fatal(err)
		}
		spans[i], spans[i+1] = left, right
	}
	close(stop)
	wg.Wait()
	if live.Load() != parts {
		t.Fatalf("%d parts alive, want %d", live.Load(), parts)
	}
	tbl.Close()
	if live.Load() != 0 {
		t.Fatalf("%d parts leaked after Close", live.Load())
	}
}

func TestUnionHoldsThePartsOfItsTablesUntilReleased(t *testing.T) {
	var released []string
	table := func(name string, first, rows int) *Table {
		tbl := NewTable()
		h := NewHandle(makePart(t, first, rows, 100, name), func() { released = append(released, name) })
		if err := tbl.Apply([]*Handle{h}, nil); err != nil {
			t.Fatal(err)
		}

		return tbl
	}
	a, b := table("a", 0, 1000), table("b", 1000, 500)
	one, other := a.Snapshot(), b.Snapshot()
	all := Union(one, other)
	one.Release()
	other.Release()
	a.Close()
	b.Close()
	if rows, sum, _ := sumN(t, all, ScanOptions{}); rows != 1500 || sum != 1499*1500/2 {
		t.Fatalf("the union holds %d rows, sum %d", rows, sum)
	}
	if len(released) != 0 {
		t.Fatalf("parts released while the union uses them: %v", released)
	}
	all.Release()
	if len(released) != 2 {
		t.Fatalf("released = %v", released)
	}
	none := Union()
	if rows, _, _ := sumN(t, none, ScanOptions{}); rows != 0 {
		t.Fatalf("a union of nothing holds %d rows", rows)
	}
	none.Release()
}
