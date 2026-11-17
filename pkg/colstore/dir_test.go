package colstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// writePartFile writes rows documents {"n": first+i, "tag": tag} to a part
// file of d.
func writePartFile(t testing.TB, d *Dir, first, rows int, tag string) *PartFile {
	t.Helper()
	pf, err := d.Create()
	if err != nil {
		t.Fatal(err)
	}
	w := part.NewWriter(pf, part.Options{})
	batch := shred.NewBatch(shred.Options{})
	for i := 0; i < rows; i++ {
		doc := fmt.Sprintf(`{"n":%d,"tag":"%s"}`, first+i, tag)
		if err := batch.AddJSON([]byte(doc), int64(first+i)); err != nil {
			t.Fatal(err)
		}
		if batch.Rows == 100 || i == rows-1 {
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch.Reset()
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	return pf
}

func rowsAndSum(t testing.TB, d *Dir) (rows, sum int64) {
	t.Helper()
	snap := d.Table().Snapshot()
	defer snap.Release()
	rows, sum, _ = sumN(t, snap, ScanOptions{})

	return rows, sum
}

func partFiles(t testing.TB, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if e.Name() != manifestName && e.Name() != endName {
			names = append(names, e.Name())
		}
	}

	return names
}

func TestDirKeepsCommittedPartsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 1000, "a"), writePartFile(t, d, 1000, 500, "b")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows, sum := rowsAndSum(t, d); rows != 1500 || sum != 1499*1500/2 {
		t.Fatalf("after commit: %d rows, sum %d", rows, sum)
	}

	// Swap both parts for one that holds the same rows, as a merge does.
	old := d.Table().Snapshot()
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 1500, "ab")}, a); err != nil {
		t.Fatal(err)
	}
	if got := partFiles(t, path); len(got) != 3 {
		t.Fatalf("replaced parts must stay while a snapshot reads them: %v", got)
	}
	if rows, sum, _ := sumN(t, old, ScanOptions{}); rows != 1500 || sum != 1499*1500/2 {
		t.Fatalf("old snapshot after the swap: %d rows, sum %d", rows, sum)
	}
	old.Release()
	if got := partFiles(t, path); len(got) != 1 {
		t.Fatalf("replaced parts must be deleted with their last reader: %v", got)
	}
	if _, err := d.Commit(nil, a); !errors.Is(err, ErrUnknownPart) {
		t.Fatalf("removing a part twice: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	d, err = OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if rows, sum := rowsAndSum(t, d); rows != 1500 || sum != 1499*1500/2 {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 1500, 10, "c")}, nil); err != nil {
		t.Fatal(err)
	}
	if got := partFiles(t, path); len(got) != 2 || got[0] >= got[1] {
		t.Fatalf("a part written after reopen reused a file name: %v", got)
	}
}

func TestDirRecoversFromInterruptedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 300, "kept")}, nil); err != nil {
		t.Fatal(err)
	}
	// A part that was written but never committed.
	abandoned := writePartFile(t, d, 300, 50, "lost")
	if err := abandoned.f.Close(); err != nil {
		t.Fatal(err)
	}
	// A part whose file was renamed into place before the crash, without a
	// manifest record.
	unlisted := writePartFile(t, d, 400, 50, "lost")
	if err := unlisted.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(unlisted.tmp, filepath.Join(path, unlisted.name)); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// A manifest record cut short by the crash.
	manifest := filepath.Join(path, manifestName)
	whole, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	torn := appendRecord(nil, []string{unlisted.name}, []PartMeta{{}}, nil)
	if err := os.WriteFile(manifest, append(append([]byte(nil), whole...), torn[:len(torn)-3]...), 0o644); err != nil {
		t.Fatal(err)
	}

	d, err = OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if rows, sum := rowsAndSum(t, d); rows != 300 || sum != 299*300/2 {
		t.Fatalf("after recovery: %d rows, sum %d", rows, sum)
	}
	if got := partFiles(t, path); len(got) != 1 {
		t.Fatalf("leftover files survived recovery: %v", got)
	}
	if after, _ := os.ReadFile(manifest); string(after) != string(whole) {
		t.Fatal("the torn manifest record was not cut off")
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 300, 100, "more")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	if rows, _ := rowsAndSum(t, d); rows != 400 {
		t.Fatalf("a commit after recovery was lost: %d rows", rows)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	// A manifest that names a file which is gone must not open quietly.
	if err := os.Remove(filepath.Join(path, partFiles(t, path)[0])); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDir(path); !errors.Is(err, ErrManifest) {
		t.Fatalf("opening with a missing part: %v", err)
	}
}

func TestDirRewritesLongManifest(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 5
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	var last []*Handle
	for i := 0; i < 23; i++ {
		// Every round replaces the previous part and adds a permanent one.
		add := []*PartFile{writePartFile(t, d, i*10, 10, "keep"), writePartFile(t, d, 100000, 1, "swap")}
		hs, err := d.Commit(add, last)
		if err != nil {
			t.Fatal(err)
		}
		last = hs[1:]
	}
	info, err := os.Stat(filepath.Join(path, manifestName))
	if err != nil || info.Size() > 2048 {
		t.Fatalf("manifest was not compacted: %v bytes, %v", info.Size(), err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if rows, _ := rowsAndSum(t, d); rows != 231 {
		t.Fatalf("%d rows after reopening a rewritten manifest", rows)
	}
	if got := partFiles(t, path); len(got) != 24 || strings.HasSuffix(got[0], tmpSuffix) {
		t.Fatalf("%d files: %v", len(got), got)
	}
}

func TestDirServesReadersDuringCommits(t *testing.T) {
	d, err := OpenDir(filepath.Join(t.TempDir(), "table"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	hs, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 400, "x"), writePartFile(t, d, 400, 400, "x")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if rows, sum := rowsAndSum(t, d); rows != 800 || sum != 799*800/2 {
					t.Errorf("reader saw %d rows, sum %d", rows, sum)
				}
			}
		}()
	}
	for round := 0; round < 25; round++ {
		merged, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 800, "x")}, hs)
		if err != nil {
			t.Fatal(err)
		}
		if hs, err = d.Commit([]*PartFile{writePartFile(t, d, 0, 400, "x"), writePartFile(t, d, 400, 400, "x")}, merged); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestDirKeepsRunAndTierOfParts(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 3
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	alone := writePartFile(t, d, 0, 10, "a")
	if alone.Meta.Run == 0 || alone.Meta.Tier != 0 {
		t.Fatalf("a created part file has meta %+v", alone.Meta)
	}
	// Two parts of one merged run.
	run := d.NewRun()
	left, right := writePartFile(t, d, 10, 10, "b"), writePartFile(t, d, 20, 10, "c")
	merged := PartMeta{Run: run, Tier: 2, Partition: -7}
	left.Meta, right.Meta = merged, merged
	hs, err := d.Commit([]*PartFile{alone, left, right}, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := map[PartMeta]int{alone.Meta: 1, merged: 2}
	check := func(d *Dir, when string) {
		t.Helper()
		snap := d.Table().Snapshot()
		defer snap.Release()
		got := map[PartMeta]int{}
		for _, h := range snap.Parts() {
			got[h.PartMeta]++
		}
		if len(got) != len(want) {
			t.Fatalf("%s: runs %v, want %v", when, got, want)
		}
		for meta, n := range want {
			if got[meta] != n {
				t.Fatalf("%s: runs %v, want %v", when, got, want)
			}
		}
	}
	check(d, "after commit")
	if hs[1].PartMeta != merged {
		t.Fatalf("handle meta %+v", hs[1].PartMeta)
	}
	// Enough further records to have the manifest rewritten.
	for i := 0; i < 5; i++ {
		pf := writePartFile(t, d, 1000, 1, "x")
		added, err := d.Commit([]*PartFile{pf}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := d.Commit(nil, added); err != nil {
			t.Fatal(err)
		}
	}
	check(d, "after a manifest rewrite")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	check(d, "after reopen")
	if next := d.NewRun(); next <= run {
		t.Fatalf("run %d was handed out again after reopen as %d", run, next)
	}
}

func TestDirReadsPartsAgainAfterReleasingTheirPages(t *testing.T) {
	d, err := OpenDir(filepath.Join(t.TempDir(), "table"))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 2000, "a"), writePartFile(t, d, 2000, 1000, "b")}, nil); err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		if rows, sum := rowsAndSum(t, d); rows != 3000 || sum != 2999*3000/2 {
			t.Fatalf("round %d: %d rows, sum %d", round, rows, sum)
		}
		d.ReleasePages()
	}
}
