package colstore

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// replaceSyncFile puts fn in place of syncFile until the test ends.
func replaceSyncFile(t testing.TB, fn func(f *os.File) error) {
	t.Helper()
	old := syncFile
	syncFile = fn
	t.Cleanup(func() { syncFile = old })
}

func TestManifestRewriteNamesThePartsTheLogNamed(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 1
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	manifest := filepath.Join(path, manifestName)
	var rewrites atomic.Int64
	var wrong atomic.Pointer[string]
	// Nothing is synced, so that commits follow each other closely. A
	// rewrite is compared, once its file is complete, with the log it
	// replaces: a part of a change still under way is in neither.
	replaceSyncFile(t, func(f *os.File) error {
		if f.Name() != manifest+tmpSuffix {
			return nil
		}
		old, oldErr := os.ReadFile(manifest)
		next, nextErr := os.ReadFile(f.Name())
		before, _, _, beforeErr := parseManifest(old)
		after, _, _, afterErr := parseManifest(next)
		if err := errors.Join(oldErr, nextErr, beforeErr, afterErr); err != nil {
			msg := err.Error()
			wrong.CompareAndSwap(nil, &msg)
		} else if !maps.Equal(before, after) {
			msg := fmt.Sprintf("a rewrite names %d parts, the log it replaces %d", len(after), len(before))
			wrong.CompareAndSwap(nil, &msg)
		}
		rewrites.Add(1)

		return nil
	})
	const committers, rounds = 8, 60
	var wg sync.WaitGroup
	for c := 0; c < committers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last []*Handle
			for i := 0; i < rounds; i++ {
				// Three parts at a time take the place of the three before,
				// as the output of a merge does.
				add := make([]*PartFile, 3)
				for j := range add {
					add[j] = writePartFile(t, d, c*1000+j, 3, "x")
				}
				hs, err := d.Commit(add, last)
				if err != nil {
					t.Error(err)

					return
				}
				last = hs
			}
		}()
	}
	wg.Wait()
	if msg := wrong.Load(); msg != nil {
		t.Fatal(*msg)
	}
	if rewrites.Load() < committers*rounds/2 {
		t.Fatalf("%d rewrites in %d commits", rewrites.Load(), committers*rounds)
	}
	if rows, _ := rowsAndSum(t, d); rows != committers*9 {
		t.Fatalf("%d rows, want %d", rows, committers*9)
	}
}

func TestFailedCommitStaysOutOfARewrittenManifest(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 2
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := d.Commit([]*PartFile{writePartFile(t, d, 5000, 100, "dropped")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Commit(nil, dropped); err != nil {
		t.Fatal(err)
	}
	// A merge that lost its input to the drop.
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 5000, 100, "merged")}, dropped); !errors.Is(err, ErrUnknownPart) {
		t.Fatalf("a commit that removes a dropped part: %v", err)
	}
	// Enough further records to have the manifest rewritten.
	for i := 0; i < 4; i++ {
		if _, err := d.Commit([]*PartFile{writePartFile(t, d, i*100, 100, "kept")}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if rows, sum := rowsAndSum(t, d); rows != 400 || sum != 399*400/2 {
		t.Fatalf("before reopen: %d rows, sum %d", rows, sum)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if rows, sum := rowsAndSum(t, d); rows != 400 || sum != 399*400/2 {
		t.Fatalf("after reopen: %d rows, sum %d; the failed commit holds 100 rows", rows, sum)
	}
}
