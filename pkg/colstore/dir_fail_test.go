package colstore

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"runtime"
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

var errInjected = errors.New("injected failure")

// checkDirMatchesManifest requires the files of a directory to be the parts
// its table holds, and its manifest to name exactly those.
func checkDirMatchesManifest(t testing.TB, d *Dir, when string) {
	t.Helper()
	live := liveNames(d)
	files := partFiles(t, d.path)
	if len(files) != len(live) {
		t.Fatalf("%s: files %v beside %d live parts", when, files, len(live))
	}
	for _, name := range files {
		if _, ok := live[name]; !ok {
			t.Fatalf("%s: file %s is not a live part", when, name)
		}
	}
	log, err := os.ReadFile(filepath.Join(d.path, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	named, _, valid, err := parseManifest(log)
	if err != nil || valid != len(log) {
		t.Fatalf("%s: the manifest reads %d of %d bytes: %v", when, valid, len(log), err)
	}
	if len(named) != len(live) {
		t.Fatalf("%s: the manifest names %d parts, %d are live", when, len(named), len(live))
	}
	for name := range named {
		if _, ok := live[name]; !ok {
			t.Fatalf("%s: the manifest names %s, which is not live", when, name)
		}
	}
}

func TestCommitThatFailsLeavesNothingBehind(t *testing.T) {
	cases := []struct {
		name string
		// arrange makes the commit of add fail and returns what it removes.
		arrange func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle
	}{
		{"a part file cannot be made durable", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			replaceSyncFile(t, func(f *os.File) error {
				if failing.Load() && f.Name() == add[1].tmp {
					return errInjected
				}

				return f.Sync()
			})

			return base[:1]
		}},
		{"a part file was closed", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			add[1].f.Close()

			return nil
		}},
		{"the directory cannot be made durable", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			replaceSyncFile(t, func(f *os.File) error {
				if failing.Load() && f.Name() == d.path {
					return errInjected
				}

				return f.Sync()
			})

			return base[:1]
		}},
		{"a part file does not hold a part", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			if err := add[1].f.Truncate(20); err != nil {
				t.Fatal(err)
			}

			return base[:1]
		}},
		{"the manifest record cannot be made durable", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			replaceSyncFile(t, func(f *os.File) error {
				// Cutting the record off again is the next sync and works.
				if f.Name() == filepath.Join(d.path, manifestName) && failing.CompareAndSwap(true, false) {
					return errInjected
				}

				return f.Sync()
			})

			return base[:1]
		}},
		{"a removed part is gone", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			return []*Handle{base[0], gone[0]}
		}},
		{"a part is removed twice", func(d *Dir, add []*PartFile, base, gone []*Handle, failing *atomic.Bool) []*Handle {
			return []*Handle{base[0], base[1], base[0]}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "table")
			d, err := OpenDir(path)
			if err != nil {
				t.Fatal(err)
			}
			base, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 100, "a"), writePartFile(t, d, 100, 100, "b")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			gone, err := d.Commit([]*PartFile{writePartFile(t, d, 9000, 10, "gone")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := d.Commit(nil, gone); err != nil {
				t.Fatal(err)
			}
			add := []*PartFile{writePartFile(t, d, 200, 100, "c"), writePartFile(t, d, 300, 100, "d"), writePartFile(t, d, 400, 100, "e")}
			var failing atomic.Bool
			failing.Store(true)
			remove := c.arrange(d, add, base, gone, &failing)
			if hs, err := d.Commit(add, remove); err == nil || hs != nil {
				t.Fatalf("the commit returned %d handles and %v", len(hs), err)
			}
			failing.Store(false)
			if rows, sum := rowsAndSum(t, d); rows != 200 || sum != 199*200/2 {
				t.Fatalf("after the failed commit: %d rows, sum %d", rows, sum)
			}
			checkDirMatchesManifest(t, d, "after the failed commit")

			// The directory takes the next change, and both outlast a reopen.
			if _, err := d.Commit([]*PartFile{writePartFile(t, d, 200, 100, "c")}, base[:1]); err != nil {
				t.Fatalf("the commit after the failed one: %v", err)
			}
			checkDirMatchesManifest(t, d, "after the next commit")
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if d, err = OpenDir(path); err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if rows, sum := rowsAndSum(t, d); rows != 200 || sum != 100*199/2+100*499/2+50*100 {
				t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
			}
			checkDirMatchesManifest(t, d, "after reopen")
		})
	}
}

func TestDirTakesNoChangeOnceARecordCanBeNeitherWrittenNorTakenBack(t *testing.T) {
	cases := []struct {
		name string
		// arrange makes every write of the manifest fail from now on.
		arrange func(d *Dir)
	}{
		{"the manifest was closed", func(d *Dir) { d.log.Close() }},
		{"the manifest cannot be made durable", func(d *Dir) {
			replaceSyncFile(t, func(f *os.File) error {
				if f.Name() == filepath.Join(d.path, manifestName) {
					return errInjected
				}

				return f.Sync()
			})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "table")
			d, err := OpenDir(path)
			if err != nil {
				t.Fatal(err)
			}
			base, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 100, "a"), writePartFile(t, d, 100, 100, "b")}, nil)
			if err != nil {
				t.Fatal(err)
			}
			c.arrange(d)
			merged := []*PartFile{writePartFile(t, d, 0, 200, "ab")}
			if _, err := d.Commit(merged, base); err == nil {
				t.Fatal("a commit whose record was not written returned no error")
			}
			syncFile = (*os.File).Sync
			// Whether the record is on disk is unknown, so no later change
			// may be built on either answer.
			for i := 0; i < 2; i++ {
				if _, err := d.Commit([]*PartFile{writePartFile(t, d, 200, 100, "c")}, nil); !errors.Is(err, ErrFailed) {
					t.Fatalf("a commit after the failed record: %v", err)
				}
			}
			if rows, sum := rowsAndSum(t, d); rows != 200 || sum != 199*200/2 {
				t.Fatalf("after the failed commits: %d rows, sum %d", rows, sum)
			}
			_ = d.Close()

			// A restart finds the merge done or not done, and nothing else.
			if d, err = OpenDir(path); err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if rows, sum := rowsAndSum(t, d); rows != 200 || sum != 199*200/2 {
				t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
			}
			checkDirMatchesManifest(t, d, "after reopen")
			if _, err := d.Commit([]*PartFile{writePartFile(t, d, 200, 100, "c")}, nil); err != nil {
				t.Fatalf("a commit after reopen: %v", err)
			}
		})
	}
}

func TestCommitStandsWhenTheManifestRewriteFails(t *testing.T) {
	cases := []struct {
		name string
		// arrange makes rewrites fail while failing is set.
		arrange func(d *Dir, failing *atomic.Bool) (disarm func())
	}{
		{"the rewritten manifest cannot be created", func(d *Dir, failing *atomic.Bool) func() {
			blocker := filepath.Join(d.path, manifestName+tmpSuffix)
			if err := os.Mkdir(blocker, 0o755); err != nil {
				t.Fatal(err)
			}

			return func() { os.Remove(blocker) }
		}},
		{"the rewritten manifest cannot be made durable", func(d *Dir, failing *atomic.Bool) func() {
			replaceSyncFile(t, func(f *os.File) error {
				if failing.Load() && f.Name() == filepath.Join(d.path, manifestName+tmpSuffix) {
					return errInjected
				}

				return f.Sync()
			})

			return func() {}
		}},
		{"the directory cannot be made durable behind the rename", func(d *Dir, failing *atomic.Bool) func() {
			renamed := false
			replaceSyncFile(t, func(f *os.File) error {
				switch f.Name() {
				case filepath.Join(d.path, manifestName+tmpSuffix):
					renamed = true
				case d.path:
					if renamed && failing.Load() {
						renamed = false

						return errInjected
					}
				}

				return f.Sync()
			})

			return func() {}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
			manifestRewriteAfter = 2
			path := filepath.Join(t.TempDir(), "table")
			d, err := OpenDir(path)
			if err != nil {
				t.Fatal(err)
			}
			rows := 0
			commit := func(when string) {
				t.Helper()
				hs, err := d.Commit([]*PartFile{writePartFile(t, d, rows, 100, "x")}, nil)
				if err != nil || len(hs) != 1 {
					t.Fatalf("%s: a commit returned %d handles and %v", when, len(hs), err)
				}
				rows += 100
				if got, sum := rowsAndSum(t, d); got != int64(rows) || sum != int64(rows-1)*int64(rows)/2 {
					t.Fatalf("%s: %d rows, sum %d; want %d rows", when, got, sum, rows)
				}
			}
			commit("first")
			commit("second")
			var failing atomic.Bool
			failing.Store(true)
			disarm := c.arrange(d, &failing)
			// Each of these is durable before the rewrite it sets off fails.
			commit("with a failing rewrite")
			commit("with a failing rewrite again")
			failing.Store(false)
			disarm()
			commit("with a working rewrite")
			if d.records != 1 {
				t.Fatalf("the manifest holds %d records after a rewrite that worked", d.records)
			}
			commit("behind the rewrite")
			checkDirMatchesManifest(t, d, "before reopen")
			if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			if d, err = OpenDir(path); err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			if got, sum := rowsAndSum(t, d); got != int64(rows) || sum != int64(rows-1)*int64(rows)/2 {
				t.Fatalf("after reopen: %d rows, sum %d; want %d rows", got, sum, rows)
			}
			checkDirMatchesManifest(t, d, "after reopen")
		})
	}
}

func TestCommitInAReadOnlyDirectoryLeavesFilesForTheNextOpen(t *testing.T) {
	if os.Geteuid() == 0 || runtime.GOOS == "windows" {
		t.Skip("the directory cannot be made read-only for this user")
	}
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 100, "a")}, nil); err != nil {
		t.Fatal(err)
	}
	add := []*PartFile{writePartFile(t, d, 100, 100, "b"), writePartFile(t, d, 200, 100, "c")}
	if err := os.Chmod(path, 0o555); err != nil {
		t.Fatal(err)
	}
	_, commitErr := d.Commit(add, nil)
	_, createErr := d.Create()
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if commitErr == nil || createErr == nil {
		t.Fatalf("in a read-only directory a commit returned %v and a new part file %v", commitErr, createErr)
	}
	if rows, sum := rowsAndSum(t, d); rows != 100 || sum != 99*100/2 {
		t.Fatalf("after the failed commit: %d rows, sum %d", rows, sum)
	}
	// The files could be neither renamed nor removed.
	if got := partFiles(t, path); len(got) != 3 {
		t.Fatalf("files %v", got)
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 100, 100, "b")}, nil); err != nil {
		t.Fatalf("the commit after the failed one: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if rows, sum := rowsAndSum(t, d); rows != 200 || sum != 199*200/2 {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
	checkDirMatchesManifest(t, d, "after reopen")
}

func TestClosedDirTakesNoCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	base, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 100, "a")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	late := writePartFile(t, d, 100, 100, "late")
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Commit([]*PartFile{late}, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("a commit after Close: %v", err)
	}
	if _, err := d.Commit(nil, base); !errors.Is(err, ErrClosed) {
		t.Fatalf("a removal after Close: %v", err)
	}
	if got := partFiles(t, path); len(got) != 1 {
		t.Fatalf("files %v after a commit to a closed directory", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("closing twice: %v", err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if rows, sum := rowsAndSum(t, d); rows != 100 || sum != 99*100/2 {
		t.Fatalf("after reopen: %d rows, sum %d", rows, sum)
	}
}
