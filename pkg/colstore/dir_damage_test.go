package colstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// copyDir copies the files of src into a directory of its own, which is what
// a process killed at this moment leaves behind.
func copyDir(t testing.TB, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "copy")
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	return dst
}

// dirContents returns the content of every file of a directory by name.
func dirContents(t testing.TB, path string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range entries {
		if e.IsDir() {
			files[e.Name()] = "a directory"

			continue
		}
		data, err := os.ReadFile(filepath.Join(path, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[e.Name()] = string(data)
	}

	return files
}

func sameContents(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for name, data := range a {
		if other, ok := b[name]; !ok || other != data {
			return false
		}
	}

	return true
}

func fileNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

// dirHistory is a directory that took flushes, a merge and a drop, with the
// state every manifest record left behind.
type dirHistory struct {
	path string
	// bounds[k] is the size of the manifest after k records.
	bounds []int
	// live[k], rows[k] and sum[k] describe the table after k records.
	live []map[string]struct{}
	rows []int64
	sum  []int64
}

func liveNames(d *Dir) map[string]struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := map[string]struct{}{}
	for _, ref := range d.live {
		names[ref.name] = struct{}{}
	}

	return names
}

func buildDirHistory(t testing.TB) *dirHistory {
	t.Helper()
	h := &dirHistory{path: filepath.Join(t.TempDir(), "table")}
	d, err := OpenDir(h.path)
	if err != nil {
		t.Fatal(err)
	}
	record := func() {
		info, err := os.Stat(filepath.Join(h.path, manifestName))
		if err != nil {
			t.Fatal(err)
		}
		rows, sum := rowsAndSum(t, d)
		h.bounds = append(h.bounds, int(info.Size()))
		h.live = append(h.live, liveNames(d))
		h.rows, h.sum = append(h.rows, rows), append(h.sum, sum)
	}
	commit := func(add []*PartFile, remove []*Handle) []*Handle {
		hs, err := d.Commit(add, remove)
		if err != nil {
			t.Fatal(err)
		}
		record()

		return hs
	}
	record()
	a := commit([]*PartFile{writePartFile(t, d, 0, 100, "a")}, nil)
	bc := commit([]*PartFile{writePartFile(t, d, 100, 100, "b"), writePartFile(t, d, 200, 50, "c")}, nil)
	// A merge of two parts, whose files go away with the record.
	commit([]*PartFile{writePartFile(t, d, 0, 200, "ab")}, []*Handle{a[0], bc[0]})
	commit([]*PartFile{writePartFile(t, d, 250, 50, "d")}, nil)
	// A drop.
	commit(nil, bc[1:])
	commit([]*PartFile{writePartFile(t, d, 300, 50, "e"), writePartFile(t, d, 350, 50, "f")}, nil)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}

	return h
}

// state returns the number of whole records in a manifest cut at size.
func (h *dirHistory) state(size int) int {
	k := 0
	for k+1 < len(h.bounds) && h.bounds[k+1] <= size {
		k++
	}

	return k
}

func TestOpenDirRefusesAManifestShorterThanItsMark(t *testing.T) {
	h := buildDirHistory(t)
	file := filepath.Join(h.path, manifestName)
	manifest, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	// Every cut, also one that falls between two records or leaves none:
	// the mark was written behind the last record.
	for size := 0; size < len(manifest); size++ {
		if err := os.WriteFile(file, manifest[:size], 0o644); err != nil {
			t.Fatal(err)
		}
		before := dirContents(t, h.path)
		if d, err := OpenDir(h.path); !errors.Is(err, ErrManifest) {
			if err == nil {
				d.Close()
			}
			t.Fatalf("cut at %d of %d: %v; files %v", size, len(manifest), err, partFiles(t, h.path))
		}
		if after := dirContents(t, h.path); !sameContents(before, after) {
			t.Fatalf("cut at %d: a refused open changed the directory: %v, was %v", size, fileNames(after), fileNames(before))
		}
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDir(h.path); !errors.Is(err, ErrManifest) {
		t.Fatalf("without the manifest: %v", err)
	}
	// With all of its records back the manifest opens.
	if err := os.WriteFile(file, manifest, 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDir(h.path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	last := len(h.rows) - 1
	if rows, sum := rowsAndSum(t, d); rows != h.rows[last] || sum != h.sum[last] {
		t.Fatalf("%d rows, sum %d", rows, sum)
	}
}

func TestOpenDirSetsPartsAsideWhenNoMarkSaysTheManifestIsWhole(t *testing.T) {
	h := buildDirHistory(t)
	manifest, err := os.ReadFile(filepath.Join(h.path, manifestName))
	if err != nil {
		t.Fatal(err)
	}
	last := len(h.bounds) - 1
	opened, refused, aside := 0, 0, 0
	// No record at all, and every cut inside the last four records, which
	// hold a flush, a drop and the merge before them.
	sizes := []int{0, 3, h.bounds[1] - 1}
	for size := h.bounds[last-4]; size <= len(manifest); size++ {
		sizes = append(sizes, size)
	}
	for _, size := range sizes {
		path := copyDir(t, h.path)
		// The marks are lost, or the directory is older than they are.
		if err := os.Remove(filepath.Join(path, endName)); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, manifestName), manifest[:size], 0o644); err != nil {
			t.Fatal(err)
		}
		half := filepath.Join(path, "00000000000000ff"+partSuffix+tmpSuffix)
		if err := os.WriteFile(half, []byte("half a part"), 0o644); err != nil {
			t.Fatal(err)
		}
		before := dirContents(t, path)
		k := h.state(size)
		complete := true
		for name := range h.live[k] {
			if _, ok := before[name]; !ok {
				complete = false
			}
		}
		d, err := OpenDir(path)
		if !complete {
			// The records that are left name parts a later record removed.
			if !errors.Is(err, ErrManifest) {
				t.Fatalf("cut at %d of %d: opening without the parts of record %d: %v", size, len(manifest), k, err)
			}
			if after := dirContents(t, path); !sameContents(before, after) {
				t.Fatalf("cut at %d: a refused open changed the directory: %v, was %v", size, fileNames(after), fileNames(before))
			}
			refused++

			continue
		}
		if err != nil {
			t.Fatalf("cut at %d of %d: %v", size, len(manifest), err)
		}
		if rows, sum := rowsAndSum(t, d); rows != h.rows[k] || sum != h.sum[k] {
			t.Fatalf("cut at %d: %d rows, sum %d; record %d left %d rows, sum %d", size, rows, sum, k, h.rows[k], h.sum[k])
		}
		lost := d.Lost()
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		after := dirContents(t, path)
		if after[manifestName] != string(manifest[:h.bounds[k]]) {
			t.Fatalf("cut at %d: the manifest holds %d bytes, want the %d of its whole records", size, len(after[manifestName]), h.bounds[k])
		}
		// The parts behind the cut may be the only copy of their rows.
		kept := map[string]string{}
		if _, ok := after[lostName]; ok {
			kept = dirContents(t, filepath.Join(path, lostName))
		}
		for name, data := range before {
			_, live := h.live[k][name]
			switch {
			case !strings.HasSuffix(name, partSuffix):
			case live && after[name] != data:
				t.Fatalf("cut at %d: live part %s is missing or changed", size, name)
			case !live && (kept[name] != data || !slices.Contains(lost, name)):
				t.Fatalf("cut at %d: part %s, which no record names, was not set aside; lost %v", size, name, lost)
			case !live && after[name] != "":
				t.Fatalf("cut at %d: part %s was set aside and left in place", size, name)
			}
		}
		if len(kept) != len(lost) {
			t.Fatalf("cut at %d: %d files set aside, %d reported", size, len(kept), len(lost))
		}
		if _, ok := after[filepath.Base(half)]; ok {
			t.Fatalf("cut at %d: a temporary file was kept", size)
		}
		aside += len(lost)
		opened++
	}
	if opened < 3 || refused == 0 || aside == 0 {
		t.Fatalf("%d cuts opened with %d parts set aside and %d were refused", opened, aside, refused)
	}
}

func TestPartsSetAsideKeepTheirNamesToThemselves(t *testing.T) {
	h := buildDirHistory(t)
	if err := os.Remove(filepath.Join(h.path, endName)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.path, manifestName), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDir(h.path)
	if err != nil {
		t.Fatal(err)
	}
	lost := d.Lost()
	if len(lost) != len(h.live[len(h.live)-1]) {
		t.Fatalf("%d parts set aside, %d were live", len(lost), len(h.live[len(h.live)-1]))
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// The next open has a mark, finds nothing unnamed and leaves the parts
	// set aside where they are; new parts get names of their own.
	if d, err = OpenDir(h.path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if len(d.Lost()) != 0 {
		t.Fatalf("an open with nothing unnamed reports %v", d.Lost())
	}
	pf := writePartFile(t, d, 0, 10, "next")
	if slices.Contains(lost, pf.name) {
		t.Fatalf("a part file took the name %s of a part set aside", pf.name)
	}
	if _, err := d.Commit([]*PartFile{pf}, nil); err != nil {
		t.Fatal(err)
	}
	if kept := dirContents(t, filepath.Join(h.path, lostName)); len(kept) != len(lost) {
		t.Fatalf("%d files are set aside, were %d", len(kept), len(lost))
	}
}

func TestOpenDirRefusesADamagedManifestAndKeepsEveryFile(t *testing.T) {
	h := buildDirHistory(t)
	file := filepath.Join(h.path, manifestName)
	manifest, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	parts := partFiles(t, h.path)
	refuse := func(when string, damaged []byte) {
		t.Helper()
		if err := os.WriteFile(file, damaged, 0o644); err != nil {
			t.Fatal(err)
		}
		d, err := OpenDir(h.path)
		if err == nil {
			rows, _ := rowsAndSum(t, d)
			d.Close()
			t.Fatalf("%s: opened with %d rows, files %v, were %v", when, rows, partFiles(t, h.path), parts)
		}
		if !errors.Is(err, ErrManifest) {
			t.Fatalf("%s: %v", when, err)
		}
		if got := partFiles(t, h.path); len(got) != len(parts) {
			t.Fatalf("%s: files %v, were %v", when, got, parts)
		}
		if after, err := os.ReadFile(file); err != nil || !bytes.Equal(after, damaged) {
			t.Fatalf("%s: the refused manifest was rewritten: %v", when, err)
		}
	}
	for i := 0; i < len(manifest)*8; i++ {
		damaged := append([]byte(nil), manifest...)
		damaged[i/8] ^= 1 << (i % 8)
		refuse(fmt.Sprintf("bit %d flipped", i), damaged)
	}
	// Bytes behind the last record that hold a whole record of their own are
	// not what an interrupted append leaves.
	last := len(h.bounds) - 1
	lastRecord := manifest[h.bounds[last-1]:]
	refuse("garbage before a whole record", append(append(append([]byte(nil), manifest...), 0xff, 0x7f, 0, 1, 2), lastRecord...))

	// What an interrupted append does leave is cut off.
	for _, tail := range [][]byte{{0}, make([]byte, 7), make([]byte, 300), lastRecord[:5], lastRecord[:len(lastRecord)-1]} {
		if err := os.WriteFile(file, append(append([]byte(nil), manifest...), tail...), 0o644); err != nil {
			t.Fatal(err)
		}
		d, err := OpenDir(h.path)
		if err != nil {
			t.Fatalf("a tail of %d bytes: %v", len(tail), err)
		}
		if rows, sum := rowsAndSum(t, d); rows != h.rows[last] || sum != h.sum[last] {
			t.Fatalf("a tail of %d bytes: %d rows, sum %d", len(tail), rows, sum)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		if after, err := os.ReadFile(file); err != nil || !bytes.Equal(after, manifest) {
			t.Fatalf("a tail of %d bytes was not cut off: %v", len(tail), err)
		}
	}
}

// crashedDir returns a copy of a directory taken while it held what a kill
// leaves behind: a part renamed into place without a record, a part file
// half written, a manifest rewrite that did not finish, a removed part whose
// file a reader still held, and a record cut short. It also returns the rows
// and the sum of the live parts and the name of one of them.
func crashedDir(t testing.TB) (path string, rows, sum int64, live string) {
	t.Helper()
	h := buildDirHistory(t)
	d, err := OpenDir(h.path)
	if err != nil {
		t.Fatal(err)
	}
	unlisted := writePartFile(t, d, 1000, 50, "unlisted")
	if err := unlisted.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(unlisted.tmp, filepath.Join(h.path, unlisted.name)); err != nil {
		t.Fatal(err)
	}
	half := writePartFile(t, d, 2000, 50, "half")
	if err := half.f.Truncate(100); err != nil {
		t.Fatal(err)
	}
	if err := half.f.Close(); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{manifestName + tmpSuffix: "half a manifest", "NOTES": "not a part"} {
		if err := os.WriteFile(filepath.Join(h.path, name), []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	reader := d.Table().Snapshot()
	defer reader.Release()
	if _, err := d.Commit(nil, reader.Parts()[:1]); err != nil {
		t.Fatal(err)
	}
	rows, sum = rowsAndSum(t, d)
	for name := range liveNames(d) {
		live = name
	}
	path = copyDir(t, h.path)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(path, manifestName), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	torn := appendRecord(nil, []string{unlisted.name}, []PartMeta{{Run: 99}}, nil)
	if _, err := f.Write(torn[:len(torn)-4]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	return path, rows, sum, live
}

func TestOpenDirRemovesWhatAKillLeavesBehind(t *testing.T) {
	path, rows, sum, _ := crashedDir(t)
	before := dirContents(t, path)
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, gotSum := rowsAndSum(t, d); got != rows || gotSum != sum {
		t.Fatalf("%d rows, sum %d; want %d rows, sum %d", got, gotSum, rows, sum)
	}
	live := liveNames(d)
	after := dirContents(t, path)
	// Gone are the unlisted part, the two temporary files and the removed
	// part; the manifest, its marks and the foreign file stay beside the
	// live parts.
	if len(after) != len(live)+3 || len(after) != len(before)-4 {
		t.Fatalf("files %v, were %v; %d parts are live", fileNames(after), fileNames(before), len(live))
	}
	for name := range live {
		if after[name] != before[name] {
			t.Fatalf("live part %s is missing or changed", name)
		}
	}
	if after["NOTES"] != before["NOTES"] {
		t.Fatal("a file that is neither a part nor a temporary file was touched")
	}
	if names, _, valid, err := parseManifest([]byte(after[manifestName])); err != nil || valid != len(after[manifestName]) || len(names) != len(live) {
		t.Fatalf("the manifest names %d parts in %d of %d bytes: %v", len(names), valid, len(after[manifestName]), err)
	}
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 5000, 10, "next")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(path); err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if got, _ := rowsAndSum(t, d); got != rows+10 {
		t.Fatalf("%d rows after a commit behind the cut, want %d", got, rows+10)
	}
}

func TestOpenDirKeepsEveryFileWhenANamedPartIsMissingOrDamaged(t *testing.T) {
	path, rows, sum, live := crashedDir(t)
	file := filepath.Join(path, live)
	whole, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	trailer := append([]byte(nil), whole...)
	trailer[len(trailer)-10] ^= 0x40
	damage := []struct {
		name  string
		apply func() error
	}{
		{"missing", func() error { return os.Remove(file) }},
		{"empty", func() error { return os.WriteFile(file, nil, 0o644) }},
		{"cut in half", func() error { return os.WriteFile(file, whole[:len(whole)/2], 0o644) }},
		{"one byte short", func() error { return os.WriteFile(file, whole[:len(whole)-1], 0o644) }},
		{"damaged trailer", func() error { return os.WriteFile(file, trailer, 0o644) }},
		{"a directory", func() error { _ = os.Remove(file); return os.Mkdir(file, 0o755) }},
	}
	for _, c := range damage {
		if err := c.apply(); err != nil {
			t.Fatal(err)
		}
		before := dirContents(t, path)
		if d, err := OpenDir(path); !errors.Is(err, ErrManifest) {
			if err == nil {
				d.Close()
			}
			t.Fatalf("%s: %v", c.name, err)
		}
		if after := dirContents(t, path); !sameContents(before, after) {
			t.Fatalf("%s: a refused open changed the directory: %v, was %v", c.name, fileNames(after), fileNames(before))
		}
	}
	// With the part back, the same directory opens.
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, whole, 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if got, gotSum := rowsAndSum(t, d); got != rows || gotSum != sum {
		t.Fatalf("%d rows, sum %d after the part came back; want %d rows, sum %d", got, gotSum, rows, sum)
	}
}

func TestOpenDirRefusesPartsThatLostTheirManifest(t *testing.T) {
	path, _, _, _ := crashedDir(t)
	if err := os.Remove(filepath.Join(path, manifestName)); err != nil {
		t.Fatal(err)
	}
	before := dirContents(t, path)
	if d, err := OpenDir(path); !errors.Is(err, ErrManifest) {
		if err == nil {
			d.Close()
		}
		t.Fatalf("opening parts without a manifest: %v", err)
	}
	if after := dirContents(t, path); !sameContents(before, after) {
		t.Fatalf("a refused open changed the directory: %v, was %v", fileNames(after), fileNames(before))
	}

	// A kill before the first record leaves an empty manifest beside the
	// files of the change that was under way, and those go.
	fresh := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(fresh)
	if err != nil {
		t.Fatal(err)
	}
	renamed := writePartFile(t, d, 0, 10, "renamed")
	if err := renamed.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(renamed.tmp, filepath.Join(fresh, renamed.name)); err != nil {
		t.Fatal(err)
	}
	written := writePartFile(t, d, 10, 10, "written")
	if err := written.f.Close(); err != nil {
		t.Fatal(err)
	}
	killed := copyDir(t, fresh)
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if d, err = OpenDir(killed); err != nil {
		t.Fatal(err)
	}
	if got := partFiles(t, killed); len(got) != 0 {
		t.Fatalf("files of a change without a record survived: %v", got)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Temporary files alone need no manifest to be told apart from parts.
	if err := os.WriteFile(filepath.Join(killed, written.name+tmpSuffix), []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(killed, manifestName)); err != nil {
		t.Fatal(err)
	}
	again, err := OpenDir(killed)
	if err != nil {
		t.Fatalf("opening temporary files without a manifest: %v", err)
	}
	defer again.Close()
	if got := partFiles(t, killed); len(got) != 0 {
		t.Fatalf("a temporary file survived: %v", got)
	}
}

func TestKillBetweenManifestAndMarkLeavesAManifestThatCountsAsWhole(t *testing.T) {
	defer func(n int) { manifestRewriteAfter = n }(manifestRewriteAfter)
	manifestRewriteAfter = 3
	path := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(path)
	if err != nil {
		t.Fatal(err)
	}
	// A part without a record lies in the directory all along: an open that
	// takes the manifest for whole removes it, any other sets it aside.
	unlisted := writePartFile(t, d, 9000, 10, "unlisted")
	if err := unlisted.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(unlisted.tmp, filepath.Join(path, unlisted.name)); err != nil {
		t.Fatal(err)
	}
	type image struct {
		path, when string
		rows       int64
	}
	var images []image
	var committed int64
	rewriteAt := -1
	// The directory is copied behind every sync around the manifest: with
	// a record that has no mark yet, with a rewritten manifest that is not
	// in place yet, and with one that is.
	replaceSyncFile(t, func(f *os.File) error {
		if err := f.Sync(); err != nil {
			return err
		}
		switch base := filepath.Base(f.Name()); {
		case base == manifestName:
			committed += 100
		case base == manifestName+tmpSuffix:
			rewriteAt = len(images)
		case f.Name() != path:
			return nil
		}
		images = append(images, image{copyDir(t, path), filepath.Base(f.Name()), committed})

		return nil
	})
	for i := 0; i < 9; i++ {
		if _, err := d.Commit([]*PartFile{writePartFile(t, d, i*100, 100, "x")}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if rewriteAt < 0 || rewriteAt+1 >= len(images) {
		t.Fatalf("no rewrite among %d syncs", len(images))
	}
	// Between the two copies around a rewrite lies the moment at which the
	// mark of the rewritten manifest is written and the manifest is not
	// renamed yet.
	between := image{copyDir(t, images[rewriteAt].path), "the mark of a rewrite", images[rewriteAt].rows}
	next, err := os.ReadFile(filepath.Join(images[rewriteAt+1].path, endName))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(between.path, endName), next, 0o644); err != nil {
		t.Fatal(err)
	}
	for i, img := range append(images, between) {
		killed, err := OpenDir(img.path)
		if err != nil {
			t.Fatalf("copy %d, behind the sync of %s: %v", i, img.when, err)
		}
		if rows, sum := rowsAndSum(t, killed); rows != img.rows || sum != (img.rows-1)*img.rows/2 {
			t.Fatalf("copy %d, behind the sync of %s: %d rows, sum %d; want %d rows", i, img.when, rows, sum, img.rows)
		}
		if lost := killed.Lost(); len(lost) != 0 {
			t.Fatalf("copy %d, behind the sync of %s: parts %v were set aside", i, img.when, lost)
		}
		checkDirMatchesManifest(t, killed, "a copy behind the sync of "+img.when)
		if err := killed.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
