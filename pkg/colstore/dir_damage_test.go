package colstore

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
