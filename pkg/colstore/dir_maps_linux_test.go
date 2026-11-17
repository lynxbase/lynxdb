//go:build linux

package colstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A part file takes two mappings whatever its size, one for the column
// chunks and one for the rest. Advice per run of blocks within one mapping
// would split it at every filter group, of which the larger part has three.
func TestDirMapsAPartFileTwice(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "table")
	d, err := OpenDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if _, err := d.Commit([]*PartFile{writePartFile(t, d, 0, 100, "a"), writePartFile(t, d, 100, 30000, "b")}, nil); err != nil {
		t.Fatal(err)
	}
	if rows, _ := rowsAndSum(t, d); rows != 30100 {
		t.Fatalf("%d rows", rows)
	}
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatal(err)
	}
	mappings := map[string]int{}
	for _, line := range strings.Split(string(maps), "\n") {
		if at := strings.Index(line, dir); at >= 0 {
			mappings[line[at:]]++
		}
	}
	if len(mappings) != 2 {
		t.Fatalf("%d part files are mapped, want 2: %v", len(mappings), mappings)
	}
	for file, n := range mappings {
		if n != 2 {
			t.Fatalf("%s has %d mappings, want 2", file, n)
		}
	}
}
