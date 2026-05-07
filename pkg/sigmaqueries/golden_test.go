package sigmaqueries

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func walkGoldenFixtures(t *testing.T, fn func(t *testing.T, fixture, line string, lineNo int)) {
	t.Helper()

	fixtures, err := filepath.Glob(filepath.Join("testdata", "golden", "*.spl2"))
	if err != nil {
		t.Fatalf("glob golden fixtures: %v", err)
	}
	if len(fixtures) == 0 {
		t.Fatal("no golden SPL2 fixtures discovered")
	}

	for _, fixture := range fixtures {
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatalf("read %s: %v", fixture, err)
		}
		for i, raw := range strings.Split(string(data), "\n") {
			lineNo := i + 1
			line := strings.TrimSpace(raw)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			t.Run(filepath.Base(fixture)+"/"+strconv.Itoa(lineNo), func(t *testing.T) {
				fn(t, fixture, line, lineNo)
			})
		}
	}
}
