package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGeneratedPagesAreCurrent(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	for rel, want := range Pages() {
		got, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("%s: %v (run go run ./internal/docgen)", rel, err)
		}
		if string(got) != want {
			t.Errorf("%s is out of date; run go run ./internal/docgen", rel)
		}
	}
}
