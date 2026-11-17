package parser

import (
	"strings"
	"testing"
)

func TestParsePipeline(t *testing.T) {
	pl, err := ParsePipeline("source=t | where x > 1 | head 5;")
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.Commands) != 3 {
		t.Fatalf("got %d commands, want 3", len(pl.Commands))
	}
	if _, err := ParsePipeline("explain source=t | head 5"); err == nil || !strings.Contains(err.Error(), "explain is not allowed here") {
		t.Fatalf("explain prefix: got %v", err)
	}
}
