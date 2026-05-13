package spl2

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestGrammarExamplesParse(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "grammar", "examples.jsonl")
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open examples: %v", err)
	}
	defer f.Close()

	type example struct {
		NL       string `json:"nl"`
		Query    string `json:"query"`
		Category string `json:"category"`
	}

	scanner := bufio.NewScanner(f)
	line := 0
	for scanner.Scan() {
		line++
		var ex example
		if err := json.Unmarshal(scanner.Bytes(), &ex); err != nil {
			t.Fatalf("%s:%d: decode: %v", path, line, err)
		}
		if _, err := ParseProgram(ex.Query); err != nil {
			t.Errorf("%s:%d %q: ParseProgram(%q): %v", path, line, ex.NL, ex.Query, err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan examples: %v", err)
	}
}
