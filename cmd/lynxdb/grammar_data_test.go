package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGrammarDataMatchesDocs(t *testing.T) {
	docPath := filepath.Join("..", "..", "docs", "grammar", "spl2.ebnf")
	doc, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read docs grammar: %v", err)
	}
	bundled, err := grammarFS.ReadFile("grammar_data/spl2.ebnf")
	if err != nil {
		t.Fatalf("read bundled grammar: %v", err)
	}
	if string(bundled) != string(doc) {
		t.Fatalf("bundled grammar_data/spl2.ebnf differs from %s", docPath)
	}
}
