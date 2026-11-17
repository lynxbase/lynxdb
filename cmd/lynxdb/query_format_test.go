package main

import "testing"

func TestStripVerticalQuerySuffix(t *testing.T) {
	got, ok := stripVerticalQuerySuffix(`search source=main level=error | head 5 \G`)
	if !ok {
		t.Fatal("expected suffix to be detected")
	}
	if want := "search source=main level=error | head 5"; got != want {
		t.Fatalf("query = %q, want %q", got, want)
	}
}

func TestStripVerticalQuerySuffixAfterSemicolon(t *testing.T) {
	got, ok := stripVerticalQuerySuffix("search source=main level=error | head 5 \\G;  \n")
	if !ok {
		t.Fatal("expected suffix to be detected")
	}
	if want := "search source=main level=error | head 5"; got != want {
		t.Fatalf("query = %q, want %q", got, want)
	}
}

func TestStripVerticalQuerySuffixAbsent(t *testing.T) {
	query := `search source=main level=error | head 5`
	got, ok := stripVerticalQuerySuffix(query)
	if ok {
		t.Fatal("did not expect suffix")
	}
	if got != query {
		t.Fatalf("query = %q, want %q", got, query)
	}
}
