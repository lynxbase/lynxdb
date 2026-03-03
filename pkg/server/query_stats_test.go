package server

import (
	"testing"
)

func TestComputeSearchSelectivity_NoRows(t *testing.T) {
	ss := &SearchStats{RowsScanned: 0}
	computeSearchSelectivity(ss)

	if ss.SearchSelectivity != 0 {
		t.Errorf("expected 0 selectivity for zero rows, got %f", ss.SearchSelectivity)
	}

	if ss.Suggestion != "" {
		t.Errorf("expected no suggestion, got %q", ss.Suggestion)
	}
}

func TestComputeSearchSelectivity_FullMatch(t *testing.T) {
	// 100% match rate on large dataset — should get suggestion.
	ss := &SearchStats{
		RowsScanned: 50_000,
		MatchedRows: 50_000,
	}
	computeSearchSelectivity(ss)

	if ss.SearchSelectivity != 1.0 {
		t.Errorf("expected selectivity 1.0, got %f", ss.SearchSelectivity)
	}

	if ss.Suggestion == "" {
		t.Error("expected suggestion for 100% selectivity on large dataset")
	}
}

func TestComputeSearchSelectivity_HighSelectivity(t *testing.T) {
	// 1% match rate — no suggestion.
	ss := &SearchStats{
		RowsScanned: 100_000,
		MatchedRows: 1_000,
	}
	computeSearchSelectivity(ss)

	expected := 0.01
	if ss.SearchSelectivity < expected-0.001 || ss.SearchSelectivity > expected+0.001 {
		t.Errorf("expected selectivity ~%f, got %f", expected, ss.SearchSelectivity)
	}

	if ss.Suggestion != "" {
		t.Errorf("expected no suggestion for high selectivity, got %q", ss.Suggestion)
	}
}

func TestComputeSearchSelectivity_SmallDataset(t *testing.T) {
	// 100% match but only 100 rows — no suggestion (below 10K threshold).
	ss := &SearchStats{
		RowsScanned: 100,
		MatchedRows: 100,
	}
	computeSearchSelectivity(ss)

	if ss.SearchSelectivity != 1.0 {
		t.Errorf("expected selectivity 1.0, got %f", ss.SearchSelectivity)
	}

	if ss.Suggestion != "" {
		t.Errorf("expected no suggestion for small dataset, got %q", ss.Suggestion)
	}
}

func TestComputeSearchSelectivity_NoFilterStage(t *testing.T) {
	// MatchedRows == 0 means no filter stage — all rows treated as matched.
	ss := &SearchStats{
		RowsScanned: 20_000,
		MatchedRows: 0,
	}
	computeSearchSelectivity(ss)

	if ss.SearchSelectivity != 1.0 {
		t.Errorf("expected selectivity 1.0 when no filter stage, got %f", ss.SearchSelectivity)
	}

	if ss.Suggestion == "" {
		t.Error("expected suggestion when no filter stage on large dataset")
	}
}

func TestComputeSearchSelectivity_BorderlineThreshold(t *testing.T) {
	// Exactly 90% selectivity with exactly 10K rows — should trigger suggestion.
	ss := &SearchStats{
		RowsScanned: 10_000,
		MatchedRows: 9_000,
	}
	computeSearchSelectivity(ss)

	expected := 0.9
	if ss.SearchSelectivity < expected-0.001 || ss.SearchSelectivity > expected+0.001 {
		t.Errorf("expected selectivity ~%f, got %f", expected, ss.SearchSelectivity)
	}

	// 0.9 >= 0.9 is true, so suggestion should fire.
	if ss.Suggestion == "" {
		t.Error("expected suggestion at 90% selectivity threshold")
	}
}

func TestComputeSearchSelectivity_JustBelowThreshold(t *testing.T) {
	// 89% selectivity — should NOT trigger suggestion.
	ss := &SearchStats{
		RowsScanned: 10_000,
		MatchedRows: 8_900,
	}
	computeSearchSelectivity(ss)

	if ss.Suggestion != "" {
		t.Errorf("expected no suggestion at 89%% selectivity, got %q", ss.Suggestion)
	}
}
