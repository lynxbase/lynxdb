package stats

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateRecommendations_NoTimeRangePruning(t *testing.T) {
	s := &QueryStats{
		TotalSegments:       10,
		ScannedSegments:     10,
		TimeSkippedSegments: 0,
		Ephemeral:           false,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Add --since")
	if found == nil {
		t.Fatal("expected time range recommendation")
	}
	if found.Priority != "medium" {
		t.Errorf("priority = %q, want medium", found.Priority)
	}
}

func TestGenerateRecommendations_NoTimeRangePruning_Ephemeral(t *testing.T) {
	s := &QueryStats{
		TotalSegments:       10,
		ScannedSegments:     10,
		TimeSkippedSegments: 0,
		Ephemeral:           true,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Add --since")
	if found != nil {
		t.Fatal("should not suggest time range for ephemeral queries")
	}
}

func TestGenerateRecommendations_LowSelectivity(t *testing.T) {
	s := &QueryStats{
		ScannedRows: 200_000,
		MatchedRows: 100, // 0.05% selectivity
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "field filter")
	if found == nil {
		t.Fatal("expected low selectivity recommendation")
	}
}

func TestGenerateRecommendations_NoBloomPruning(t *testing.T) {
	s := &QueryStats{
		TotalSegments:        8,
		ScannedSegments:      8,
		BloomSkippedSegments: 0,
		Ephemeral:            false,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "bloom filter")
	if found == nil {
		t.Fatal("expected bloom filter recommendation")
	}
}

func TestGenerateRecommendations_LowSkipRate(t *testing.T) {
	s := &QueryStats{
		TotalSegments:   20,
		ScannedSegments: 18,
		Ephemeral:       false,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Most segments scanned")
	if found == nil {
		t.Fatal("expected low skip rate recommendation")
	}
}

func TestGenerateRecommendations_SortDominatesPipeline(t *testing.T) {
	s := &QueryStats{
		ExecDuration: 100 * time.Millisecond,
		Stages: []StageStats{
			{Name: "Sort", InputRows: 50_000, Duration: 80 * time.Millisecond},
		},
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Sort consuming")
	if found == nil {
		t.Fatal("expected sort dominance recommendation")
	}
}

func TestGenerateRecommendations_MVSuggestion(t *testing.T) {
	s := &QueryStats{
		ScannedRows:   2_000_000,
		Ephemeral:     false,
		AcceleratedBy: "",
		Stages: []StageStats{
			{Name: "Stats", InputRows: 2_000_000, OutputRows: 10},
		},
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "materialized view")
	if found == nil {
		t.Fatal("expected MV suggestion")
	}
}

func TestGenerateRecommendations_SegmentsErrored(t *testing.T) {
	s := &QueryStats{
		SegmentsErrored: 3,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "segment(s) failed")
	if found == nil {
		t.Fatal("expected segment error recommendation")
	}
	if found.Category != "correctness" {
		t.Errorf("category = %q, want correctness", found.Category)
	}
	if found.Priority != "high" {
		t.Errorf("priority = %q, want high", found.Priority)
	}
}

func TestGenerateRecommendations_CacheMiss(t *testing.T) {
	s := &QueryStats{
		ScannedRows: 200_000,
		CacheHit:    false,
		Ephemeral:   false,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "First execution")
	if found == nil {
		t.Fatal("expected cache miss recommendation")
	}
}

func TestGenerateRecommendations_HighMemory(t *testing.T) {
	s := &QueryStats{
		MemAllocBytes: 200 * 1024 * 1024, // 200 MB
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "High memory")
	if found == nil {
		t.Fatal("expected high memory recommendation")
	}
}

func TestGenerateRecommendations_LargeEphemeral(t *testing.T) {
	s := &QueryStats{
		Ephemeral:   true,
		ScannedRows: 2_000_000,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Large dataset in pipe mode")
	if found == nil {
		t.Fatal("expected large ephemeral dataset recommendation")
	}
}

func TestGenerateRecommendations_PartialAggMissing(t *testing.T) {
	s := &QueryStats{
		Ephemeral:      false,
		TotalSegments:  5,
		PartialAggUsed: false,
		Stages: []StageStats{
			{Name: "Stats", InputRows: 100, OutputRows: 5},
		},
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "partial aggregation")
	if found == nil {
		t.Fatal("expected partial aggregation recommendation")
	}
}

func TestGenerateRecommendations_InvertedIndexNotUsed(t *testing.T) {
	s := &QueryStats{
		InvertedIndexHits:    0,
		BloomSkippedSegments: 5,
		Ephemeral:            false,
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "inverted index")
	if found == nil {
		t.Fatal("expected inverted index recommendation")
	}
}

func TestGenerateRecommendations_Empty(t *testing.T) {
	// Minimal stats should produce no recommendations.
	s := &QueryStats{
		Ephemeral:   true,
		ScannedRows: 10,
		ResultRows:  5,
	}

	recs := GenerateRecommendations(s)
	if len(recs) != 0 {
		t.Errorf("expected 0 recommendations, got %d", len(recs))
		for _, r := range recs {
			t.Logf("  [%s] %s", r.Category, r.Message)
		}
	}
}

func TestGenerateRecommendations_HotSegment(t *testing.T) {
	s := &QueryStats{
		ScanDuration: 100 * time.Millisecond,
		SegmentDetails: []SegmentDetail{
			{SegmentID: "seg-001", ReadDuration: 80 * time.Millisecond},
			{SegmentID: "seg-002", ReadDuration: 10 * time.Millisecond},
			{SegmentID: "seg-003", ReadDuration: 10 * time.Millisecond},
		},
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Hot segment")
	if found == nil {
		t.Fatal("expected hot segment recommendation")
	}
	if found.Priority != "medium" {
		t.Errorf("priority = %q, want medium", found.Priority)
	}
	if !strings.Contains(found.Message, "seg-001") {
		t.Errorf("expected message to contain segment ID, got: %s", found.Message)
	}
}

func TestGenerateRecommendations_HotSegment_NotTriggered(t *testing.T) {
	s := &QueryStats{
		ScanDuration: 100 * time.Millisecond,
		SegmentDetails: []SegmentDetail{
			{SegmentID: "seg-001", ReadDuration: 40 * time.Millisecond},
			{SegmentID: "seg-002", ReadDuration: 30 * time.Millisecond},
			{SegmentID: "seg-003", ReadDuration: 30 * time.Millisecond},
		},
	}

	recs := GenerateRecommendations(s)
	found := findRec(recs, "Hot segment")
	if found != nil {
		t.Fatal("should not trigger hot segment recommendation when load is balanced")
	}
}

func TestGenerateRecommendations_SpeedupEstimates(t *testing.T) {
	t.Run("no time range has speedup", func(t *testing.T) {
		s := &QueryStats{
			TotalSegments:       10,
			ScannedSegments:     10,
			TimeSkippedSegments: 0,
		}
		recs := GenerateRecommendations(s)
		found := findRec(recs, "Add --since")
		if found == nil {
			t.Fatal("expected recommendation")
		}
		if found.EstimatedSpeedup != "~2-10x" {
			t.Errorf("speedup = %q, want ~2-10x", found.EstimatedSpeedup)
		}
	})

	t.Run("low selectivity has speedup", func(t *testing.T) {
		s := &QueryStats{
			ScannedRows: 200_000,
			MatchedRows: 100,
		}
		recs := GenerateRecommendations(s)
		found := findRec(recs, "field filter")
		if found == nil {
			t.Fatal("expected recommendation")
		}
		if found.EstimatedSpeedup != "~10x" {
			t.Errorf("speedup = %q, want ~10x", found.EstimatedSpeedup)
		}
	})

	t.Run("MV suggestion has speedup and action", func(t *testing.T) {
		s := &QueryStats{
			ScannedRows: 2_000_000,
			Stages:      []StageStats{{Name: "Stats", InputRows: 2_000_000, OutputRows: 10}},
		}
		recs := GenerateRecommendations(s)
		found := findRec(recs, "materialized view")
		if found == nil {
			t.Fatal("expected recommendation")
		}
		if found.EstimatedSpeedup != "~100-400x" {
			t.Errorf("speedup = %q, want ~100-400x", found.EstimatedSpeedup)
		}
		if found.SuggestedAction == "" {
			t.Error("expected SuggestedAction to be set")
		}
	})
}

func TestGenerateRecommendations_SuggestedActions(t *testing.T) {
	t.Run("segments errored has action", func(t *testing.T) {
		s := &QueryStats{SegmentsErrored: 1}
		recs := GenerateRecommendations(s)
		found := findRec(recs, "segment(s) failed")
		if found == nil {
			t.Fatal("expected recommendation")
		}
		if found.SuggestedAction != "lynxdb doctor" {
			t.Errorf("action = %q, want 'lynxdb doctor'", found.SuggestedAction)
		}
	})

	t.Run("large pipe mode has action", func(t *testing.T) {
		s := &QueryStats{Ephemeral: true, ScannedRows: 2_000_000}
		recs := GenerateRecommendations(s)
		found := findRec(recs, "Large dataset in pipe mode")
		if found == nil {
			t.Fatal("expected recommendation")
		}
		if found.SuggestedAction == "" {
			t.Error("expected SuggestedAction to be set")
		}
	})
}

// findRec searches for a recommendation containing the given substring.
func findRec(recs []Recommendation, substr string) *Recommendation {
	for i := range recs {
		if strings.Contains(recs[i].Message, substr) {
			return &recs[i]
		}
	}

	return nil
}
