package stats

import (
	"fmt"
	"time"
)

// GenerateRecommendations analyzes query statistics and returns actionable
// suggestions for improving query performance, correctness, or cost.
func GenerateRecommendations(s *QueryStats) []Recommendation {
	var recs []Recommendation

	rules := []func(*QueryStats) *Recommendation{
		ruleNoTimeRange,
		ruleLowSelectivity,
		ruleNoBloomPruning,
		ruleLowSkipRate,
		ruleSortDominates,
		ruleNoMVAcceleration,
		ruleSegmentsErrored,
		ruleCacheMiss,
		ruleHighMemory,
		ruleLargePipeMode,
		ruleNoPartialAgg,
		ruleBloomWithoutInverted,
		ruleHotSegment,
	}

	for _, rule := range rules {
		if r := rule(s); r != nil {
			recs = append(recs, *r)
		}
	}

	return recs
}

func ruleNoTimeRange(s *QueryStats) *Recommendation {
	if s.TotalSegments > 0 && s.TimeSkippedSegments == 0 && !s.Ephemeral {
		return &Recommendation{
			Category:         "performance",
			Priority:         "medium",
			Message:          "Add --since or --from/--to to narrow the time range and skip segments",
			EstimatedSpeedup: "~2-10x",
		}
	}

	return nil
}

func ruleLowSelectivity(s *QueryStats) *Recommendation {
	if s.ScannedRows > 100_000 && s.Selectivity() > 0 && s.Selectivity() < 0.01 {
		return &Recommendation{
			Category:         "performance",
			Priority:         "medium",
			Message:          fmt.Sprintf("Search matched <%.1f%% of rows; add a field filter for index-accelerated filtering", s.Selectivity()*100),
			EstimatedSpeedup: "~10x",
		}
	}

	return nil
}

func ruleNoBloomPruning(s *QueryStats) *Recommendation {
	if s.BloomSkippedSegments == 0 && s.TotalSegments > 5 && !s.Ephemeral {
		return &Recommendation{
			Category: "performance",
			Priority: "low",
			Message:  "No bloom filter pruning; add specific search terms to enable segment skip",
		}
	}

	return nil
}

func ruleLowSkipRate(s *QueryStats) *Recommendation {
	if s.ScannedSegments > 10 && s.SkipPercent() < 20 && !s.Ephemeral {
		return &Recommendation{
			Category: "performance",
			Priority: "medium",
			Message:  "Most segments scanned; narrow time range or add indexed term filter",
		}
	}

	return nil
}

func ruleSortDominates(s *QueryStats) *Recommendation {
	if len(s.Stages) == 0 || s.ExecDuration <= 0 {
		return nil
	}

	for _, stage := range s.Stages {
		if stage.Name == "Sort" && stage.InputRows > 10_000 {
			sortPct := float64(stage.Duration) / float64(s.ExecDuration) * 100
			if sortPct > 50 {
				return &Recommendation{
					Category:         "performance",
					Priority:         "medium",
					Message:          fmt.Sprintf("Sort consuming %.0f%% of pipeline time; consider adding head N or using TopN", sortPct),
					EstimatedSpeedup: fmt.Sprintf("~%.0fx", sortPct/10),
				}
			}
		}
	}

	return nil
}

func ruleNoMVAcceleration(s *QueryStats) *Recommendation {
	if s.AcceleratedBy == "" && s.ScannedRows > 1_000_000 && !s.Ephemeral && hasStatsStage(s) {
		return &Recommendation{
			Category:         "performance",
			Priority:         "info",
			Message:          "Consider creating a materialized view for this aggregation query",
			EstimatedSpeedup: "~100-400x",
			SuggestedAction:  "lynxdb mv create <name> '<query>'",
		}
	}

	return nil
}

func ruleSegmentsErrored(s *QueryStats) *Recommendation {
	if s.SegmentsErrored > 0 {
		return &Recommendation{
			Category:        "correctness",
			Priority:        "high",
			Message:         fmt.Sprintf("%d segment(s) failed to read; run 'lynxdb doctor' to check storage health", s.SegmentsErrored),
			SuggestedAction: "lynxdb doctor",
		}
	}

	return nil
}

func ruleCacheMiss(s *QueryStats) *Recommendation {
	if !s.CacheHit && s.ScannedRows > 100_000 && !s.Ephemeral {
		return &Recommendation{
			Category: "info",
			Priority: "low",
			Message:  "First execution; subsequent runs may hit query cache",
		}
	}

	return nil
}

func ruleHighMemory(s *QueryStats) *Recommendation {
	if s.MemAllocBytes > 100*1024*1024 {
		return &Recommendation{
			Category: "performance",
			Priority: "low",
			Message: fmt.Sprintf("High memory usage (%s allocated); consider streaming with --format ndjson",
				formatBytesHuman(s.MemAllocBytes)),
		}
	}

	return nil
}

// 10. Large dataset in pipe mode.
func ruleLargePipeMode(s *QueryStats) *Recommendation {
	if s.Ephemeral && s.ScannedRows > 1_000_000 {
		return &Recommendation{
			Category:        "performance",
			Priority:        "info",
			Message:         "Large dataset in pipe mode; consider ingesting to server for indexed queries",
			SuggestedAction: "lynxdb ingest <file> && lynxdb query '<query>'",
		}
	}

	return nil
}

// 11. Stats without partial aggregation.
func ruleNoPartialAgg(s *QueryStats) *Recommendation {
	if hasStatsStage(s) && !s.PartialAggUsed && !s.Ephemeral && s.TotalSegments > 3 {
		return &Recommendation{
			Category: "performance",
			Priority: "info",
			Message:  "Query may benefit from partial aggregation pushdown",
		}
	}

	return nil
}

// 12. Bloom active but inverted index not used.
func ruleBloomWithoutInverted(s *QueryStats) *Recommendation {
	if s.InvertedIndexHits == 0 && s.BloomSkippedSegments > 0 && !s.Ephemeral {
		return &Recommendation{
			Category: "performance",
			Priority: "low",
			Message:  "Bloom filters active but inverted index not used; field=value filters may leverage inverted index",
		}
	}

	return nil
}

// 13. Hot segment detected — one segment took >50% of total scan time (trace only).
func ruleHotSegment(s *QueryStats) *Recommendation {
	if len(s.SegmentDetails) < 2 || s.ScanDuration <= 0 {
		return nil
	}

	var hotSeg SegmentDetail
	var maxDur time.Duration

	for _, sd := range s.SegmentDetails {
		if sd.ReadDuration > maxDur {
			maxDur = sd.ReadDuration
			hotSeg = sd
		}
	}

	hotPct := float64(maxDur) / float64(s.ScanDuration) * 100
	if hotPct > 50 {
		return &Recommendation{
			Category: "performance",
			Priority: "medium",
			Message: fmt.Sprintf("Hot segment %s took %.0f%% of scan time (%s); check for skew or cold-tier latency",
				hotSeg.SegmentID, hotPct, formatDur(maxDur)),
		}
	}

	return nil
}

// hasStatsStage returns true if the pipeline includes a Stats/Aggregate stage.
func hasStatsStage(s *QueryStats) bool {
	for _, stage := range s.Stages {
		if stage.Name == "Stats" || stage.Name == "Aggregate" || stage.Name == "PartialAggregate" {
			return true
		}
	}

	return false
}
