package server

import (
	"fmt"

	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
)

// zeroResultSuggestions generates actionable suggestions when a query returns
// zero results. For schema-on-read log analytics, this is critical UX: users
// frequently query fields that don't exist or have different names than expected.
//
// The function examines the query's field predicates and segment metadata to
// determine why zero results were returned and suggests alternatives.
func zeroResultSuggestions(hints *spl2.QueryHints, ss storeStats) []string {
	if hints == nil {
		return nil
	}

	var suggestions []string

	// Check field equality predicates: if the user searched field=value and
	// got zero results, suggest searching _raw for the value instead.
	for _, fp := range hints.FieldPredicates {
		if fp.Op != "=" {
			continue
		}

		// If we scanned segments but found nothing, the field may not exist
		// or the value may not be present in that field.
		if ss.SegmentsScanned > 0 {
			suggestions = append(suggestions,
				fmt.Sprintf("Field %q=%q matched no events. Try: search %q (searches raw log text)",
					fp.Field, fp.Value, fp.Value))
		}
	}

	// If the entire query had no segments scanned (all skipped by time/index),
	// suggest broadening the time range.
	if ss.SegmentsTotal > 0 && ss.SegmentsScanned == 0 {
		skippedTime := ss.SegmentsSkippedTime
		if skippedTime > 0 {
			suggestions = append(suggestions,
				fmt.Sprintf("All %d segments were outside the query time range. Try a broader time window.",
					skippedTime))
		}
	}

	// If search terms were used and all segments were bloom-skipped, suggest
	// verifying the search term.
	if ss.SegmentsTotal > 0 && ss.SegmentsSkippedBF == ss.SegmentsTotal {
		suggestions = append(suggestions,
			"All segments were skipped by bloom filter — the search term does not appear in any segment.")
	}

	return suggestions
}
