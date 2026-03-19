package segment

import "github.com/lynxbase/lynxdb/pkg/event"

// PrefetchReader wraps a Reader with row group prefetching.
// While processing row group N, it prefetches row group N+1 in background.
type PrefetchReader struct {
	reader *Reader
}

// NewPrefetchReader creates a prefetching segment reader.
func NewPrefetchReader(reader *Reader) *PrefetchReader {
	return &PrefetchReader{
		reader: reader,
	}
}

// prefetchResult holds the result of a prefetched row group read.
type prefetchResult struct {
	events []*event.Event
	err    error
}

// ReadEventsWithPrefetch reads events using prefetching for sequential scans.
func (pr *PrefetchReader) ReadEventsWithPrefetch(hints QueryHints) ([]*event.Event, error) {
	rgCount := pr.reader.RowGroupCount()
	if rgCount <= 1 {
		// No prefetching benefit for single row group.
		return pr.reader.ReadEventsWithHints(hints)
	}

	need := make(map[string]bool)
	for _, c := range hints.Columns {
		need[c] = true
	}

	// Pre-compute bloom eligibility.
	var bloomEligible map[int]bool
	if len(hints.SearchTerms) > 0 {
		eligible, err := pr.reader.CheckBloomAllTermsForRowGroups(hints.SearchTerms)
		if err == nil {
			bloomEligible = make(map[int]bool, len(eligible))
			for _, idx := range eligible {
				bloomEligible[idx] = true
			}
		}
	}

	var allEvents []*event.Event
	var prefetchCh chan prefetchResult

	for rgi := 0; rgi < rgCount; rgi++ {
		rg := &pr.reader.footer.RowGroups[rgi]

		// Zone map pruning.
		if hints.MinTime != nil || hints.MaxTime != nil {
			if pr.reader.canPruneRowGroup(rg, hints.MinTime, hints.MaxTime) {
				continue
			}
		}

		// Bloom filter pruning.
		if bloomEligible != nil && !bloomEligible[rgi] {
			continue
		}

		// Start prefetch for next row group.
		if rgi+1 < rgCount {
			nextRGI := rgi + 1
			prefetchCh = make(chan prefetchResult, 1)
			go func(idx int) {
				nextRG := &pr.reader.footer.RowGroups[idx]
				// Check if next RG should be pruned.
				if hints.MinTime != nil || hints.MaxTime != nil {
					if pr.reader.canPruneRowGroup(nextRG, hints.MinTime, hints.MaxTime) {
						prefetchCh <- prefetchResult{}

						return
					}
				}
				if bloomEligible != nil && !bloomEligible[idx] {
					prefetchCh <- prefetchResult{}

					return
				}
				var events []*event.Event
				var err error
				if len(need) > 0 {
					events, err = pr.reader.readRowGroupEventsProjected(idx, nextRG, need)
				} else {
					events, err = pr.reader.readRowGroupEvents(idx, nextRG)
				}
				prefetchCh <- prefetchResult{events: events, err: err}
			}(nextRGI)
		}

		// Read current row group (or use prefetched result).
		var rgEvents []*event.Event
		var err error
		if len(need) > 0 {
			rgEvents, err = pr.reader.readRowGroupEventsProjected(rgi, rg, need)
		} else {
			rgEvents, err = pr.reader.readRowGroupEvents(rgi, rg)
		}
		if err != nil {
			return nil, err
		}
		allEvents = append(allEvents, rgEvents...)

		// If we prefetched the next RG, skip it in the loop.
		if prefetchCh != nil && rgi+1 < rgCount {
			result := <-prefetchCh
			prefetchCh = nil
			if result.err != nil {
				return nil, result.err
			}
			if result.events != nil {
				allEvents = append(allEvents, result.events...)
			}
			rgi++ // skip next since we already read it
		}
	}

	return allEvents, nil
}
