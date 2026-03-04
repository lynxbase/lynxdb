package pipeline

import (
	"context"
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
)

// ScanIterator reads events from a pre-loaded slice and yields batches.
type ScanIterator struct {
	events            []*event.Event
	batchSize         int
	offset            int
	columns           []string            // optional column filter (nil = all)
	scanCalls         int                 // for testing: how many Next() calls produced data
	acct              stats.MemoryAccount // per-operator memory tracking (nil *BoundAccount = no tracking)
	lastBatchEstimate int64               // tracks previous batch size for Shrink
}

// NewScanIterator creates a scan iterator over a slice of events.
func NewScanIterator(events []*event.Event, batchSize int) *ScanIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &ScanIterator{
		events:    events,
		batchSize: batchSize,
		acct:      stats.NopAccount(),
	}
}

// NewScanIteratorWithBudget creates a scan iterator with memory budget tracking.
// When the budget is genuinely exceeded (real pressure from downstream operators),
// the scan returns an explicit error instead of silently truncating.
func NewScanIteratorWithBudget(events []*event.Event, batchSize int, acct stats.MemoryAccount) *ScanIterator {
	s := NewScanIterator(events, batchSize)
	s.acct = stats.EnsureAccount(acct)

	return s
}

func (s *ScanIterator) Init(ctx context.Context) error { return nil }

func (s *ScanIterator) Next(ctx context.Context) (*Batch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.offset >= len(s.events) {
		return nil, nil
	}

	// Shrink previous batch — it has been consumed by downstream operator.
	if s.acct != nil && s.lastBatchEstimate > 0 {
		s.acct.Shrink(s.lastBatchEstimate)
		s.lastBatchEstimate = 0
	}

	end := s.offset + s.batchSize
	if end > len(s.events) {
		end = len(s.events)
	}

	// Budget tracking with hard error on genuine budget pressure.
	// After Shrink, the account reflects only the new batch. If Grow still
	// fails, it means real pressure from downstream operators — return an
	// explicit error, never silent truncation.
	if s.acct != nil {
		var estimate int64
		for _, ev := range s.events[s.offset:end] {
			estimate += event.EstimateEventSize(ev)
		}
		if err := s.acct.Grow(estimate); err != nil {
			return nil, fmt.Errorf("query memory limit exceeded at event %d: %w", s.offset, err)
		}
		s.lastBatchEstimate = estimate
	}

	slice := s.events[s.offset:end]
	s.offset = end
	s.scanCalls++

	if s.columns != nil {
		return batchFromEventsFiltered(slice, s.columns), nil
	}

	return BatchFromEvents(slice), nil
}

func (s *ScanIterator) Close() error {
	if s.acct != nil && s.lastBatchEstimate > 0 {
		s.acct.Shrink(s.lastBatchEstimate)
		s.lastBatchEstimate = 0
	}
	s.acct.Close()

	return nil
}

func (s *ScanIterator) Schema() []FieldInfo { return nil }

// ScanCalls returns how many Next() calls produced non-nil batches.
func (s *ScanIterator) ScanCalls() int { return s.scanCalls }

// MemoryUsed returns the current tracked memory for this operator.
func (s *ScanIterator) MemoryUsed() int64 {
	return s.acct.Used()
}

func batchFromEventsFiltered(events []*event.Event, columns []string) *Batch {
	b := NewBatch(len(events))
	colSet := make(map[string]bool, len(columns))
	for _, c := range columns {
		colSet[c] = true
	}
	for _, ev := range events {
		fields := make(map[string]event.Value, len(columns))
		if colSet["_time"] {
			fields["_time"] = event.TimestampValue(ev.Time)
		}
		if colSet["_raw"] {
			fields["_raw"] = event.StringValue(ev.Raw)
		}
		if ev.Source != "" {
			sv := event.StringValue(ev.Source)
			if colSet["_source"] {
				fields["_source"] = sv
			}
			if colSet["source"] {
				fields["source"] = sv
			}
		}
		if ev.SourceType != "" {
			stv := event.StringValue(ev.SourceType)
			if colSet["_sourcetype"] {
				fields["_sourcetype"] = stv
			}
			if colSet["sourcetype"] {
				fields["sourcetype"] = stv
			}
		}
		if colSet["host"] && ev.Host != "" {
			fields["host"] = event.StringValue(ev.Host)
		}
		// "index" is a Splunk-compatible alias for _source, not Event.Index.
		if colSet["index"] && ev.Source != "" {
			sv := event.StringValue(ev.Source)
			fields["index"] = sv
		}
		for k, v := range ev.Fields {
			if colSet[k] {
				fields[k] = v
			}
		}
		b.AddRow(fields)
	}

	return b
}
