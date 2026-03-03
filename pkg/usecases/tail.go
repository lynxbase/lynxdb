package usecases

import (
	"context"
	"fmt"
	"time"

	enginepipeline "github.com/OrlovEvgeny/Lynxdb/pkg/engine/pipeline"
	"github.com/OrlovEvgeny/Lynxdb/pkg/event"
	"github.com/OrlovEvgeny/Lynxdb/pkg/planner"
	"github.com/OrlovEvgeny/Lynxdb/pkg/spl2"
	"github.com/OrlovEvgeny/Lynxdb/pkg/storage"
)

// TailRequest describes a live tail request.
type TailRequest struct {
	Query string // SPL2 query
	Count int    // Historical catchup count (default 100)
	From  string // Catchup lookback (default "-1h")
}

// TailPlan is a validated plan for tail streaming.
type TailPlan struct {
	Program    *spl2.Program
	ExternalTB *spl2.TimeBounds
	Count      int
	RawQuery   string
}

// TailSession holds the state of an active tail session including the EventBus
// subscription, the live pipeline iterator, and the subscription metadata
// needed for drop detection.
type TailSession struct {
	Iter    enginepipeline.Iterator
	SubID   uint64
	Bus     *storage.EventBus
	Cleanup func()
}

// TailService provides live event streaming with full SPL2 pipeline support.
type TailService struct {
	planner planner.Planner
	engine  TailEngine
}

// NewTailService creates a TailService.
func NewTailService(p planner.Planner, engine TailEngine) *TailService {
	return &TailService{planner: p, engine: engine}
}

// Plan parses, optimizes, and validates the query for tail streaming.
func (s *TailService) Plan(req TailRequest) (*TailPlan, error) {
	count := req.Count
	if count <= 0 {
		count = 100
	}
	from := req.From
	if from == "" {
		from = "-1h"
	}

	plan, err := s.planner.Plan(planner.PlanRequest{
		Query: req.Query,
		From:  from,
		To:    "now",
	})
	if err != nil {
		return nil, err
	}

	if err := planner.ValidateForTail(plan.Program); err != nil {
		return nil, err
	}

	return &TailPlan{
		Program:    plan.Program,
		ExternalTB: plan.ExternalTimeBounds,
		Count:      count,
		RawQuery:   plan.RawQuery,
	}, nil
}

// SubscribeAndCatchup subscribes to the EventBus FIRST, then executes the
// catchup query against stored data. This ordering guarantees that no events
// are lost between the catchup snapshot and live streaming: any event ingested
// during catchup is captured by the subscription channel.
//
// Returns the catchup rows (last N in chronological order) and a TailSession
// for the live phase. The TailSession's iterator has a dedup cursor set to
// the latest catchup timestamp so events already returned in catchup are not
// duplicated in the live stream.
//
// The caller MUST call session.Cleanup() when done.
func (s *TailService) SubscribeAndCatchup(ctx context.Context, plan *TailPlan) ([]map[string]event.Value, *TailSession, error) {
	bus := s.engine.EventBus()

	// Subscribe BEFORE reading storage — guarantees no gap.
	subID, ch, err := bus.Subscribe()
	if err != nil {
		return nil, nil, fmt.Errorf("tail: subscribe: %w", err)
	}

	// Run catchup against stored data using a streaming ring buffer
	// to bound memory to O(count) instead of O(total_events).
	rows, err := s.catchupRing(ctx, plan)
	if err != nil {
		bus.Unsubscribe(subID)

		return nil, nil, err
	}

	// Determine the dedup cursor — the latest _time from catchup.
	// Events at or before this timestamp will be skipped in the live stream.
	var latestCatchup time.Time
	if len(rows) > 0 {
		last := rows[len(rows)-1]
		if tv, ok := last["_time"]; ok {
			if t, valid := tv.TryAsTimestamp(); valid {
				latestCatchup = t
			}
		}
	}

	// Build the live pipeline with the dedup cursor.
	var commands []spl2.Command
	if plan.Program.Main != nil {
		commands = plan.Program.Main.Commands
	}

	source := enginepipeline.NewLiveScanIterator(ch, 64, 100*time.Millisecond)
	if !latestCatchup.IsZero() {
		source.SetSkipBefore(latestCatchup)
	}

	iter, err := enginepipeline.BuildFromSource(ctx, source, commands, enginepipeline.DefaultBatchSize)
	if err != nil {
		bus.Unsubscribe(subID)

		return nil, nil, err
	}

	if err := iter.Init(ctx); err != nil {
		bus.Unsubscribe(subID)

		return nil, nil, err
	}

	session := &TailSession{
		Iter:  iter,
		SubID: subID,
		Bus:   bus,
		Cleanup: func() {
			iter.Close()
			bus.Unsubscribe(subID)
		},
	}

	return rows, session, nil
}

// catchupRing executes the catchup query using a streaming ring buffer that
// keeps only the last N rows in memory, bounding peak allocation to O(count)
// instead of O(total_matching_events).
func (s *TailService) catchupRing(ctx context.Context, plan *TailPlan) ([]map[string]event.Value, error) {
	iter, _, err := s.engine.BuildStreamingPipeline(ctx, plan.Program, plan.ExternalTB)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	capacity := plan.Count
	if capacity <= 0 {
		capacity = 100
	}

	ring := make([]map[string]event.Value, 0, capacity)
	pos := 0 // write position in ring when full
	full := false

	for {
		batch, err := iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			if !full {
				ring = append(ring, row)
				if len(ring) == capacity {
					full = true
					pos = 0
				}
			} else {
				ring[pos] = row
				pos = (pos + 1) % capacity
			}
		}
	}

	if !full {
		// Fewer rows than capacity — already in chronological order.
		return ring, nil
	}

	// Linearize: ring[pos:] + ring[:pos] gives chronological order.
	result := make([]map[string]event.Value, capacity)
	copy(result, ring[pos:])
	copy(result[capacity-pos:], ring[:pos])

	return result, nil
}
