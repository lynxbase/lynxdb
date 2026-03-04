package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/stats"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// EventStatsIterator implements two-pass aggregation: accumulate globals, then enrich rows.
//
// When a SpillManager is configured, the iterator spills rows to disk when the
// memory budget is exceeded. Aggregation group state stays in memory (it is
// typically much smaller than the row set). During pass 2, rows are read back
// from disk and enriched with aggregation results.
type EventStatsIterator struct {
	child     Iterator
	aggs      []AggFunc
	groupBy   []string
	rows      []map[string]event.Value // in-memory rows (before spill or if small enough)
	emitted   bool
	batchSize int
	offset    int
	acct      stats.MemoryAccount // per-operator memory tracking (nil *BoundAccount = no tracking)

	// Spill-to-disk support.
	spillMgr        *SpillManager        // lifecycle manager for spill files (nil = no spill support)
	spillWriter     *ColumnarSpillWriter // writes rows to disk during pass 1 (non-nil after spill)
	spillPath       string               // path to spill file (for cleanup)
	spillReader     *ColumnarSpillReader // reads rows back during pass 2
	spilled         bool                 // true after rows started going to disk
	spilledRows     int64                // total rows written to spill file
	spillBytesTotal int64                // persisted spill bytes (survives Close)
	groups          map[string]*aggGroup // agg state (stays in memory in both modes)
}

// NewEventStatsIterator creates a two-pass eventstats operator.
func NewEventStatsIterator(child Iterator, aggs []AggFunc, groupBy []string, batchSize int) *EventStatsIterator {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &EventStatsIterator{
		child:     child,
		aggs:      aggs,
		groupBy:   groupBy,
		batchSize: batchSize,
		acct:      stats.NopAccount(),
	}
}

// NewEventStatsIteratorWithBudget creates an eventstats operator with memory budget tracking.
func NewEventStatsIteratorWithBudget(child Iterator, aggs []AggFunc, groupBy []string, batchSize int, acct stats.MemoryAccount) *EventStatsIterator {
	e := NewEventStatsIterator(child, aggs, groupBy, batchSize)
	e.acct = stats.EnsureAccount(acct)

	return e
}

// newEventStatsIteratorWithSpill creates an eventstats operator with memory budget
// tracking and disk spill support. When the in-memory row buffer exceeds the
// budget, rows are written to a spill file while aggregation state stays in
// memory. During pass 2, rows are read back from disk and enriched.
func newEventStatsIteratorWithSpill(child Iterator, aggs []AggFunc, groupBy []string, batchSize int, acct stats.MemoryAccount, mgr *SpillManager) *EventStatsIterator {
	e := NewEventStatsIteratorWithBudget(child, aggs, groupBy, batchSize, acct)
	e.spillMgr = mgr

	return e
}

// Init delegates to the child iterator.
func (e *EventStatsIterator) Init(ctx context.Context) error {
	return e.child.Init(ctx)
}

// Next returns the next batch of enriched rows. On first call, materializes
// all input (pass 1: accumulate aggregates, buffer/spill rows), then emits
// enriched rows in batches (pass 2).
func (e *EventStatsIterator) Next(ctx context.Context) (*Batch, error) {
	if !e.emitted {
		if err := e.materialize(ctx); err != nil {
			return nil, err
		}
	}

	if e.spilled {
		return e.nextFromSpill()
	}

	// In-memory path.
	if e.offset >= len(e.rows) {
		return nil, nil
	}
	end := e.offset + e.batchSize
	if end > len(e.rows) {
		end = len(e.rows)
	}
	batch := BatchFromRows(e.rows[e.offset:end])
	e.offset = end

	return batch, nil
}

// Close releases resources: memory budget account, spill writer/reader, and spill file.
func (e *EventStatsIterator) Close() error {
	e.acct.Close()

	// Close spill writer if still open (e.g., materialize() returned an error
	// after transitionToSpill succeeded but before CloseFile was called).
	if e.spillWriter != nil {
		_ = e.spillWriter.CloseFile()
		e.spillWriter = nil
	}

	if e.spillReader != nil {
		e.spillReader.Close()
		e.spillReader = nil
	}
	if e.spillPath != "" {
		// Persist spill bytes before cleanup (ResourceStats may be called after Close).
		if info, err := os.Stat(e.spillPath); err == nil {
			e.spillBytesTotal = info.Size()
		}
		if e.spillMgr != nil {
			e.spillMgr.Release(e.spillPath)
		} else {
			os.Remove(e.spillPath)
		}
		e.spillPath = ""
	}

	return e.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (e *EventStatsIterator) MemoryUsed() int64 {
	return e.acct.Used()
}

// ResourceStats implements ResourceReporter for per-operator spill metrics.
// Safe to call after Close() — spill metrics are persisted before cleanup.
func (e *EventStatsIterator) ResourceStats() OperatorResourceStats {
	spillBytes := e.spillBytesTotal
	if e.spillPath != "" {
		if info, err := os.Stat(e.spillPath); err == nil {
			spillBytes = info.Size()
		}
	}

	return OperatorResourceStats{
		PeakBytes:   e.acct.MaxUsed(),
		SpilledRows: e.spilledRows,
		SpillBytes:  spillBytes,
	}
}

// Schema delegates to the child iterator.
func (e *EventStatsIterator) Schema() []FieldInfo { return e.child.Schema() }

func (e *EventStatsIterator) materialize(ctx context.Context) error {
	// Pass 1: collect all rows and compute aggregates.
	groups := make(map[string]*aggGroup)
	e.groups = groups

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := e.child.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}

		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)

			// Store the row: in-memory or on disk.
			if e.spilled {
				// Disk mode: write directly to spill file.
				if writeErr := e.spillWriter.WriteRow(row); writeErr != nil {
					return fmt.Errorf("eventstats.materialize: write spill: %w", writeErr)
				}
				e.spilledRows++
			} else {
				// In-memory mode: try to grow budget.
				if growErr := e.acct.Grow(estimatedRowBytes); growErr != nil {
					// Budget exceeded — try to spill if SpillManager is available.
					if e.spillMgr != nil {
						if spillErr := e.transitionToSpill(row); spillErr != nil {
							return fmt.Errorf("eventstats.materialize: %w", spillErr)
						}
						// Row was already written to disk by transitionToSpill.
					} else {
						return fmt.Errorf("eventstats.materialize: %w", growErr)
					}
				} else {
					e.rows = append(e.rows, row)
				}
			}

			// Update aggregation state (same for both paths).
			key := e.groupKey(row)
			group, ok := groups[key]
			if !ok {
				// Track memory for new group.
				groupBytes := int64(len(e.groupBy))*estimatedKeyBytes + int64(len(e.aggs))*estimatedAggStateBytes
				if growErr := e.acct.Grow(groupBytes); growErr != nil {
					// Groups must fit in memory — cannot spill aggregation state.
					return fmt.Errorf("eventstats.materialize: too many groups: %w", growErr)
				}
				group = &aggGroup{
					key:    e.extractGroupKey(row),
					states: make([]aggState, len(e.aggs)),
				}
				for j := range group.states {
					group.states[j].min = event.NullValue()
					group.states[j].max = event.NullValue()
					group.states[j].first = event.NullValue()
					group.states[j].last = event.NullValue()
					group.states[j].values = make(map[string]bool)
				}
				groups[key] = group
			}
			for j, agg := range e.aggs {
				val := e.extractValue(agg, row)
				updateAggState(&group.states[j], agg.Name, val)
			}
		}
	}

	// Close spill writer handle so it can be read back in pass 2.
	if e.spilled && e.spillWriter != nil {
		if err := e.spillWriter.CloseFile(); err != nil {
			return fmt.Errorf("eventstats.materialize: close spill: %w", err)
		}
	}

	// Pass 2: enrich rows with aggregated values.
	if !e.spilled {
		// In-memory path: enrich all rows in place.
		for _, row := range e.rows {
			key := e.groupKey(row)
			group := groups[key]
			for j, agg := range e.aggs {
				row[agg.Alias] = finalizeAggState(&group.states[j], agg.Name)
			}
		}
	}
	// If spilled, pass 2 enrichment happens during Next() via nextFromSpill().

	e.emitted = true

	return nil
}

// transitionToSpill moves from in-memory to disk mode. All buffered in-memory
// rows are written to a spill file, followed by the current row that triggered
// the transition. The in-memory row slice is cleared and row-tracking memory
// is released back to the budget.
func (e *EventStatsIterator) transitionToSpill(currentRow map[string]event.Value) error {
	sw, err := NewColumnarSpillWriter(e.spillMgr, "eventstats")
	if err != nil {
		return fmt.Errorf("create spill file: %w", err)
	}

	// Set spillPath and spillWriter BEFORE writing so that Close() can always
	// release the file handle and spill path even if a write error occurs below.
	e.spillWriter = sw
	e.spillPath = sw.Path()
	e.spilled = true

	// Write all buffered in-memory rows to disk.
	for _, row := range e.rows {
		if writeErr := sw.WriteRow(row); writeErr != nil {
			return fmt.Errorf("write buffered row: %w", writeErr)
		}
	}
	e.spilledRows = int64(len(e.rows))

	// Write the current row that triggered the spill.
	if writeErr := sw.WriteRow(currentRow); writeErr != nil {
		return fmt.Errorf("write current row: %w", writeErr)
	}
	e.spilledRows++

	// Release row memory from budget (group memory stays tracked).
	rowMem := int64(len(e.rows)) * estimatedRowBytes
	e.acct.Shrink(rowMem)

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := e.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	// Clear in-memory rows.
	e.rows = nil

	return nil
}

// nextFromSpill reads rows back from the spill file, enriches each with
// aggregation results, and returns them as batches.
func (e *EventStatsIterator) nextFromSpill() (*Batch, error) {
	// Lazy-open the reader on first call.
	if e.spillReader == nil {
		r, err := NewColumnarSpillReader(e.spillPath)
		if err != nil {
			return nil, fmt.Errorf("eventstats: open spill reader: %w", err)
		}
		e.spillReader = r
	}

	batch := NewBatch(e.batchSize)
	for batch.Len < e.batchSize {
		row, err := e.spillReader.ReadRow()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("eventstats: read spill: %w", err)
		}

		// Enrich with aggregation results.
		key := e.groupKey(row)
		if group, ok := e.groups[key]; ok {
			for j, agg := range e.aggs {
				row[agg.Alias] = finalizeAggState(&group.states[j], agg.Name)
			}
		}
		batch.AddRow(row)
	}

	if batch.Len == 0 {
		return nil, nil
	}

	return batch, nil
}

// groupKey builds a composite key from the BY-clause fields of a row.
//
// Uses null byte (\x00) as separator instead of '|' to avoid collisions when
// field values contain the separator character. Each field is prefixed with a
// presence marker: \x01 for present values, \x00 for null/missing. This
// ensures that ("a", null) and (null, "a") produce distinct keys, and that
// values like "x|y" in a single field don't collide with "x" and "y" in two
// separate fields.
func (e *EventStatsIterator) groupKey(row map[string]event.Value) string {
	if len(e.groupBy) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, g := range e.groupBy {
		if i > 0 {
			sb.WriteByte(0) // null byte separator
		}
		v, ok := row[g]
		if !ok || v.IsNull() {
			sb.WriteByte(0) // null/missing marker
		} else {
			sb.WriteByte(1) // present marker
			sb.WriteString(v.String())
		}
	}

	return sb.String()
}

func (e *EventStatsIterator) extractGroupKey(row map[string]event.Value) map[string]event.Value {
	key := make(map[string]event.Value, len(e.groupBy))
	for _, g := range e.groupBy {
		if v, ok := row[g]; ok {
			key[g] = v
		} else {
			key[g] = event.NullValue()
		}
	}

	return key
}

func (e *EventStatsIterator) extractValue(agg AggFunc, row map[string]event.Value) event.Value {
	if agg.Field == "" {
		return event.IntValue(1)
	}
	if v, ok := row[agg.Field]; ok {
		return v
	}

	return event.NullValue()
}

func updateAggState(s *aggState, fn string, val event.Value) {
	switch strings.ToLower(fn) {
	case aggCount:
		if !val.IsNull() {
			s.count++
		}
	case aggSum:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
		}
	case aggAvg:
		if f, ok := vm.ValueToFloat(val); ok {
			s.sum += f
			s.count++
		}
	case aggMin:
		if !val.IsNull() && (s.min.IsNull() || vm.CompareValues(val, s.min) < 0) {
			s.min = val
		}
	case aggMax:
		if !val.IsNull() && (s.max.IsNull() || vm.CompareValues(val, s.max) > 0) {
			s.max = val
		}
	case "dc":
		if !val.IsNull() {
			s.values[val.String()] = true
		}
	}
}

func finalizeAggState(s *aggState, fn string) event.Value {
	switch strings.ToLower(fn) {
	case aggCount:
		return event.IntValue(s.count)
	case aggSum:
		return event.FloatValue(s.sum)
	case aggAvg:
		if s.count == 0 {
			return event.NullValue()
		}

		return event.FloatValue(s.sum / float64(s.count))
	case aggMin:
		return s.min
	case aggMax:
		return s.max
	case "dc":
		return event.IntValue(int64(len(s.values)))
	}

	return event.NullValue()
}
