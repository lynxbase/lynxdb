package pipeline

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// SessionizeIterator groups events into sessions based on time gaps.
// When consecutive events by a group key exceed maxpause, a new session starts.
// Adds _session_id, _session_start, _session_end fields to each row.
type SessionizeIterator struct {
	child     Iterator
	maxPause  time.Duration
	groupBy   []string
	batchSize int

	done   bool
	output []map[string]event.Value
	offset int
}

// NewSessionizeIterator creates a sessionize operator.
func NewSessionizeIterator(child Iterator, maxPause string, groupBy []string, batchSize int) *SessionizeIterator {
	dur := parseDuration(maxPause)
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &SessionizeIterator{
		child:     child,
		maxPause:  dur,
		groupBy:   groupBy,
		batchSize: batchSize,
	}
}

func (s *SessionizeIterator) Init(ctx context.Context) error {
	return s.child.Init(ctx)
}

func (s *SessionizeIterator) Next(ctx context.Context) (*Batch, error) {
	if !s.done {
		if err := s.materialize(ctx); err != nil {
			return nil, err
		}
		s.done = true
	}
	if s.output == nil || s.offset >= len(s.output) {
		return nil, nil
	}
	end := s.offset + s.batchSize
	if end > len(s.output) {
		end = len(s.output)
	}
	batch := BatchFromRows(s.output[s.offset:end])
	s.offset = end

	return batch, nil
}

func (s *SessionizeIterator) materialize(ctx context.Context) error {
	// Drain child.
	var rows []map[string]event.Value
	for {
		batch, err := s.child.Next(ctx)
		if batch == nil {
			break
		}
		if err != nil {
			return err
		}
		for i := 0; i < batch.Len; i++ {
			rows = append(rows, batch.Row(i))
		}
	}

	if len(rows) == 0 {
		return nil
	}

	// Sort by group keys + time.
	sort.Slice(rows, func(i, j int) bool {
		for _, f := range s.groupBy {
			vi := ""
			if v, ok := rows[i][f]; ok {
				vi = v.String()
			}
			vj := ""
			if v, ok := rows[j][f]; ok {
				vj = v.String()
			}
			if vi != vj {
				return vi < vj
			}
		}

		return getTime(rows[i]).Before(getTime(rows[j]))
	})

	// Assign sessions.
	sessionID := int64(0)
	var lastGroup string
	var lastTime time.Time

	for _, row := range rows {
		// Compute group key.
		groupKey := ""
		for _, f := range s.groupBy {
			if v, ok := row[f]; ok {
				groupKey += "|" + v.String()
			}
		}

		t := getTime(row)

		// New session if group changed or gap exceeded.
		if groupKey != lastGroup || (lastTime != (time.Time{}) && t.Sub(lastTime) > s.maxPause) {
			sessionID++
			row["_session_start"] = event.TimestampValue(t)
		} else if _, ok := row["_session_start"]; !ok {
			row["_session_start"] = event.TimestampValue(t)
		}

		row["_session_id"] = event.IntValue(sessionID)
		row["_session_end"] = event.TimestampValue(t)

		lastGroup = groupKey
		lastTime = t
	}

	// Second pass: set correct _session_end for all rows in a session.
	sessionEnds := make(map[int64]time.Time)
	for i := len(rows) - 1; i >= 0; i-- {
		sid := rows[i]["_session_id"].AsInt()
		if _, ok := sessionEnds[sid]; !ok {
			sessionEnds[sid] = getTime(rows[i])
		}
		rows[i]["_session_end"] = event.TimestampValue(sessionEnds[sid])
	}

	s.output = rows

	return nil
}

func (s *SessionizeIterator) Close() error {
	return s.child.Close()
}

// MemoryUsed returns 0 — sessionize uses the child's memory budget.
func (s *SessionizeIterator) MemoryUsed() int64 { return 0 }

func (s *SessionizeIterator) Schema() []FieldInfo {
	return append(s.child.Schema(),
		FieldInfo{Name: "_session_id", Type: "int"},
		FieldInfo{Name: "_session_start", Type: "timestamp"},
		FieldInfo{Name: "_session_end", Type: "timestamp"},
	)
}

// String returns a debug representation.
func (s *SessionizeIterator) String() string {
	if len(s.groupBy) > 0 {
		return fmt.Sprintf("Sessionize(maxpause=%s, by=%v)", s.maxPause, s.groupBy)
	}

	return fmt.Sprintf("Sessionize(maxpause=%s)", s.maxPause)
}
