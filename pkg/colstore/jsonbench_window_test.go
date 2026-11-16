package colstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// timeWindow is an inclusive range of row timestamps in nanoseconds.
type timeWindow struct{ lo, hi int64 }

type windowWorker struct {
	times, col, other vec.Column
	mask              []uint8
	hits              []uint32
	marks             []bool
	count             int64
	groups            map[string]int64
	latest            []timedEvent
}

type timedEvent struct {
	time       int64
	collection string
}

// timeMask returns which rows of b lie in the window, or nil when all do.
func (w *windowWorker) timeMask(p *part.Part, b *part.Block, win timeWindow) ([]uint8, error) {
	if b.MinTime >= win.lo && b.MaxTime <= win.hi {
		return nil, nil
	}
	if _, err := column(p, b, part.TimePath, vec.KindInt, &w.times); err != nil {
		return nil, err
	}
	if cap(w.mask) < b.Rows {
		w.mask = make([]uint8, b.Rows)
	}
	w.mask = w.mask[:b.Rows]
	if w.times.Layout == vec.Const {
		fill := uint8(0)
		if t := w.times.Int(0); t >= win.lo && t <= win.hi {
			fill = 0xFF
		}
		for i := range w.mask {
			w.mask[i] = fill
		}
	} else {
		vec.MaskRangeInt64(w.mask, w.times.Ints[:b.Rows], win.lo, win.hi)
	}

	return w.mask, nil
}

// windowStats is what the latest window query's scan read and skipped, and
// windowNote what it wants printed with its result.
var (
	windowStats ScanStats
	windowNote  string
)

func scanWindow(snap *Snapshot, workers int, win timeWindow, tokens []uint64, fn func(w *windowWorker, p *part.Part, b *part.Block) error) ([]windowWorker, error) {
	ws := make([]windowWorker, workers)
	stats, err := snap.Scan(context.Background(), ScanOptions{Workers: workers, MinTime: win.lo, MaxTime: win.hi, Tokens: tokens},
		func(n int, p *part.Part, b *part.Block) error { return fn(&ws[n], p, b) })
	windowStats = stats

	return ws, err
}

// queryWindowCount counts the rows in the window, and of those only the rows
// whose string at path holds pat when a path is given.
func queryWindowCount(snap *Snapshot, workers int, win timeWindow, path string, pat tokenfilter.Pattern) (int64, error) {
	var tokens []uint64
	if path != "" {
		tokens = pat.Tokens(nil)
	}
	ws, err := scanWindow(snap, workers, win, tokens, func(w *windowWorker, p *part.Part, b *part.Block) error {
		mask, err := w.timeMask(p, b, win)
		if err != nil {
			return err
		}
		if path == "" {
			if mask == nil {
				w.count += int64(b.Rows)
			} else {
				w.count += int64(vec.MaskCount(mask))
			}

			return nil
		}
		ref, err := column(p, b, path, vec.KindString, &w.col)
		if err != nil || ref == nil {
			return err
		}
		c := &w.col
		in := func(row int) bool { return (mask == nil || mask[row] != 0) && c.IsValid(row) }
		switch c.Layout {
		case vec.Const:
			if !pat.Match(c.DictValue(0)) {
				return nil
			}
			for row := 0; row < b.Rows; row++ {
				if in(row) {
					w.count++
				}
			}
		case vec.Dict:
			w.hits = pat.AppendMatches(w.hits[:0], c.Offsets[:c.DictLen()+1], c.Data)
			if len(w.hits) == 0 {
				return nil
			}
			if cap(w.marks) < c.DictLen() {
				w.marks = make([]bool, c.DictLen())
			}
			w.marks = w.marks[:c.DictLen()]
			clear(w.marks)
			for _, code := range w.hits {
				w.marks[code] = true
			}
			for row := 0; row < b.Rows; row++ {
				if w.marks[c.Code(row)] && in(row) {
					w.count++
				}
			}
		default:
			w.hits = pat.AppendMatches(w.hits[:0], c.Offsets[:c.Rows+1], c.Data)
			for _, row := range w.hits {
				if in(int(row)) {
					w.count++
				}
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}
	var total int64
	for i := range ws {
		total += ws[i].count
	}

	return total, nil
}

// queryWindowGroups counts the rows in the window by their string at path.
func queryWindowGroups(snap *Snapshot, workers int, win timeWindow, path string) (map[string]int64, error) {
	ws, err := scanWindow(snap, workers, win, nil, func(w *windowWorker, p *part.Part, b *part.Block) error {
		mask, err := w.timeMask(p, b, win)
		if err != nil {
			return err
		}
		if w.groups == nil {
			w.groups = map[string]int64{}
		}
		ref, err := column(p, b, path, vec.KindString, &w.col)
		if err != nil {
			return err
		}
		c := &w.col
		if ref == nil {
			if mask == nil {
				w.groups[""] += int64(b.Rows)
			} else {
				w.groups[""] += int64(vec.MaskCount(mask))
			}

			return nil
		}
		if c.Layout == vec.Flat {
			for row := 0; row < b.Rows; row++ {
				if mask == nil || mask[row] != 0 {
					if c.IsValid(row) {
						w.groups[string(c.Bytes(row))]++
					} else {
						w.groups[""]++
					}
				}
			}

			return nil
		}
		// One counter per dictionary entry, and one past them for rows
		// without a value.
		n := c.DictLen()
		w.hits = append(w.hits[:0], make([]uint32, n+1)...)
		for row := 0; row < b.Rows; row++ {
			if mask != nil && mask[row] == 0 {
				continue
			}
			switch {
			case !c.IsValid(row):
				w.hits[n]++
			case c.Layout == vec.Const:
				w.hits[0]++
			default:
				w.hits[c.Code(row)]++
			}
		}
		for code, count := range w.hits[:n] {
			if count != 0 {
				w.groups[string(c.DictValue(code))] += int64(count)
			}
		}
		if w.hits[n] != 0 {
			w.groups[""] += int64(w.hits[n])
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	total := map[string]int64{}
	for i := range ws {
		for k, v := range ws[i].groups {
			total[k] += v
		}
	}

	return total, nil
}

// queryLatestEvents returns the limit latest events whose string at path is
// want, newest first, with their commit.collection.
func queryLatestEvents(snap *Snapshot, workers int, path, want string, limit int) ([]timedEvent, error) {
	value := []byte(want)
	all := timeWindow{lo: -1 << 63, hi: 1<<63 - 1}
	ws, err := scanWindow(snap, workers, all, tokenfilter.AppendHashes(nil, value), func(w *windowWorker, p *part.Part, b *part.Block) error {
		ref, err := column(p, b, path, vec.KindString, &w.col)
		if err != nil || ref == nil {
			return err
		}
		if w.col.Layout == vec.Dict {
			if _, ok := w.col.DictFind(value); !ok {
				return nil
			}
		}
		if cap(w.mask) < b.Rows {
			w.mask = make([]uint8, b.Rows)
		}
		w.mask = w.mask[:b.Rows]
		vec.MaskEqBytes(w.mask, &w.col, value)
		w.hits = vec.MaskIndexes(w.hits[:0], w.mask)
		if len(w.hits) == 0 {
			return nil
		}
		if _, err := column(p, b, part.TimePath, vec.KindInt, &w.times); err != nil {
			return err
		}
		collection, err := column(p, b, "commit.collection", vec.KindString, &w.other)
		if err != nil {
			return err
		}
		for _, row := range w.hits {
			e := timedEvent{time: w.times.Int(int(row))}
			if collection != nil && w.other.IsValid(int(row)) {
				e.collection = string(w.other.Bytes(int(row)))
			}
			w.latest = append(w.latest, e)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	var out []timedEvent
	for i := range ws {
		out = append(out, ws[i].latest...)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].time != out[j].time {
			return out[i].time > out[j].time
		}

		return out[i].collection < out[j].collection
	})

	return out[:min(limit, len(out))], nil
}

// benchWindow returns the window named by env as "lo,hi" in microseconds,
// lo inclusive and hi exclusive, or else a window of the given width around
// the time that half of the snapshot's rows precede.
func benchWindow(t testing.TB, snap *Snapshot, env string, width time.Duration) timeWindow {
	t.Helper()
	if v := os.Getenv(env); v != "" {
		lo, hi, ok := strings.Cut(v, ",")
		from, err1 := strconv.ParseInt(lo, 10, 64)
		to, err2 := strconv.ParseInt(hi, 10, 64)
		if !ok || err1 != nil || err2 != nil || to <= from {
			t.Fatalf("%s=%q is not lo,hi in microseconds", env, v)
		}

		return timeWindow{lo: from * 1000, hi: to*1000 - 1}
	}
	type span struct {
		at   int64
		rows int
	}
	var spans []span
	total := 0
	for _, h := range snap.Parts() {
		for i := 0; i < h.NumBlocks(); i++ {
			rows, lo, hi := h.BlockStats(i)
			spans = append(spans, span{at: lo/2 + hi/2, rows: rows})
			total += rows
		}
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].at < spans[j].at })
	mid, seen := int64(0), 0
	for _, s := range spans {
		if seen += s.rows; seen >= total/2 {
			mid = s.at

			break
		}
	}
	// Whole minutes, so that the same window can be asked of other systems.
	mid -= mid % int64(time.Minute)

	return timeWindow{lo: mid - int64(width)/2, hi: mid + int64(width)/2 - 1}
}

// benchWindows returns the time-bounded queries of the benchmark, each
// giving a row count to compare with the reference systems, and a line that
// states their windows.
func benchWindows(t testing.TB, snap *Snapshot) ([]benchSearch, string) {
	t.Helper()
	ten := benchWindow(t, snap, "LYNXDB_BENCH_T10_US", 10*time.Minute)
	hour := benchWindow(t, snap, "LYNXDB_BENCH_T60_US", time.Hour)
	bounds := fmt.Sprintf("10-minute window %d..%d us, 1-hour window %d..%d us", ten.lo/1000, (ten.hi+1)/1000, hour.lo/1000, (hour.hi+1)/1000)

	return []benchSearch{
		{"T1 count in 10 minutes", func(s *Snapshot, n int) (int64, error) {
			return queryWindowCount(s, n, ten, "", tokenfilter.Pattern{})
		}},
		{"T2 token in 10 minutes", func(s *Snapshot, n int) (int64, error) {
			return queryWindowCount(s, n, ten, textPath, tokenfilter.Word("love"))
		}},
		{"T3 groups in 1 hour", func(s *Snapshot, n int) (int64, error) {
			groups, err := queryWindowGroups(s, n, hour, "commit.collection")
			var rows int64
			for _, count := range groups {
				rows += count
			}

			return rows, err
		}},
		{"T4 latest 20 of a user", func(s *Snapshot, n int) (int64, error) {
			events, err := queryLatestEvents(s, n, "did", rareDID, 20)
			var newest bytes.Buffer
			for _, e := range events[:min(3, len(events))] {
				newest.WriteString(" " + strconv.FormatInt(e.time/1000, 10) + ":" + e.collection)
			}
			windowNote = "; newest:" + newest.String()

			return int64(len(events)), err
		}},
	}, bounds
}
