package exec

import (
	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// countedParts are the parts that an aggregate counted without their blocks.
type countedParts struct {
	parts  map[*part.Part]struct{}
	blocks int64
}

func (c *countedParts) skip(p *part.Part, _ int) bool {
	_, ok := c.parts[p]

	return ok
}

// countParts answers a count of rows by one stored column from the parts
// that count its values, and returns them for the scan to leave out.
func (x *execution) countParts(snap *colstore.Snapshot) *countedParts {
	q := x.q
	ap := q.agg
	if ap == nil || !ap.rowsOnly || len(ap.keys) != 1 || !q.timed || x.shared != nil {
		return nil
	}
	e := ap.keys[0]
	if e.op != OpColumn || (e.kind != KindAny && e.kind != vec.KindString) {
		return nil
	}
	var c *countedParts
	var a *aggWorker
	for _, h := range snap.Parts() {
		if h.Rows == 0 || h.MinTime < q.minT || h.MaxTime > q.maxT {
			continue
		}
		var counts *part.Counts
		if id, ok := h.PathID(q.paths[e.path]); ok {
			if counts, ok = h.Counts(id); !ok {
				continue
			}
		}
		if c == nil {
			c = &countedParts{parts: map[*part.Part]struct{}{}}
			w := x.workers[0]
			if w == nil {
				w = newWorker(x)
				x.workers[0] = w
			}
			a = &w.agg
		}
		c.parts[h.Part] = struct{}{}
		c.blocks += int64(h.NumBlocks())
		a.w.blocks += int64(h.NumBlocks())
		a.w.matched += int64(h.Rows)
		// A counted path holds no null, so the other rows miss the key.
		missing := h.Rows
		if counts != nil {
			missing -= counts.Held
			for i, v := range counts.Values {
				a.s.key = appendKeyBytes(a.s.key[:0], tagString, v, true)
				a.addRows(a.s.key, counts.Rows[i])
			}
		}
		if missing != 0 {
			a.s.key = append(a.s.key[:0], tagMissing)
			a.addRows(a.s.key, missing)
		}
	}

	return c
}

// addRows adds rows to the group of key.
func (a *aggWorker) addRows(key []byte, rows uint64) {
	g := a.group(key, keyHash(key))
	a.words[g*a.stride+a.plan.rows] += int64(rows)
}
