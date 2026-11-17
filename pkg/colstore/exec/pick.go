package exec

import (
	"sort"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// resolve replaces, in the final rows, the places that First, Last, Earliest
// and Latest left by the values the located rows hold. It opens each block
// that holds such a row once, so its cost follows the rows of the result.
func resolve(snap *colstore.Snapshot, q *query, res *Result) error {
	type located struct {
		loc int64
		row int
	}
	var picks []located
	var b part.Block
	var col vec.Column
	parts := snap.Parts()
	for c, arg := range q.located {
		if arg < 0 {
			continue
		}
		e := q.agg.args[arg].expr
		picks = picks[:0]
		for r := 0; r < res.Rows; r++ {
			if v := res.Value(r, c); v.Kind == vec.KindInt {
				picks = append(picks, located{v.Int, r})
			}
		}
		sort.Slice(picks, func(i, j int) bool { return picks[i].loc < picks[j].loc })
		vals := make([]Value, res.Rows)
		for i := 0; i < len(picks); {
			block := picks[i].loc >> 16
			end := i
			for end < len(picks) && picks[end].loc>>16 == block {
				end++
			}
			p := parts[block>>28].Part
			if err := p.OpenBlock(int(block&(1<<28-1)), &b); err != nil {
				return err
			}
			pathID, ok := p.PathID(q.paths[e.path])
			// A row that holds values of several kinds gives the lowest.
			for kind := vec.KindVariant; ok && kind >= vec.KindBool; kind-- {
				ref, has := b.Column(pathID, kind)
				if e.kinds&kindBit(kind) == 0 || !has || ref.Count == 0 {
					continue
				}
				if err := b.Decode(ref, &col); err != nil {
					return err
				}
				for _, pk := range picks[i:end] {
					r := int(pk.loc & 0xFFFF)
					if !col.IsValid(r) {
						continue
					}
					v := Value{Kind: kind}
					switch kind {
					case vec.KindBool:
						v.Bool = col.Bool(r)
					case vec.KindInt:
						v.Int = col.Int(r)
					case vec.KindFloat:
						v.Float = col.Float(r)
					default:
						v.Bytes = append([]byte(nil), col.Bytes(r)...)
					}
					vals[pk.row] = v
				}
			}
			i = end
		}
		var out colVec
		for _, v := range vals {
			out.appendValue(v)
		}
		res.Columns[c] = out.column(res.Columns[c].Name)
	}

	return nil
}
