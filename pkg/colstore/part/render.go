package part

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/lynxbase/lynxdb/pkg/colstore/variant"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// Renderer rebuilds JSON documents from the decoded columns of a block, with
// members ordered by path rather than as ingested.
type Renderer struct {
	items []renderItem
	open  [][]byte
}

type renderItem struct {
	path string
	segs [][]byte
	col  *vec.Column
}

// Reset prepares to render rows whose path paths[i] is held by cols[i], where
// a path may appear once per value kind.
func (r *Renderer) Reset(paths []string, cols []*vec.Column) {
	r.items = r.items[:0]
	for i, p := range paths {
		r.items = append(r.items, renderItem{path: p, segs: bytes.Split([]byte(p), []byte{'.'}), col: cols[i]})
	}
	sort.SliceStable(r.items, func(i, j int) bool { return r.items[i].path < r.items[j].path })
}

// AppendRow appends the document of row to dst.
func (r *Renderer) AppendRow(dst []byte, row int) []byte {
	dst = append(dst, '{')
	r.open = r.open[:0]
	empty := true
	for i := range r.items {
		it := &r.items[i]
		if !it.col.IsValid(row) {
			continue
		}
		parents := it.segs[:len(it.segs)-1]
		shared := 0
		for shared < len(r.open) && shared < len(parents) && bytes.Equal(r.open[shared], parents[shared]) {
			shared++
		}
		for len(r.open) > shared {
			dst = append(dst, '}')
			r.open = r.open[:len(r.open)-1]
			empty = false
		}
		for _, seg := range parents[shared:] {
			if !empty {
				dst = append(dst, ',')
			}
			dst = variant.AppendJSONString(dst, seg)
			dst = append(dst, ':', '{')
			r.open = append(r.open, seg)
			empty = true
		}
		if !empty {
			dst = append(dst, ',')
		}
		empty = false
		dst = variant.AppendJSONString(dst, it.segs[len(it.segs)-1])
		dst = append(dst, ':')
		dst = appendValue(dst, it.col, row)
	}
	for range r.open {
		dst = append(dst, '}')
	}

	return append(dst, '}')
}

func appendValue(dst []byte, c *vec.Column, row int) []byte {
	switch c.Kind {
	case vec.KindBool:
		return strconv.AppendBool(dst, c.Bool(row))
	case vec.KindInt:
		return strconv.AppendInt(dst, c.Int(row), 10)
	case vec.KindFloat:
		return variant.AppendJSONFloat(dst, c.Float(row))
	case vec.KindString:
		return variant.AppendJSONString(dst, c.Bytes(row))
	case vec.KindVariant:
		if out, err := variant.AppendJSON(dst, c.Bytes(row)); err == nil {
			return out
		}
	}

	return append(dst, "null"...)
}
