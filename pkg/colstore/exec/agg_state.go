package exec

import (
	"bytes"
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// stateOp says what the bytes of one state of a group hold.
type stateOp uint8

const (
	// stateMinBytes and stateMaxBytes hold the least and the greatest
	// string after a tag byte, so that an empty string is not no state.
	stateMinBytes stateOp = iota + 1
	stateMaxBytes
)

// stateSpec is one state of variable size per group. The group's row of
// words holds its stateRef at word; the bytes live in the stateHeap of
// whoever owns the row, and travel in spill records after the words.
type stateSpec struct {
	op   stateOp
	word int
	arg  int
}

// state adds a state of op fed by argument arg, or returns the one there is.
func (ap *aggPlan) state(op stateOp, arg int) int {
	for i, s := range ap.states {
		if s.op == op && s.arg == arg {
			return i
		}
	}
	ap.words = append(ap.words, wordSpec{op: wordState, arg: -1})
	ap.states = append(ap.states, stateSpec{op: op, word: len(ap.words) - 1, arg: arg})
	ap.args[arg].states = append(ap.args[arg].states, len(ap.states)-1)

	return len(ap.states) - 1
}

// stateSource reads the states of a group that is merged into another: from
// the heap they live in, or in turn from the tail of a spill record.
type stateSource struct {
	heap  *stateHeap
	words []int64
	tail  []byte
}

func (s *stateSource) next(word int) []byte {
	if s.heap != nil {
		return s.heap.get(stateRef(s.words[word]))
	}
	n, used := binary.Uvarint(s.tail)
	b := s.tail[used : used+int(n)]
	s.tail = s.tail[used+int(n):]

	return b
}

// appendStates appends the states of a group as a spill record carries them:
// each preceded by its length, then the length of them all in four bytes.
func (ap *aggPlan) appendStates(dst []byte, h *stateHeap, words []int64) []byte {
	start := len(dst)
	for i := range ap.states {
		b := h.get(stateRef(words[ap.states[i].word]))
		dst = append(binary.AppendUvarint(dst, uint64(len(b))), b...)
	}

	return binary.LittleEndian.AppendUint32(dst, uint32(len(dst)-start))
}

// splitRecord returns the key, the words and the states of a spill record.
func (ap *aggPlan) splitRecord(rec []byte) (key, words, tail []byte) {
	end := len(rec)
	if len(ap.states) != 0 {
		end -= 4
		n := int(binary.LittleEndian.Uint32(rec[end:]))
		tail, end = rec[end-n:end], end-n
	}
	k := end - 8*len(ap.words)

	return rec[:k], rec[k:end], tail
}

// releaseStates gives up the states of a group.
func (ap *aggPlan) releaseStates(h *stateHeap, words []int64) {
	for i := range ap.states {
		w := ap.states[i].word
		h.release(stateRef(words[w]))
		words[w] = 0
	}
}

// adoptStates makes dst, whose words were copied from the group of src, the
// owner of copies of that group's states in h.
func (ap *aggPlan) adoptStates(h *stateHeap, dst []int64, src *stateSource) {
	for i := range ap.states {
		dst[ap.states[i].word] = 0
	}
	ap.mergeStates(h, dst, src)
}

// mergeStates folds the states of the group of src into those of dst, which
// live in h.
func (ap *aggPlan) mergeStates(h *stateHeap, dst []int64, src *stateSource) {
	for i := range ap.states {
		st := &ap.states[i]
		b := src.next(st.word)
		if len(b) == 0 {
			continue
		}
		ref := stateRef(dst[st.word])
		cur := h.get(ref)
		if len(cur) == 0 {
			dst[st.word] = int64(h.set(ref, b))

			continue
		}
		switch st.op {
		case stateMinBytes, stateMaxBytes:
			if c := bytes.Compare(b[1:], cur[1:]); c != 0 && (c > 0) == (st.op == stateMaxBytes) {
				dst[st.word] = int64(h.set(ref, b))
			}
		}
	}
}

func columnBytes(c *vec.Column, r int) []byte {
	switch c.Layout {
	case vec.Flat:
		return c.Data[c.Offsets[r]:c.Offsets[r+1]]
	case vec.Dict:
		return c.DictValue(c.Code(r))
	}

	return c.DictValue(0)
}

// feedStrings offers the strings of c in the n rows listed in rows, or the
// first n rows when it is nil, to the extremes of an argument.
func (a *aggWorker) feedStrings(arg *aggArg, t *target, c *vec.Column, gid, rows []uint32, n int) {
	for _, i := range arg.states {
		st := &a.plan.states[i]
		if st.op != stateMinBytes && st.op != stateMaxBytes {
			continue
		}
		greatest := st.op == stateMaxBytes
		lastG := -1
		var last []byte
		for j := 0; j < n; j++ {
			g, r := t.one, j
			if t.gid != nil {
				g = int(gid[j])
			}
			if rows != nil {
				r = int(rows[j])
			}
			v := columnBytes(c, r)
			word := &t.words[g*a.stride+st.word]
			// Rows of one group in a row compare with the string kept
			// without looking the state up again.
			if g != lastG {
				lastG, last = g, nil
				if cur := a.heap.get(stateRef(*word)); len(cur) != 0 {
					last = cur[1:]
				} else {
					lastG = -1
				}
			}
			if lastG == g {
				if cmp := bytes.Compare(v, last); cmp == 0 || (cmp > 0) != greatest {
					continue
				}
			}
			ref, b := a.heap.resize(stateRef(*word), 1+len(v))
			b[0] = tagString
			copy(b[1:], v)
			*word = int64(ref)
			lastG, last = g, b[1:]
		}
	}
}
