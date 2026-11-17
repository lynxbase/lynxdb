package exec

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/vec"
)

// stateOp says what the bytes of one state of a group hold.
type stateOp uint8

const (
	// stateMinBytes and stateMaxBytes hold the least and the greatest
	// string after a tag byte, so that an empty string is not no state.
	stateMinBytes stateOp = iota + 1
	stateMaxBytes
	// stateHLL holds the hashes of the distinct values or a sketch of them,
	// as hll.go lays them out.
	stateHLL
	// stateSketchPos and stateSketchNeg hold the stores of a percentile
	// sketch for the positive and the negative numbers, as ddsketch.go lays
	// them out.
	stateSketchPos
	stateSketchNeg
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
	switch op {
	case stateMinBytes, stateMaxBytes:
		ap.args[arg].stateKinds |= kindBit(vec.KindString)
	case stateHLL:
		ap.args[arg].stateKinds |= allKinds
	case stateSketchPos, stateSketchNeg:
		ap.args[arg].stateKinds |= numericKinds
	}

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
		if st.op == stateSketchPos || st.op == stateSketchNeg {
			ap.args[st.arg].sketch.merge(h, dst, b, st.op == stateSketchPos)

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
		case stateHLL:
			dst[st.word] = int64(hllMerge(h, ref, b))
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

// The hashes that an approximate distinct count gives values; kinds are
// told apart as in an exact one.
func hashInt(v int64) uint64 { return vec.Mix64(uint64(v) ^ kindSeeds[vec.KindInt]) }

func hashFloat(v float64) uint64 { return vec.Mix64(math.Float64bits(v) ^ kindSeeds[vec.KindFloat]) }

func hashBool(v bool) uint64 { return vec.Mix64(uint64(btoi(v)) ^ kindSeeds[vec.KindBool]) }

func hashBytes(kind vec.Kind, v []byte) uint64 { return vec.Hash(v) ^ kindSeeds[kind] }

// feedHashes adds the values of one kind in the n rows listed in rows, or
// the first n rows when it is nil, to the distinct-count states of an
// argument: the integers or floats given, or else the values of c.
func (a *aggWorker) feedHashes(arg *aggArg, t *target, kind vec.Kind, c *vec.Column, ints []int64, floats []float64, gid, rows []uint32, n int) {
	for _, i := range arg.states {
		st := &a.plan.states[i]
		if st.op != stateHLL {
			continue
		}
		// A dictionary is hashed once per entry.
		var entries []uint64
		if ints == nil && floats == nil && kind != vec.KindBool && c.Layout == vec.Dict {
			a.s.hashes = sized(a.s.hashes, c.DictLen())
			entries = a.s.hashes
			for e := range entries {
				entries[e] = hashBytes(kind, c.DictValue(e))
			}
		}
		lastG, last := -1, uint64(0)
		for j := 0; j < n; j++ {
			g, r := t.one, j
			if t.gid != nil {
				g = int(gid[j])
			}
			if rows != nil {
				r = int(rows[j])
			}
			var hash uint64
			switch {
			case ints != nil:
				hash = hashInt(ints[r])
			case floats != nil:
				hash = hashFloat(floats[r])
			case entries != nil:
				hash = entries[c.Code(r)]
			case kind == vec.KindBool:
				hash = hashBool(c.Bool(r))
			default:
				hash = hashBytes(kind, columnBytes(c, r))
			}
			if g == lastG && hash == last {
				continue
			}
			lastG, last = g, hash
			word := &t.words[g*a.stride+st.word]
			*word = int64(hllAdd(&a.heap, stateRef(*word), hash))
		}
	}
}

// sketch gives argument arg a percentile sketch: a word that counts its
// zeros and the states of two stores.
func (ap *aggPlan) sketch(arg int) {
	a := &ap.args[arg]
	if a.sketched {
		return
	}
	// The word is fed with the stores, not with the argument's other words.
	ap.words = append(ap.words, wordSpec{op: wordZeros, arg: -1})
	a.sketch.zeros = len(ap.words) - 1
	a.sketch.pos, a.sketch.neg = ap.states[ap.state(stateSketchPos, arg)].word, ap.states[ap.state(stateSketchNeg, arg)].word
	a.sketched = true
}

// feedSketch adds the numbers of the n rows listed in rows, or the first n
// rows when it is nil, to the percentile sketch of an argument: the integers
// or else the floats given.
func (a *aggWorker) feedSketch(arg *aggArg, t *target, ints []int64, floats []float64, gid, rows []uint32, n int) {
	for j := 0; j < n; j++ {
		g, r := t.one, j
		if t.gid != nil {
			g = int(gid[j])
		}
		if rows != nil {
			r = int(rows[j])
		}
		var x float64
		if ints != nil {
			x = float64(ints[r])
		} else {
			x = floats[r]
		}
		words := t.words[g*a.stride : (g+1)*a.stride]
		switch {
		case x != x:
		case x == 0:
			words[arg.sketch.zeros]++
		default:
			arg.sketch.add(&a.heap, words, x, 1)
		}
	}
}
