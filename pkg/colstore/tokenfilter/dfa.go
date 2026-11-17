package tokenfilter

import (
	"regexp/syntax"
	"slices"
	"sort"
	"sync"
	"unicode"
	"unicode/utf8"
	"unsafe"
)

// automaton answers whether a regular expression matches anywhere in a text
// by walking a deterministic automaton whose states are built as texts
// reach them: one table lookup per ASCII byte.
type automaton struct {
	prog *syntax.Prog
	// bounds holds, in ascending order, the first rune of every class of
	// runes that no instruction tells apart.
	bounds []rune
	// caches holds the states built so far, one set per goroutine at work.
	caches sync.Pool
}

const (
	// maxStates and maxTableBytes bound the states of one cache and their
	// tables of moves; an expression that needs more is left to the library.
	maxStates     = 4096
	maxTableBytes = 2 << 20

	stateUnknown = 0
	stateMatched = -1
	stateFull    = -2
)

// newAutomaton returns the automaton of an expression, or nil when the
// expression uses an assertion that depends on more than the ends of the
// text.
func newAutomaton(re *syntax.Regexp) *automaton {
	prog, err := syntax.Compile(re.Simplify())
	if err != nil {
		return nil
	}
	for i := range prog.Inst {
		inst := &prog.Inst[i]
		if inst.Op == syntax.InstEmptyWidth && syntax.EmptyOp(inst.Arg)&^(syntax.EmptyBeginText|syntax.EmptyEndText) != 0 {
			return nil
		}
	}
	a := &automaton{prog: prog}
	edge := func(r rune) { a.bounds = append(a.bounds, r, r+1) }
	for i := range prog.Inst {
		inst := &prog.Inst[i]
		switch {
		case inst.Op == syntax.InstRuneAnyNotNL:
			edge('\n')
		case inst.Op != syntax.InstRune && inst.Op != syntax.InstRune1:
		case len(inst.Rune) == 1:
			edge(inst.Rune[0])
			if syntax.Flags(inst.Arg)&syntax.FoldCase != 0 {
				for r := unicode.SimpleFold(inst.Rune[0]); r != inst.Rune[0]; r = unicode.SimpleFold(r) {
					edge(r)
				}
			}
		default:
			for j := 0; j+1 < len(inst.Rune); j += 2 {
				a.bounds = append(a.bounds, inst.Rune[j], inst.Rune[j+1]+1)
			}
		}
	}
	slices.Sort(a.bounds)
	a.bounds = slices.Compact(a.bounds)
	a.caches.New = func() any { return newStates(a) }

	return a
}

// class returns the number of the class of r.
func (a *automaton) class(r rune) int {
	return sort.Search(len(a.bounds), func(i int) bool { return a.bounds[i] > r })
}

// states is the part of an automaton built so far. State 1 is the one at
// the start of a text; the others are numbered as they are met.
type states struct {
	auto *automaton
	prog *syntax.Prog
	// ascii holds, for state s and byte b, the state after b at s<<7|b;
	// wide holds the same for the classes of runes, classes to a state.
	ascii   []int32
	wide    []int32
	classes int
	limit   int // most states the tables may hold
	// sets holds the instructions of each state and endMatch whether the
	// state matches when the text ends there.
	sets     [][]uint32
	endMatch []bool
	ids      map[string]int32
	// startMatch is set when the expression matches the empty string.
	startMatch bool
	// restart is what every position adds: a match may start anywhere.
	restart []uint32

	seen  []uint32 // per instruction, the mark of the closure that took it
	mark  uint32
	stack []uint32
	next  []uint32
}

func newStates(a *automaton) *states {
	c := &states{auto: a, prog: a.prog, classes: len(a.bounds) + 1, ids: map[string]int32{}, seen: make([]uint32, len(a.prog.Inst))}
	c.limit = max(8, min(maxStates, maxTableBytes/(4*(1<<7+c.classes))))
	c.ascii = make([]int32, 2<<7)
	c.wide = make([]int32, 2*c.classes)
	c.sets = make([][]uint32, 2)
	c.endMatch = make([]bool, 2)
	c.begin()
	c.restart = slices.Clone(c.closure(nil, uint32(c.prog.Start), false, false))
	c.begin()
	start := slices.Clone(c.closure(nil, uint32(c.prog.Start), true, false))
	c.sets[1], c.endMatch[1] = start, c.matchesAtEnd(start)
	for _, pc := range start {
		c.startMatch = c.startMatch || c.prog.Inst[pc].Op == syntax.InstMatch
	}

	return c
}

// closure appends to set the instructions reachable from pc without taking
// a rune: those that take one, the match, and the assertions of the end of
// the text, which stay until the text ends.
func (c *states) closure(set []uint32, pc uint32, atStart, atEnd bool) []uint32 {
	c.stack = append(c.stack[:0], pc)
	for len(c.stack) > 0 {
		pc := c.stack[len(c.stack)-1]
		c.stack = c.stack[:len(c.stack)-1]
		if c.seen[pc] == c.mark {
			continue
		}
		c.seen[pc] = c.mark
		inst := &c.prog.Inst[pc]
		switch inst.Op {
		case syntax.InstAlt, syntax.InstAltMatch:
			c.stack = append(c.stack, inst.Arg, inst.Out)
		case syntax.InstCapture, syntax.InstNop:
			c.stack = append(c.stack, inst.Out)
		case syntax.InstEmptyWidth:
			need := syntax.EmptyOp(inst.Arg)
			switch {
			case need&syntax.EmptyBeginText != 0 && !atStart:
			case need&syntax.EmptyEndText != 0 && !atEnd:
				set = append(set, pc)
			default:
				c.stack = append(c.stack, inst.Out)
			}
		case syntax.InstFail:
		default:
			set = append(set, pc)
		}
	}

	return set
}

// begin starts the set of a state; every closure into it shares one mark.
func (c *states) begin() {
	c.mark++
	if c.mark == 0 {
		clear(c.seen)
		c.mark = 1
	}
}

func (c *states) matchesAtEnd(set []uint32) bool {
	c.begin()
	var out []uint32
	for _, pc := range set {
		if inst := &c.prog.Inst[pc]; inst.Op == syntax.InstEmptyWidth {
			out = c.closure(out[:0], inst.Out, false, true)
			for _, reached := range out {
				if c.prog.Inst[reached].Op == syntax.InstMatch {
					return true
				}
			}
		}
	}

	return false
}

// step returns the state after r in state s, stateMatched when the
// expression has matched, or stateFull when the cache has no room.
func (c *states) step(s int32, r rune) int32 {
	c.begin()
	next := c.next[:0]
	for _, pc := range c.sets[s] {
		inst := &c.prog.Inst[pc]
		var takes bool
		switch inst.Op {
		case syntax.InstRune1:
			takes = inst.Rune[0] == r || (syntax.Flags(inst.Arg)&syntax.FoldCase != 0 && inst.MatchRune(r))
		case syntax.InstRune:
			takes = inst.MatchRune(r)
		case syntax.InstRuneAny:
			takes = true
		case syntax.InstRuneAnyNotNL:
			takes = r != '\n'
		}
		if takes {
			next = c.closure(next, inst.Out, false, false)
		}
	}
	for _, pc := range c.restart {
		if c.seen[pc] != c.mark {
			c.seen[pc] = c.mark
			next = append(next, pc)
		}
	}
	c.next = next
	for _, pc := range next {
		if c.prog.Inst[pc].Op == syntax.InstMatch {
			return stateMatched
		}
	}
	slices.Sort(next)
	key := unsafe.String((*byte)(unsafe.Pointer(unsafe.SliceData(next))), 4*len(next))
	if id, ok := c.ids[key]; ok {
		return id
	}
	if len(c.sets) >= c.limit {
		return stateFull
	}
	id := int32(len(c.sets))
	set := slices.Clone(next)
	c.sets = append(c.sets, set)
	c.endMatch = append(c.endMatch, c.matchesAtEnd(set))
	c.ascii = append(c.ascii, make([]int32, 1<<7)...)
	c.wide = append(c.wide, make([]int32, c.classes)...)
	c.ids[unsafe.String((*byte)(unsafe.Pointer(unsafe.SliceData(set))), 4*len(set))] = id

	return id
}

// match reports whether the expression matches in text; decided is false
// when the cache ran out of room and the answer has to come from elsewhere.
func (c *states) match(text []byte) (matched, decided bool) {
	if len(text) == 0 {
		return false, false
	}
	if c.startMatch {
		return true, true
	}
	s := int32(1)
	for i := 0; i < len(text); {
		var next int32
		if b := text[i]; b < utf8.RuneSelf {
			i++
			if next = c.ascii[int(s)<<7|int(b)]; next > 0 {
				s = next

				continue
			}
			if next == stateUnknown {
				next = c.step(s, rune(b))
				if next != stateFull {
					c.ascii[int(s)<<7|int(b)] = next
				}
			}
		} else {
			r, size := utf8.DecodeRune(text[i:])
			i += size
			at := int(s)*c.classes + c.auto.class(r)
			if next = c.wide[at]; next == stateUnknown {
				if next = c.step(s, r); next != stateFull {
					c.wide[at] = next
				}
			}
		}
		switch next {
		case stateMatched:
			return true, true
		case stateFull:
			return false, false
		}
		s = next
	}

	return c.endMatch[s], true
}
