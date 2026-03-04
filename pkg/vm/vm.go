package vm

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

const StackSize = 256

var (
	ErrStackOverflow  = errors.New("vm: stack overflow")
	ErrStackUnderflow = errors.New("vm: stack underflow")
)

// ErrInvalidBytecode is returned when the program contains truncated
// instructions or out-of-range operand indices.
var ErrInvalidBytecode = errors.New("vm: invalid bytecode")

// VM is a stack-based virtual machine for evaluating SPL2 expressions.
// The VM is designed to be reused across events with zero allocations on the hot path.
type VM struct {
	stack        [StackSize]event.Value
	sp           int
	regexCache   []*regexp.Regexp
	replaceHints []func(s, replacement string) string // per-regex fast-path; nil = use regex
	matchHints   []func(string) bool                  // per-regex fast-path; nil = use regex

	// jsonCache is a single-entry cache for JSON dot-notation lookups.
	// When a query accesses multiple paths from the same root (e.g.
	// request.method, request.path, request.duration), the first access
	// unmarshals the root field and caches the top-level keys. Subsequent
	// accesses skip re-parsing. The cache is keyed by the root field's
	// string value — a new event naturally invalidates it because the
	// string value differs.
	jsonCache jsonParseCache
}

// jsonParseCache caches the result of unmarshaling a JSON string so that
// multiple dot-path lookups against the same root object avoid redundant parsing.
type jsonParseCache struct {
	source string                     // source JSON string (cache key)
	parsed map[string]json.RawMessage // top-level keys → raw JSON
}

// isASCIILower returns true if s contains only lowercase ASCII and non-letter ASCII.
// Returns false for any uppercase ASCII or non-ASCII byte, ensuring safe fallback
// to strings.ToLower for Unicode correctness.
func isASCIILower(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			return false
		}
		if c > 127 {
			return false
		}
	}

	return true
}

// isASCIIAlphaNumLower returns true if s is non-empty and contains only [a-z0-9].
func isASCIIAlphaNumLower(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}

	return true
}

// analyzeReplacePattern detects common replace patterns that can be handled
// by string operations instead of regex. Returns nil if no fast path applies.
//
// Supported patterns:
//   - "^.*\\." (greedy: match up to last dot) → strings.LastIndexByte
//   - "^.*?\\." (non-greedy: match up to first dot) → strings.IndexByte
func analyzeReplacePattern(pattern string) func(s, replacement string) string {
	// Greedy: ^.*\. → replace everything up to and including the last dot
	if pattern == `^.*\.` {
		return func(s, replacement string) string {
			idx := strings.LastIndexByte(s, '.')
			if idx < 0 {
				return s
			}

			return replacement + s[idx+1:]
		}
	}
	// Non-greedy: ^.*?\. → replace everything up to and including the first dot
	if pattern == `^.*?\.` {
		return func(s, replacement string) string {
			idx := strings.IndexByte(s, '.')
			if idx < 0 {
				return s
			}

			return replacement + s[idx+1:]
		}
	}

	return nil
}

// analyzeMatchPattern detects common match patterns that can be handled
// by byte-level scanning instead of regex. Returns nil if no fast path applies.
//
// Supported patterns:
//   - "^[a-z0-9]+$" → full-string ASCII alphanumeric lowercase check
func analyzeMatchPattern(pattern string) func(string) bool {
	if pattern == `^[a-z0-9]+$` {
		return isASCIIAlphaNumLower
	}

	return nil
}

// readOperandSafe reads a uint16 operand from ins at the given offset.
// Returns ErrInvalidBytecode if there are not enough bytes remaining.
func readOperandSafe(ins []byte, ip int) (uint16, error) {
	if ip+2 > len(ins) {
		return 0, fmt.Errorf("%w: truncated operand at ip=%d", ErrInvalidBytecode, ip)
	}

	return ReadUint16(ins[ip:]), nil
}

// readIndexSafe reads a uint16 operand and validates it against a slice length.
// Returns ErrInvalidBytecode if the instruction stream is truncated or the
// index is out of range.
func readIndexSafe(ins []byte, ip, sliceLen int, label string) (int, error) {
	operand, err := readOperandSafe(ins, ip)
	if err != nil {
		return 0, err
	}

	idx := int(operand)
	if idx >= sliceLen {
		return 0, fmt.Errorf("%w: %s index %d out of range (len=%d)", ErrInvalidBytecode, label, idx, sliceLen)
	}

	return idx, nil
}

// execConst loads a constant onto the stack with bounds checking.
func (vm *VM) execConst(prog *Program, ins []byte, ip int) (int, error) {
	if vm.sp >= StackSize {
		return 0, ErrStackOverflow
	}

	idx, err := readIndexSafe(ins, ip, len(prog.Constants), "constant")
	if err != nil {
		return 0, err
	}

	vm.stack[vm.sp] = prog.Constants[idx]
	vm.sp++

	return ip + 2, nil
}

// execLoadField loads a field value onto the stack with bounds checking.
// When a direct field lookup fails and the field name contains a dot, the VM
// attempts JSON extraction as a fallback — enabling `response.status` syntax
// without an explicit unpack step. The fast path (direct lookup) is unchanged.
func (vm *VM) execLoadField(prog *Program, ins []byte, ip int, fields map[string]event.Value) (int, error) {
	if vm.sp >= StackSize {
		return 0, ErrStackOverflow
	}

	idx, err := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
	if err != nil {
		return 0, err
	}

	name := prog.FieldNames[idx]
	if val, ok := fields[name]; ok {
		// Fast path: field exists directly (e.g., unpack_json already ran,
		// or the field was set by EVAL, or it's a physical column).
		vm.stack[vm.sp] = val
	} else if dotIdx := strings.IndexByte(name, '.'); dotIdx > 0 {
		// Dot-notation fallback: try JSON extraction from the root field
		// or from _raw. Only triggered on cache-miss (field not in map)
		// and only when the field name contains a dot.
		root := name[:dotIdx]
		path := name[dotIdx+1:]
		if rootVal, ok := fields[root]; ok && !rootVal.IsNull() {
			vm.stack[vm.sp] = vm.jsonExtractCached(rootVal.String(), path)
		} else if rawVal, ok := fields["_raw"]; ok && !rawVal.IsNull() {
			vm.stack[vm.sp] = vm.jsonExtractCached(rawVal.String(), name)
		} else {
			vm.stack[vm.sp] = event.NullValue()
		}
	} else {
		vm.stack[vm.sp] = event.NullValue()
	}

	vm.sp++

	return ip + 2, nil
}

// execStoreField stores the top-of-stack value into a field with bounds checking.
func (vm *VM) execStoreField(prog *Program, ins []byte, ip int, fields map[string]event.Value) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
	if err != nil {
		return 0, err
	}

	name := prog.FieldNames[idx]
	vm.sp--
	fields[name] = vm.stack[vm.sp]

	return ip + 2, nil
}

// execFieldExists pushes a bool indicating whether a field exists with bounds checking.
func (vm *VM) execFieldExists(prog *Program, ins []byte, ip int, fields map[string]event.Value) (int, error) {
	if vm.sp >= StackSize {
		return 0, ErrStackOverflow
	}

	idx, err := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
	if err != nil {
		return 0, err
	}

	name := prog.FieldNames[idx]
	val, ok := fields[name]
	vm.stack[vm.sp] = event.BoolValue(ok && !val.IsNull())
	vm.sp++

	return ip + 2, nil
}

// execStrMatch performs regex matching with bounds checking.
func (vm *VM) execStrMatch(ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
	if err != nil {
		return 0, err
	}

	a := vm.stack[vm.sp-1]
	if a.IsNull() {
		vm.stack[vm.sp-1] = event.BoolValue(false)
	} else {
		s := valueToString(a)
		// Fast path: use precomputed byte-scan function for common patterns
		// (e.g., ^[a-z0-9]+$ → isASCIIAlphaNumLower) instead of regex.
		if fastFn := vm.matchHints[idx]; fastFn != nil {
			vm.stack[vm.sp-1] = event.BoolValue(fastFn(s))
		} else {
			re := vm.regexCache[idx]
			vm.stack[vm.sp-1] = event.BoolValue(re.MatchString(s))
		}
	}

	return ip + 2, nil
}

// execGlobMatch performs glob matching with bounds checking.
func (vm *VM) execGlobMatch(prog *Program, ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(prog.RegexPatterns), "pattern")
	if err != nil {
		return 0, err
	}

	a := vm.stack[vm.sp-1]
	if a.IsNull() {
		vm.stack[vm.sp-1] = event.BoolValue(false)
	} else {
		pattern := prog.RegexPatterns[idx]
		matched, _ := filepath.Match(pattern, valueToString(a))
		vm.stack[vm.sp-1] = event.BoolValue(matched)
	}

	return ip + 2, nil
}

// execInList checks if a value is in a list on the stack with bounds checking.
func (vm *VM) execInList(ins []byte, ip int) (int, error) {
	operand, err := readOperandSafe(ins, ip)
	if err != nil {
		return 0, err
	}

	count := int(operand)
	val := vm.stack[vm.sp-count-1]
	found := false

	for i := 0; i < count; i++ {
		item := vm.stack[vm.sp-count+i]
		if valuesEqual(val, item) {
			found = true

			break
		}
	}

	vm.sp -= count                             // pop items
	vm.stack[vm.sp-1] = event.BoolValue(found) // replace value with result

	return ip + 2, nil
}

// execReplace performs regex replacement with bounds checking.
func (vm *VM) execReplace(ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
	if err != nil {
		return 0, err
	}

	replacement := vm.stack[vm.sp-1]
	str := vm.stack[vm.sp-2]
	vm.sp--

	if str.IsNull() {
		vm.stack[vm.sp-1] = event.NullValue()
	} else {
		s := valueToString(str)
		r := valueToString(replacement)
		// Fast path: use precomputed string function for common patterns
		// (e.g., ^.*\. → LastIndexByte) instead of regex.
		if fastFn := vm.replaceHints[idx]; fastFn != nil {
			vm.stack[vm.sp-1] = event.StringValue(fastFn(s, r))
		} else {
			re := vm.regexCache[idx]
			vm.stack[vm.sp-1] = event.StringValue(re.ReplaceAllString(s, r))
		}
	}

	return ip + 2, nil
}

// Execute runs a compiled program against a set of event fields.
// Returns the top-of-stack value. The VM instance can be reused.
func (vm *VM) Execute(prog *Program, fields map[string]event.Value) (event.Value, error) {
	vm.sp = 0
	vm.ensureRegexCache(prog)

	ins := prog.Instructions
	ip := 0

	for ip < len(ins) {
		op := Opcode(ins[ip])
		ip++

		switch op {
		case OpNop:
			// do nothing

		case OpPop:
			if vm.sp <= 0 {
				return event.NullValue(), ErrStackUnderflow
			}
			vm.sp--

		case OpDup:
			if vm.sp <= 0 {
				return event.NullValue(), ErrStackUnderflow
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = vm.stack[vm.sp-1]
			vm.sp++

		// === Constants ===
		case OpConstInt, OpConstFloat, OpConstStr:
			newIP, err := vm.execConst(prog, ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpConstTrue:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = event.BoolValue(true)
			vm.sp++

		case OpConstFalse:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = event.BoolValue(false)
			vm.sp++

		case OpConstNull:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = event.NullValue()
			vm.sp++

		// === Field Access ===
		case OpLoadField:
			newIP, err := vm.execLoadField(prog, ins, ip, fields)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpStoreField:
			newIP, err := vm.execStoreField(prog, ins, ip, fields)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpFieldExists:
			newIP, err := vm.execFieldExists(prog, ins, ip, fields)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		// === Integer Arithmetic ===
		case OpAddInt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.IntValue(a.AsInt() + b.AsInt())

		case OpSubInt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.IntValue(a.AsInt() - b.AsInt())

		case OpMulInt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.IntValue(a.AsInt() * b.AsInt())

		case OpDivInt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			bv := b.AsInt()
			if bv == 0 {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.IntValue(a.AsInt() / bv)
			}

		case OpModInt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			bv := b.AsInt()
			if bv == 0 {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.IntValue(a.AsInt() % bv)
			}

		case OpNegInt:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.IntValue(-a.AsInt())

		// === Float Arithmetic ===
		case OpAddFloat:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.FloatValue(a.AsFloat() + b.AsFloat())

		case OpSubFloat:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.FloatValue(a.AsFloat() - b.AsFloat())

		case OpMulFloat:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.FloatValue(a.AsFloat() * b.AsFloat())

		case OpDivFloat:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			bv := b.AsFloat()
			if bv == 0 {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.FloatValue(a.AsFloat() / bv)
			}

		case OpNegFloat:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.FloatValue(-a.AsFloat())

		// === Mixed Arithmetic ===
		case OpAdd:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = addValues(a, b)

		case OpSub:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = subValues(a, b)

		case OpMul:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = mulValues(a, b)

		case OpDiv:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = divValues(a, b)

		case OpMod:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = modValues(a, b)

		// === String Operations ===
		case OpConcat:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.StringValue(valueToString(a) + valueToString(b))
			}

		case OpStrLen:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.IntValue(int64(len(valueToString(a))))
			}

		case OpSubstr:
			length := vm.stack[vm.sp-1]
			start := vm.stack[vm.sp-2]
			str := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = substrValue(str, start, length)

		case OpToLower:
			a := vm.stack[vm.sp-1]
			if !a.IsNull() {
				s := valueToString(a)
				// Fast path: skip strings.ToLower allocation when string is already
				// lowercase ASCII (common for file extensions, log levels, etc.).
				if !isASCIILower(s) {
					s = strings.ToLower(s)
				}
				vm.stack[vm.sp-1] = event.StringValue(s)
			}

		case OpToUpper:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				// keep null
			} else {
				vm.stack[vm.sp-1] = event.StringValue(strings.ToUpper(valueToString(a)))
			}

		case OpStrMatch:
			newIP, err := vm.execStrMatch(ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpGlobMatch:
			newIP, err := vm.execGlobMatch(prog, ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpReplace:
			newIP, err := vm.execReplace(ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpSplit:
			delim := vm.stack[vm.sp-1]
			str := vm.stack[vm.sp-2]
			vm.sp--
			if str.IsNull() || delim.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				parts := strings.Split(valueToString(str), valueToString(delim))
				vm.stack[vm.sp-1] = event.StringValue(strings.Join(parts, "|||"))
			}

		// === Comparison ===
		case OpEq:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(valuesEqual(a, b))

		case OpNeq:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(!valuesEqual(a, b))

		case OpLt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) < 0)

		case OpLte:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) <= 0)

		case OpGt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) > 0)

		case OpGte:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) >= 0)

		case OpLike:
			b := vm.stack[vm.sp-1] // pattern
			a := vm.stack[vm.sp-2] // text
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(matchLike(valueToString(a), valueToString(b)))
			}

		case OpInList:
			newIP, err := vm.execInList(ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		// === Logic ===
		case OpAnd:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(IsTruthy(a) && IsTruthy(b))

		case OpOr:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(IsTruthy(a) || IsTruthy(b))

		case OpNot:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(!IsTruthy(a))

		// === Control Flow ===
		case OpJump:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip = int(operand)

		case OpJumpIfFalse:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			pos := int(operand)
			ip += 2
			a := vm.stack[vm.sp-1]
			vm.sp--
			if !IsTruthy(a) {
				ip = pos
			}

		case OpJumpIfTrue:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			pos := int(operand)
			ip += 2
			a := vm.stack[vm.sp-1]
			vm.sp--
			if IsTruthy(a) {
				ip = pos
			}

		// === Type Conversion ===
		case OpToInt:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = toIntValue(a)

		case OpToFloat:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = toFloatValue(a)

		case OpToString:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				// keep null
			} else {
				vm.stack[vm.sp-1] = event.StringValue(valueToString(a))
			}

		case OpToBool:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(IsTruthy(a))

		// === Math Functions ===
		case OpRound:
			decimals := vm.stack[vm.sp-1]
			num := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = roundValue(num, decimals)

		case OpLn:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = lnValue(a)

		case OpAbs:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = absValue(a)

		case OpCeil:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = ceilValue(a)

		case OpFloor:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = floorValue(a)

		case OpSqrt:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = sqrtValue(a)

		// === Multivalue ===
		case OpMvAppend:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			parts := make([]string, 0, count)
			for i := vm.sp - count; i < vm.sp; i++ {
				parts = append(parts, valueToString(vm.stack[i]))
			}
			vm.sp -= count
			// Store as pipe-delimited multivalue string
			vm.stack[vm.sp] = event.StringValue(strings.Join(parts, "|||"))
			vm.sp++

		case OpMvJoin:
			delim := vm.stack[vm.sp-1]
			mv := vm.stack[vm.sp-2]
			vm.sp--
			if mv.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				parts := strings.Split(valueToString(mv), "|||")
				vm.stack[vm.sp-1] = event.StringValue(strings.Join(parts, valueToString(delim)))
			}

		case OpMvDedup:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				// keep null
			} else {
				parts := strings.Split(valueToString(a), "|||")
				seen := make(map[string]bool, len(parts))
				deduped := make([]string, 0, len(parts))
				for _, p := range parts {
					if !seen[p] {
						seen[p] = true
						deduped = append(deduped, p)
					}
				}
				vm.stack[vm.sp-1] = event.StringValue(strings.Join(deduped, "|||"))
			}

		case OpMvCount:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.IntValue(0)
			} else {
				parts := strings.Split(valueToString(a), "|||")
				vm.stack[vm.sp-1] = event.IntValue(int64(len(parts)))
			}

		// === Null Handling ===
		case OpCoalesce:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			result := event.NullValue()
			for i := vm.sp - count; i < vm.sp; i++ {
				if !vm.stack[i].IsNull() {
					result = vm.stack[i]

					break
				}
			}
			vm.sp -= count
			vm.stack[vm.sp] = result
			vm.sp++

		case OpIsNull:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(a.IsNull())

		case OpIsNotNull:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(!a.IsNull())

		// === Time ===
		case OpStrftime:
			format := vm.stack[vm.sp-1]
			ts := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = strftimeValue(ts, format)

		// === JSON Functions ===
		case OpJsonExtract:
			// Stack: [..., field, path] → [..., result]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonExtractValue(field, path)

		case OpJsonValid:
			// Stack: [..., field] → [..., bool]
			field := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = jsonValidValue(field)

		case OpJsonKeys:
			// Stack: [..., field, path] → [..., result]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonKeysValue(field, path)

		case OpJsonArrayLen:
			// Stack: [..., field, path] → [..., result]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonArrayLenValue(field, path)

		case OpJsonObject:
			// Operand: arg count (must be even). Pop N values, build JSON object.
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			values := make([]event.Value, count)
			for i := 0; i < count; i++ {
				values[i] = vm.stack[vm.sp-count+i]
			}
			vm.sp -= count
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = jsonObjectValue(values)
			vm.sp++

		case OpJsonArray:
			// Operand: arg count. Pop N values, build JSON array.
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			values := make([]event.Value, count)
			for i := 0; i < count; i++ {
				values[i] = vm.stack[vm.sp-count+i]
			}
			vm.sp -= count
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = jsonArrayValue(values)
			vm.sp++

		case OpJsonType:
			// Stack: [..., field, path] → [..., type_string]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonTypeValue(field, path)

		case OpJsonSet:
			// Stack: [..., field, path, value] → [..., modified_json]
			value := vm.stack[vm.sp-1]
			path := vm.stack[vm.sp-2]
			field := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = jsonSetValue(field, path, value)

		case OpJsonRemove:
			// Stack: [..., field, path] → [..., modified_json]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonRemoveValue(field, path)

		case OpJsonMerge:
			// Stack: [..., json1, json2] → [..., merged_json]
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonMergeValue(a, b)

		// === Return ===
		case OpReturn:
			if vm.sp > 0 {
				return vm.stack[vm.sp-1], nil
			}

			return event.NullValue(), nil

		default:
			return event.NullValue(), fmt.Errorf("unknown opcode: 0x%02x at ip=%d", byte(op), ip-1)
		}

		if vm.sp > StackSize {
			return event.NullValue(), ErrStackOverflow
		}
	}

	if vm.sp > 0 {
		return vm.stack[vm.sp-1], nil
	}

	return event.NullValue(), nil
}

func (vm *VM) ensureRegexCache(prog *Program) {
	if len(vm.regexCache) == len(prog.RegexPatterns) {
		return
	}
	n := len(prog.RegexPatterns)
	vm.regexCache = make([]*regexp.Regexp, n)
	vm.replaceHints = make([]func(s, replacement string) string, n)
	vm.matchHints = make([]func(string) bool, n)
	for i, p := range prog.RegexPatterns {
		vm.regexCache[i] = regexp.MustCompile(p)
		vm.replaceHints[i] = analyzeReplacePattern(p)
		vm.matchHints[i] = analyzeMatchPattern(p)
	}
}

// jsonExtractCached extracts a dot-path from a JSON string, using a single-entry
// cache to avoid re-parsing the same JSON object on consecutive calls. When the
// source string matches the cached value, top-level key lookup is O(1).
// Otherwise the JSON is parsed and cached for subsequent calls.
func (vm *VM) jsonExtractCached(source, path string) event.Value {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return event.NullValue()
	}

	// Check if cache is valid for this source.
	if vm.jsonCache.source != source || vm.jsonCache.parsed == nil {
		s := strings.TrimSpace(source)
		if len(s) == 0 || s[0] != '{' {
			return event.NullValue()
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal([]byte(s), &obj); err != nil {
			return event.NullValue()
		}
		vm.jsonCache.source = source
		vm.jsonCache.parsed = obj
	}

	// Fast single-level lookup.
	raw, ok := vm.jsonCache.parsed[parts[0]]
	if !ok {
		return event.NullValue()
	}

	if len(parts) == 1 {
		return jsonFragmentToValue(raw)
	}

	// Multi-level: walk remaining path from the cached first-level value.
	return walkJSONPath(raw, parts[1:])
}

// Value helper functions

// IsTruthy returns whether an event value is considered true.
func IsTruthy(v event.Value) bool {
	switch v.Type() {
	case event.FieldTypeNull:
		return false
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeInt:
		return v.AsInt() != 0
	case event.FieldTypeFloat:
		return v.AsFloat() != 0
	case event.FieldTypeString:
		return v.AsString() != ""
	case event.FieldTypeTimestamp:
		return true
	}

	return false
}

func valueToString(v event.Value) string {
	return v.String()
}

// ValueToFloat converts an event value to float64, returning false if not convertible.
func ValueToFloat(v event.Value) (float64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	case event.FieldTypeString:
		f, err := strconv.ParseFloat(v.AsString(), 64)
		if err == nil {
			return f, true
		}
	case event.FieldTypeBool:
		if v.AsBool() {
			return 1, true
		}

		return 0, true
	}

	return 0, false
}

func addValues(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	// String concatenation
	if a.Type() == event.FieldTypeString && b.Type() == event.FieldTypeString {
		return event.StringValue(a.AsString() + b.AsString())
	}
	// Int + Int
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		return event.IntValue(a.AsInt() + b.AsInt())
	}
	// Float + Float
	if a.Type() == event.FieldTypeFloat && b.Type() == event.FieldTypeFloat {
		return event.FloatValue(a.AsFloat() + b.AsFloat())
	}
	// Mixed: promote to float
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		return event.FloatValue(af + bf)
	}

	return event.NullValue()
}

func subValues(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		return event.IntValue(a.AsInt() - b.AsInt())
	}
	if a.Type() == event.FieldTypeFloat && b.Type() == event.FieldTypeFloat {
		return event.FloatValue(a.AsFloat() - b.AsFloat())
	}
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		return event.FloatValue(af - bf)
	}

	return event.NullValue()
}

func mulValues(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		return event.IntValue(a.AsInt() * b.AsInt())
	}
	if a.Type() == event.FieldTypeFloat && b.Type() == event.FieldTypeFloat {
		return event.FloatValue(a.AsFloat() * b.AsFloat())
	}
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		return event.FloatValue(af * bf)
	}

	return event.NullValue()
}

func divValues(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		bv := b.AsInt()
		if bv == 0 {
			return event.NullValue()
		}

		return event.IntValue(a.AsInt() / bv)
	}
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		if bf == 0 {
			return event.NullValue()
		}

		return event.FloatValue(af / bf)
	}

	return event.NullValue()
}

func modValues(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		bv := b.AsInt()
		if bv == 0 {
			return event.NullValue()
		}

		return event.IntValue(a.AsInt() % bv)
	}

	return event.NullValue()
}

func valuesEqual(a, b event.Value) bool {
	if a.IsNull() && b.IsNull() {
		return true
	}
	if a.IsNull() || b.IsNull() {
		return false
	}
	if a.Type() == b.Type() {
		switch a.Type() {
		case event.FieldTypeString:
			return a.AsString() == b.AsString()
		case event.FieldTypeInt:
			return a.AsInt() == b.AsInt()
		case event.FieldTypeFloat:
			return a.AsFloat() == b.AsFloat()
		case event.FieldTypeBool:
			return a.AsBool() == b.AsBool()
		case event.FieldTypeTimestamp:
			return a.AsTimestamp().Equal(b.AsTimestamp())
		}
	}
	// Cross-type numeric comparison
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		return af == bf
	}
	// Fall back to string comparison
	return valueToString(a) == valueToString(b)
}

// CompareValues performs a three-way comparison of two event values.
func CompareValues(a, b event.Value) int {
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Same-type fast paths
	if a.Type() == b.Type() {
		switch a.Type() {
		case event.FieldTypeInt:
			ai, bi := a.AsInt(), b.AsInt()
			if ai < bi {
				return -1
			} else if ai > bi {
				return 1
			}

			return 0
		case event.FieldTypeFloat:
			af, bf := a.AsFloat(), b.AsFloat()
			if af < bf {
				return -1
			} else if af > bf {
				return 1
			}

			return 0
		case event.FieldTypeString:
			as, bs := a.AsString(), b.AsString()
			// Schema-on-read numeric promotion: KV-extracted fields are strings
			// even when they contain numbers (e.g., response_time="786").
			// Try numeric comparison first so that min/max/sort/comparisons
			// produce correct results without requiring explicit tonumber().
			af, aErr := strconv.ParseFloat(as, 64)
			bf, bErr := strconv.ParseFloat(bs, 64)
			if aErr == nil && bErr == nil {
				if af < bf {
					return -1
				} else if af > bf {
					return 1
				}

				return 0
			}

			return strings.Compare(as, bs)
		case event.FieldTypeBool:
			ab, bb := boolToInt(a.AsBool()), boolToInt(b.AsBool())

			return ab - bb
		case event.FieldTypeTimestamp:
			at, bt := a.AsTimestamp(), b.AsTimestamp()
			if at.Before(bt) {
				return -1
			} else if at.After(bt) {
				return 1
			}

			return 0
		}
	}

	// Cross-type numeric comparison
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}

		return 0
	}

	// Fall back to string comparison
	return strings.Compare(valueToString(a), valueToString(b))
}

// matchLike implements SQL LIKE pattern matching (case-insensitive).
// '%' matches zero or more characters, '_' matches exactly one character.
// All other characters are matched literally.
func matchLike(text, pattern string) bool {
	// Case-insensitive: lowercase both sides.
	text = strings.ToLower(text)
	pattern = strings.ToLower(pattern)

	// Fast paths for common patterns to avoid recursion/DP overhead.
	// Pure '%' matches everything.
	if pattern == "%" {
		return true
	}
	// %literal% → strings.Contains
	if len(pattern) >= 3 && pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		inner := pattern[1 : len(pattern)-1]
		if !strings.ContainsAny(inner, "%_") {
			return strings.Contains(text, inner)
		}
	}
	// literal% → strings.HasPrefix
	if len(pattern) >= 2 && pattern[len(pattern)-1] == '%' && !strings.ContainsAny(pattern[:len(pattern)-1], "%_") {
		return strings.HasPrefix(text, pattern[:len(pattern)-1])
	}
	// %literal → strings.HasSuffix
	if len(pattern) >= 2 && pattern[0] == '%' && !strings.ContainsAny(pattern[1:], "%_") {
		return strings.HasSuffix(text, pattern[1:])
	}

	// General case: iterative two-pointer match with greedy '%' backtracking.
	// O(n*m) worst case but typically linear for log patterns.
	ti, pi := 0, 0             // text index, pattern index
	starIdx, matchIdx := -1, 0 // last '%' position in pattern, matching text position

	for ti < len(text) {
		if pi < len(pattern) && (pattern[pi] == '_' || pattern[pi] == text[ti]) {
			ti++
			pi++
		} else if pi < len(pattern) && pattern[pi] == '%' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx >= 0 {
			// Backtrack: let the last '%' consume one more character.
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}

	// Consume trailing '%' in pattern.
	for pi < len(pattern) && pattern[pi] == '%' {
		pi++
	}

	return pi == len(pattern)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}

	return 0
}

func toIntValue(v event.Value) event.Value {
	switch v.Type() {
	case event.FieldTypeInt:
		return v
	case event.FieldTypeFloat:
		return event.IntValue(int64(v.AsFloat()))
	case event.FieldTypeString:
		// Try int first, then float
		if n, err := strconv.ParseInt(v.AsString(), 10, 64); err == nil {
			return event.IntValue(n)
		}
		if f, err := strconv.ParseFloat(v.AsString(), 64); err == nil {
			return event.IntValue(int64(f))
		}

		return event.NullValue()
	case event.FieldTypeBool:
		if v.AsBool() {
			return event.IntValue(1)
		}

		return event.IntValue(0)
	default:
		return event.NullValue()
	}
}

func toFloatValue(v event.Value) event.Value {
	switch v.Type() {
	case event.FieldTypeFloat:
		return v
	case event.FieldTypeInt:
		return event.FloatValue(float64(v.AsInt()))
	case event.FieldTypeString:
		if f, err := strconv.ParseFloat(v.AsString(), 64); err == nil {
			return event.FloatValue(f)
		}

		return event.NullValue()
	case event.FieldTypeBool:
		if v.AsBool() {
			return event.FloatValue(1)
		}

		return event.FloatValue(0)
	default:
		return event.NullValue()
	}
}

func substrValue(str, start, length event.Value) event.Value {
	if str.IsNull() || start.IsNull() || length.IsNull() {
		return event.NullValue()
	}
	s := valueToString(str)
	// SPL2 substr is 1-indexed
	startIdx, _ := ValueToFloat(start)
	lenVal, _ := ValueToFloat(length)
	si := int(startIdx) - 1 // convert to 0-indexed
	li := int(lenVal)
	if si < 0 {
		si = 0
	}
	if si >= len(s) {
		return event.StringValue("")
	}
	end := si + li
	if end > len(s) {
		end = len(s)
	}

	return event.StringValue(s[si:end])
}

func roundValue(num, decimals event.Value) event.Value {
	if num.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(num)
	if !ok {
		return event.NullValue()
	}
	d := 0
	if !decimals.IsNull() {
		df, dok := ValueToFloat(decimals)
		if dok {
			d = int(df)
		}
	}
	pow := math.Pow(10, float64(d))

	return event.FloatValue(math.Round(f*pow) / pow)
}

func lnValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok || f <= 0 {
		return event.NullValue()
	}

	return event.FloatValue(math.Log(f))
}

func absValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() == event.FieldTypeInt {
		n := v.AsInt()
		if n < 0 {
			n = -n
		}

		return event.IntValue(n)
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(math.Abs(f))
}

func ceilValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(math.Ceil(f))
}

func floorValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(math.Floor(f))
}

func sqrtValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok || f < 0 {
		return event.NullValue()
	}

	return event.FloatValue(math.Sqrt(f))
}

func strftimeValue(ts, format event.Value) event.Value {
	if ts.IsNull() || format.IsNull() {
		return event.NullValue()
	}
	var t time.Time
	if ts.Type() == event.FieldTypeTimestamp {
		t = ts.AsTimestamp().UTC()
	} else if ts.Type() == event.FieldTypeInt {
		t = time.Unix(ts.AsInt(), 0).UTC()
	} else if f, ok := ValueToFloat(ts); ok {
		t = time.Unix(int64(f), 0).UTC()
	} else {
		return event.NullValue()
	}
	fmtStr := valueToString(format)
	goFmt := splTimeToGo(fmtStr)

	return event.StringValue(t.Format(goFmt))
}

// splTimeReplacer converts SPL2 strftime format tokens to Go time layout tokens.
var splTimeReplacer = strings.NewReplacer(
	"%Y", "2006",
	"%m", "01",
	"%d", "02",
	"%H", "15",
	"%M", "04",
	"%S", "05",
	"%p", "PM",
	"%I", "03",
	"%Z", "MST",
	"%z", "-0700",
	"%b", "Jan",
	"%B", "January",
	"%a", "Mon",
	"%A", "Monday",
)

// splTimeToGo converts SPL2 strftime format to Go time layout.
func splTimeToGo(spl string) string { return splTimeReplacer.Replace(spl) }
