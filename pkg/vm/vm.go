package vm //nolint:staticcheck // Intentional use of deprecated As*() methods — see VM struct godoc.

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/net/idna"
	"golang.org/x/text/unicode/norm"

	"github.com/lynxbase/lynxdb/internal/glob"
	"github.com/lynxbase/lynxdb/pkg/event"
)

const StackSize = 256

const maxGenerateSeriesElements = 1_000_000

var (
	ErrStackOverflow  = errors.New("vm: stack overflow")
	ErrStackUnderflow = errors.New("vm: stack underflow")
)

// ErrInvalidBytecode is returned when the program contains truncated
// instructions or out-of-range operand indices.
var ErrInvalidBytecode = errors.New("vm: invalid bytecode")

// ErrVMTypeMismatch is returned when a deprecated As*() method panics due to
// a type mismatch. This indicates a compiler bug.
var ErrVMTypeMismatch = errors.New("vm: type mismatch (possible compiler bug)")

// VM is a stack-based virtual machine that evaluates compiled IR expressions.
// The VM is designed to be reused across events with zero allocations on the hot path.
//
// NOTE: This file intentionally uses the deprecated As*() methods (AsInt, AsFloat,
// AsString, AsBool, AsTimestamp) instead of the error-returning As*E() variants.
// All calls occur within type-dispatched opcodes (OpAddInt, OpSubInt, etc.) or
// inside switch v.Type() blocks where the value type is statically guaranteed by
// the compiler. Adding error branches on the hot path would regress the 22ns/op
// benchmark for no safety benefit. If a type mismatch occurs here, it indicates a
// compiler bug — a panic is the correct response.
type VM struct {
	stack         [StackSize]event.Value
	sp            int
	regexCache    []*regexp.Regexp
	regexPatterns []string                             // source patterns for regexCache error messages
	replaceHints  []func(s, replacement string) string // per-regex fast-path; nil = use regex
	matchHints    []func(string) bool                  // per-regex fast-path; nil = use regex
	globCache     []*regexp.Regexp                     // compiled Program.GlobPatterns (glob→regex); nil entry = invalid pattern
	globPatterns  []string                             // source patterns for globCache error messages
	cidrNets      []*net.IPNet                         // lazy-copied from Program.CIDRNets

	// jsonCache is a single-entry cache for JSON dot-notation lookups.
	// When a query accesses multiple paths from the same root (e.g.
	// request.method, request.path, request.duration), the first access
	// unmarshals the root field and caches the top-level keys. Subsequent
	// accesses skip re-parsing. The cache is keyed by the root field's
	// string value — a new event naturally invalidates it because the
	// string value differs.
	jsonCache jsonParseCache

	predicateCtx *PredicateContext

	// lambdaParams is a stack of lambda parameter values, used during
	// OpArrayAny/All/Filter/Map execution. The last element is the current
	// (innermost) lambda parameter. Nested lambdas push additional values.
	// OpLoadLambdaParam reads from this stack by depth: 0 = current (last),
	// 1 = enclosing, etc.
	lambdaParams []event.Value

	// Warnings counts runtime warnings such as incompatible comparisons;
	// nil disables counting.
	Warnings *WarningCounters
}

// PredicateContext carries per-row scan metadata used by predicate opcodes.
type PredicateContext struct {
	RowIndex         uint32
	BSIHandledRows   interface{ Contains(uint32) bool }
	BSIHandledFields map[string]interface{ Contains(uint32) bool }
}

func (ctx *PredicateContext) bsiHandled(field string) bool {
	if ctx == nil {
		return false
	}
	if len(ctx.BSIHandledFields) > 0 {
		if bm := ctx.BSIHandledFields[field]; bm != nil {
			return bm.Contains(ctx.RowIndex)
		}
		return false
	}
	if ctx.BSIHandledRows == nil {
		return false
	}

	return ctx.BSIHandledRows.Contains(ctx.RowIndex)
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
// When a direct field lookup fails, the VM attempts JSON extraction from the
// root field (for dot-notation like `response.status`) or from `_raw` as a
// fallback — enabling field access without an explicit parse/unpack step.
// The fast path (direct lookup) is unchanged.
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
	} else if rawVal, ok := fields["_raw"]; ok && !rawVal.IsNull() {
		// Simple field fallback: try JSON extraction from _raw.
		// Enables `log_type` access without explicit `parse json(_raw)`.
		vm.stack[vm.sp] = vm.jsonExtractCached(rawVal.String(), name)
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
// Falls back to JSON extraction from _raw when the field is not a direct column.
func (vm *VM) execFieldExists(prog *Program, ins []byte, ip int, fields map[string]event.Value) (int, error) {
	if vm.sp >= StackSize {
		return 0, ErrStackOverflow
	}

	idx, err := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
	if err != nil {
		return 0, err
	}

	name := prog.FieldNames[idx]
	_, ok := fields[name]
	exists := ok
	if !ok {
		// JSON fallback: check if the field is extractable from _raw.
		if rawVal, rawOk := fields["_raw"]; rawOk && !rawVal.IsNull() {
			extracted := vm.jsonExtractCached(rawVal.String(), name)
			exists = !extracted.IsNull()
		}
	}
	vm.stack[vm.sp] = event.BoolValue(exists)
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
			if re == nil {
				return 0, fmt.Errorf("regex match: invalid pattern %q", vm.lastProgRegexPattern(idx))
			}
			vm.stack[vm.sp-1] = event.BoolValue(re.MatchString(s))
		}
	}

	return ip + 2, nil
}

// execGlobMatch performs glob matching with bounds checking. Patterns are
// compiled once per program by ensureGlobCache (RFC glob syntax: *, ?, [a-z],
// {a,b}, backslash escapes — portable across platforms, unlike filepath.Match).
func (vm *VM) execGlobMatch(prog *Program, ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(prog.GlobPatterns), "pattern")
	if err != nil {
		return 0, err
	}

	a := vm.stack[vm.sp-1]
	if a.IsNull() {
		vm.stack[vm.sp-1] = event.BoolValue(false)
	} else {
		re := vm.globCache[idx]
		if re == nil {
			return 0, fmt.Errorf("glob match: invalid pattern %q", prog.GlobPatterns[idx])
		}
		vm.stack[vm.sp-1] = event.BoolValue(re.MatchString(valueToString(a)))
	}

	return ip + 2, nil
}

// execHasTokenGlob implements has_glob(field, pattern): case-insensitive
// whole-token glob match. The pattern is lowercased at compile time;
// tokens are lowercased here, so the cached regex stays case-sensitive.
func (vm *VM) execHasTokenGlob(prog *Program, ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(prog.GlobPatterns), "pattern")
	if err != nil {
		return 0, err
	}

	a := vm.stack[vm.sp-1]
	if a.IsNull() {
		vm.stack[vm.sp-1] = event.NullValue()
		return ip + 2, nil
	}
	re := vm.globCache[idx]
	if re == nil {
		return 0, fmt.Errorf("has_glob: invalid pattern %q", prog.GlobPatterns[idx])
	}

	matched := false
	for _, tok := range tokenize(strings.ToLower(valueToString(a))) {
		if re.MatchString(tok) {
			matched = true
			break
		}
	}
	vm.stack[vm.sp-1] = event.BoolValue(matched)

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
			if re == nil {
				return 0, fmt.Errorf("replace: invalid pattern %q", vm.lastProgRegexPattern(idx))
			}
			vm.stack[vm.sp-1] = event.StringValue(re.ReplaceAllString(s, r))
		}
	}

	return ip + 2, nil
}

func (vm *VM) execReplaceFirst(ins []byte, ip int) (int, error) {
	idx, err := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
	if err != nil {
		return 0, err
	}

	replacement := vm.stack[vm.sp-1]
	str := vm.stack[vm.sp-2]
	vm.sp--

	if str.IsNull() {
		vm.stack[vm.sp-1] = event.NullValue()
		return ip + 2, nil
	}
	re := vm.regexCache[idx]
	if re == nil {
		return 0, fmt.Errorf("replace_first: invalid pattern %q", vm.lastProgRegexPattern(idx))
	}
	s := valueToString(str)
	loc := re.FindStringSubmatchIndex(s)
	if loc == nil {
		vm.stack[vm.sp-1] = event.StringValue(s)
		return ip + 2, nil
	}
	repl := re.ExpandString(nil, valueToString(replacement), s, loc)
	vm.stack[vm.sp-1] = event.StringValue(s[:loc[0]] + string(repl) + s[loc[1]:])

	return ip + 2, nil
}

// Execute runs a compiled program against a set of event fields.
// Returns the top-of-stack value. The VM instance can be reused.
func (vm *VM) Execute(prog *Program, fields map[string]event.Value) (result event.Value, err error) {
	return vm.ExecuteWithContext(prog, fields, nil)
}

// ExecuteWithContext runs a compiled program with optional per-row predicate metadata.
func (vm *VM) ExecuteWithContext(prog *Program, fields map[string]event.Value, predicateCtx *PredicateContext) (result event.Value, err error) {
	defer func() {
		vm.predicateCtx = nil
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrVMTypeMismatch, r)
		}
	}()

	vm.sp = 0
	vm.predicateCtx = predicateCtx
	vm.ensureRegexCache(prog)
	vm.ensureGlobCache(prog)
	vm.ensureCIDRCache(prog)

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

		case OpHasTokenGlob:
			newIP, err := vm.execHasTokenGlob(prog, ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpTokens:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execTokens(a)

		case OpReplace:
			newIP, err := vm.execReplace(ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpReplaceFirst:
			newIP, err := vm.execReplaceFirst(ins, ip)
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

		case OpSplitArr:
			// split(s, sep) -> array of strings.
			delim := vm.stack[vm.sp-1]
			str := vm.stack[vm.sp-2]
			vm.sp--
			if str.IsNull() || delim.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				parts := strings.Split(valueToString(str), valueToString(delim))
				elems := make([]event.Value, len(parts))
				for i, p := range parts {
					elems[i] = event.StringValue(p)
				}
				vm.stack[vm.sp-1] = event.ArrayValue(elems)
			}

		case OpSplitRegex:
			regIdx, reErr := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
			if reErr != nil {
				return event.NullValue(), reErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() || a.Type() != event.FieldTypeString {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if vm.regexCache[regIdx] == nil {
				return event.NullValue(), fmt.Errorf("split_regex: invalid pattern %q", vm.lastProgRegexPattern(regIdx))
			} else {
				vm.stack[vm.sp-1] = splitRegexValue(vm.regexCache[regIdx], a.AsString())
			}

		case OpJoinArr:
			// join(arr, sep) -> string; concatenates array elements.
			sep := vm.stack[vm.sp-1]
			arr := vm.stack[vm.sp-2]
			vm.sp--
			if arr.IsNull() || sep.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if arr.Type() == event.FieldTypeArray {
				elems := arr.AsArray()
				parts := make([]string, len(elems))
				for i, e := range elems {
					parts[i] = valueToString(e)
				}
				vm.stack[vm.sp-1] = event.StringValue(strings.Join(parts, valueToString(sep)))
			} else {
				// Fallback for legacy |||‐delimited multivalue strings.
				parts := strings.Split(valueToString(arr), "|||")
				vm.stack[vm.sp-1] = event.StringValue(strings.Join(parts, valueToString(sep)))
			}

		case OpTrim:
			chars := vm.stack[vm.sp-1]
			str := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = trimValue(str, chars, trimBoth)

		case OpLTrim:
			chars := vm.stack[vm.sp-1]
			str := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = trimValue(str, chars, trimLeft)

		case OpRTrim:
			chars := vm.stack[vm.sp-1]
			str := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = trimValue(str, chars, trimRight)

		case OpURLDecode:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = urlDecodeValue(a)

		case OpURLEncode:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = urlEncodeValue(a)

		case OpRegexEscape:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = regexEscapeValue(a)

		case OpNormalizeUTF8:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if (count != 1 && count != 2) || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: normalize_utf8 count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := normalizeUTF8Value(args)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpPunycodeEncode:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = punycodeEncodeValue(a)

		case OpPunycodeDecode:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = punycodeDecodeValue(a)

		case OpCountSubstr:
			sub := vm.stack[vm.sp-1]
			s := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = countSubstrValue(s, sub)

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

		case OpBSIHandledCompare:
			fieldIdx, err := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
			if err != nil {
				return event.NullValue(), err
			}
			target, err := readOperandSafe(ins, ip+2)
			if err != nil {
				return event.NullValue(), err
			}
			if vm.predicateCtx.bsiHandled(prog.FieldNames[fieldIdx]) {
				ip = int(target)
			} else {
				ip += 4
			}

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

		case OpXor:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(IsTruthy(a) != IsTruthy(b))

		case OpNot:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(!IsTruthy(a))

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
			vm.stack[vm.sp-1] = toBoolValue(a)

		case OpArrayAggReduce:
			nameIdx, opErr := readIndexSafe(ins, ip, len(prog.Constants), "constant")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			aggName := valueToString(prog.Constants[nameIdx])
			arr := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayAggReduce(aggName, arr)

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

		case OpExp:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = expValue(a)

		case OpPow:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = powValue(a, b)

		case OpLog:
			base := vm.stack[vm.sp-1]
			value := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = logValue(value, base)

		case OpMathUnary:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = unaryMathValue(a, int(operand))

		case OpMathBinary:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = binaryMathValue(a, b, int(operand))

		case OpIsFinite:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = isFiniteValue(a)

		case OpIsNaN:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = isNaNValue(a)

		case OpBitAnd:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = bitBinaryValue(a, b, bitOpAnd)

		case OpBitOr:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = bitBinaryValue(a, b, bitOpOr)

		case OpBitXor:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = bitBinaryValue(a, b, bitOpXor)

		case OpBitShl:
			count := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = bitShiftValue(a, count, true)

		case OpBitShr:
			count := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = bitShiftValue(a, count, false)

		case OpGreatest:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if int(count) <= 0 || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: greatest count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			result := scalarExtremaValue(vm.stack[vm.sp-int(count):vm.sp], true, vm.Warnings)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpLeast:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if int(count) <= 0 || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: least count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			result := scalarExtremaValue(vm.stack[vm.sp-int(count):vm.sp], false, vm.Warnings)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpRemap:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if (count != 3 && count != 4) || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: remap count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := remapValue(args)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpRandom:
			vm.stack[vm.sp] = event.IntValue(int64(rand.Int31()))
			vm.sp++

		case OpMax:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			if count <= 0 || count > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: max count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			result, err := maxMinValue(vm.stack[vm.sp-count:vm.sp], true)
			if err != nil {
				return event.NullValue(), err
			}
			vm.sp -= count
			vm.stack[vm.sp] = result
			vm.sp++

		case OpMin:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			if count <= 0 || count > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: min count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			result, err := maxMinValue(vm.stack[vm.sp-count:vm.sp], false)
			if err != nil {
				return event.NullValue(), err
			}
			vm.sp -= count
			vm.stack[vm.sp] = result
			vm.sp++

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

		case OpCallNative:
			fn, opErr := readIndexSafe(ins, ip, len(prog.Natives), "native function")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			argc, opErr := readOperandSafe(ins, ip+2)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 4
			n := int(argc)
			if n > vm.sp {
				return event.NullValue(), ErrStackUnderflow
			}
			if n == 0 && vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			res := prog.Natives[fn](vm.stack[vm.sp-n : vm.sp])
			vm.sp -= n
			vm.stack[vm.sp] = res
			vm.sp++

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

		case OpStrftime:
			format := vm.stack[vm.sp-1]
			ts := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = strftimeValue(ts, format)

		case OpStrftimeTZ:
			tz := vm.stack[vm.sp-1]
			format := vm.stack[vm.sp-2]
			ts := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = strftimeValueTZ(ts, format, tz, vm.Warnings)

		case OpStrptime:
			format := vm.stack[vm.sp-1]
			ts := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = strptimeValue(ts, format)

		case OpBin:
			// bin(ts, dur) or bin(x, width): pop width/span (TOS), pop value.
			// Coercion rule:
			//   - number + number → snap to numeric width
			//   - FieldTypeTimestamp → snap directly
			//   - FieldTypeString parseable as RFC3339 → parse, snap, return timestamp
			//   - FieldTypeInt → treat as Unix nanoseconds, snap, return timestamp
			//   - FieldTypeDuration → treat as nanoseconds (e.g. from another bin), snap
			//   - anything else → null
			durVal := vm.stack[vm.sp-1]
			tsVal := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = binValue(tsVal, durVal)

		case OpBinOrigin:
			originVal := vm.stack[vm.sp-1]
			widthVal := vm.stack[vm.sp-2]
			xVal := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = binValueWithOrigin(xVal, widthVal, originVal)

		case OpFromUnix:
			unit := vm.stack[vm.sp-1]
			n := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = fromUnixValue(n, unit, vm.Warnings)

		case OpToUnix:
			unit := vm.stack[vm.sp-1]
			ts := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = toUnixValue(ts, unit, vm.Warnings)

		case OpDateTrunc:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if (count != 2 && count != 3) || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: date_trunc count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := dateTruncValue(args, vm.Warnings)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpDatePart:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if (count != 2 && count != 3) || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: date_part count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := datePartValue(args, vm.Warnings)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpDateDiff:
			part := vm.stack[vm.sp-1]
			b := vm.stack[vm.sp-2]
			a := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = dateDiffValue(a, b, part, vm.Warnings)

		case OpTimestampGuess:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = timestampGuessValue(v)

		case OpBucket:
			bounds := vm.stack[vm.sp-1]
			x := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execBucket(x, bounds)

		case OpPathNormalize:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execPathNormalize(v)

		case OpUserAgentParse:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execUserAgentParse(v)

		case OpTimeOfDay:
			// time_of_day(ts) -> duration since midnight UTC.
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = timeOfDayValue(v, vm.Warnings)

		case OpTimeOfDayTZ:
			// time_of_day(ts, tz) -> duration since local midnight in tz.
			tz := vm.stack[vm.sp-1]
			v := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = timeOfDayValueTZ(v, tz, vm.Warnings)

		case OpDayOfWeek:
			// day_of_week(ts) -> int 0-6 (0=Sunday).
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = dayOfWeekValue(v, vm.Warnings)

		case OpXXHash64:
			// xxhash64(s) -> hex-encoded 64-bit hash string.
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = xxhash64Value(v)

		case OpHumanizeBytes:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execHumanizeBytes(v)

		case OpHumanizeCount:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execHumanizeCount(v)

		case OpHumanizeDur:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execHumanizeDuration(v)

		case OpBar:
			width := vm.stack[vm.sp-1]
			hi := vm.stack[vm.sp-2]
			lo := vm.stack[vm.sp-3]
			x := vm.stack[vm.sp-4]
			vm.sp -= 3
			vm.stack[vm.sp-1] = execBar(x, lo, hi, width)

		case OpIndexOf:
			needle := vm.stack[vm.sp-1]
			container := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execIndexOf(container, needle)

		case OpSplitPart:
			n := vm.stack[vm.sp-1]
			sep := vm.stack[vm.sp-2]
			s := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = execSplitPart(s, sep, n)

		case OpBase64Decode:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execBase64Decode(v)

		case OpBase64Encode:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execBase64Encode(v)

		case OpBase64URLDecode:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execBase64URLDecode(v)

		case OpBase64URLEncode:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execBase64URLEncode(v)

		case OpHex:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execHex(v)

		case OpUnhex:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execUnhex(v)

		case OpRepeat:
			count := vm.stack[vm.sp-1]
			s := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execRepeat(s, count)

		case OpLPad:
			pad := vm.stack[vm.sp-1]
			width := vm.stack[vm.sp-2]
			s := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = execPad(s, width, pad, true)

		case OpRPad:
			pad := vm.stack[vm.sp-1]
			width := vm.stack[vm.sp-2]
			s := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = execPad(s, width, pad, false)

		case OpTranslate:
			to := vm.stack[vm.sp-1]
			from := vm.stack[vm.sp-2]
			s := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = execTranslate(s, from, to)

		case OpLevenshtein:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if (count != 2 && count != 3) || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: levenshtein count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := execLevenshtein(args)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpJaroWinkler:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execJaroWinkler(a, b)

		case OpURLParam:
			name := vm.stack[vm.sp-1]
			rawURL := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execURLParam(rawURL, name)

		case OpURLStrip:
			fragment := vm.stack[vm.sp-1]
			rawURL := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execURLStripQuery(rawURL, fragment)

		case OpURLETLD1:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execURLETLD1(v)

		case OpURLTLD:
			v := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execURLTLD(v)

		case OpMD5:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = hashValue(a, "md5")

		case OpSHA1:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = hashValue(a, "sha1")

		case OpSHA256:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = hashValue(a, "sha256")

		case OpSHA512:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = hashValue(a, "sha512")

		case OpPrintf:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			if count <= 0 || count > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: printf count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			result := printfValue(vm.stack[vm.sp-count : vm.sp])
			vm.sp -= count
			vm.stack[vm.sp] = result
			vm.sp++

		case OpIPMask:
			ip := vm.stack[vm.sp-1]
			mask := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = ipMaskValue(mask, ip)

		case OpSearchMatch:
			// Reserved: no compiler emits 0xC9.
			return event.NullValue(), fmt.Errorf("%w: OpSearchMatch (0xC9) is reserved", ErrInvalidBytecode)

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

		case OpJsonValue:
			// Stack: [..., field, path] → [..., scalar]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonValueValue(field, path)

		case OpJsonQuery:
			// Stack: [..., field, path] → [..., raw_json]
			path := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = jsonQueryValue(field, path)

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

		case OpStartsWith:
			b := vm.stack[vm.sp-1] // prefix
			a := vm.stack[vm.sp-2] // value
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(strings.HasPrefix(valueToString(a), valueToString(b)))
			}

		case OpEndsWith:
			b := vm.stack[vm.sp-1] // suffix
			a := vm.stack[vm.sp-2] // value
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(strings.HasSuffix(valueToString(a), valueToString(b)))
			}

		case OpContains:
			b := vm.stack[vm.sp-1] // substr
			a := vm.stack[vm.sp-2] // value
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(strings.Contains(valueToString(a), valueToString(b)))
			}

		case OpIsNum:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				switch a.Type() {
				case event.FieldTypeInt, event.FieldTypeFloat:
					vm.stack[vm.sp-1] = event.BoolValue(true)
				default:
					_, err := strconv.ParseFloat(valueToString(a), 64)
					vm.stack[vm.sp-1] = event.BoolValue(err == nil)
				}
			}

		case OpIsInt:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				switch a.Type() {
				case event.FieldTypeInt:
					vm.stack[vm.sp-1] = event.BoolValue(true)
				default:
					_, err := strconv.ParseInt(valueToString(a), 10, 64)
					vm.stack[vm.sp-1] = event.BoolValue(err == nil)
				}
			}

		case OpIsBool:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(a.Type() == event.FieldTypeBool)

		case OpIsArray:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(a.Type() == event.FieldTypeArray || isJSONKind(a, '['))

		case OpIsObject:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.BoolValue(a.Type() == event.FieldTypeObject || isJSONKind(a, '{'))

		case OpTypeOf:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = event.StringValue(a.Type().String())

		case OpCIDRMatch:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			idx := int(operand)
			ip += 2
			if idx >= len(vm.cidrNets) {
				return event.NullValue(), fmt.Errorf("%w: CIDR index %d out of range (len=%d)", ErrInvalidBytecode, idx, len(vm.cidrNets))
			}
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				ipStr := valueToString(a)
				parsedIP := net.ParseIP(ipStr)
				if parsedIP == nil {
					vm.stack[vm.sp-1] = event.BoolValue(false)
				} else {
					vm.stack[vm.sp-1] = event.BoolValue(vm.cidrNets[idx].Contains(parsedIP))
				}
			}

		// Typed-value opcodes (duration, array, object).

		case OpConstDuration:
			newIP, err := vm.execConst(prog, ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpArrayBuild:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			if count > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: ArrayBuild count %d exceeds stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			elems := make([]event.Value, count)
			copy(elems, vm.stack[vm.sp-count:vm.sp])
			vm.sp -= count
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = event.ArrayValue(elems)
			vm.sp++

		case OpObjectBuild:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			entryCount := int(operand)
			ip += 2
			pairCount := entryCount * 2
			if pairCount > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: ObjectBuild entry count %d requires %d stack slots, have %d", ErrInvalidBytecode, entryCount, pairCount, vm.sp)
			}
			obj := make(map[string]event.Value, entryCount)
			base := vm.sp - pairCount
			for i := 0; i < entryCount; i++ {
				key := vm.stack[base+i*2]
				val := vm.stack[base+i*2+1]
				obj[valueToString(key)] = val
			}
			vm.sp -= pairCount
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = event.ObjectValue(obj)
			vm.sp++

		case OpIndex:
			// Stack: [..., container, index] → [..., result]
			idx := vm.stack[vm.sp-1]
			container := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = indexValue(container, idx)

		case OpMember:
			// Operand: constant-pool index of the key string.
			constIdx, memErr := readIndexSafe(ins, ip, len(prog.Constants), "constant")
			if memErr != nil {
				return event.NullValue(), memErr
			}
			ip += 2
			obj := vm.stack[vm.sp-1]
			key := valueToString(prog.Constants[constIdx])
			vm.stack[vm.sp-1] = memberValue(obj, key)

		case OpLen:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = lenValue(a)

		// --- strict comparison ---

		case OpEqStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			eq, isNull := strictEq(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(eq)
			}

		case OpNeqStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			eq, isNull := strictEq(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(!eq)
			}

		case OpLtStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			cmp, isNull := strictCompare(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(cmp < 0)
			}

		case OpLteStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			cmp, isNull := strictCompare(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(cmp <= 0)
			}

		case OpGtStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			cmp, isNull := strictCompare(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(cmp > 0)
			}

		case OpGteStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			cmp, isNull := strictCompare(a, b, vm.Warnings)
			if isNull {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(cmp >= 0)
			}

		// --- three-valued logic ---

		case OpNot3VL:
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				// not(null) = null
			} else if a.Type() == event.FieldTypeBool {
				vm.stack[vm.sp-1] = event.BoolValue(!a.AsBool())
			} else {
				// not on non-bool → null + warning
				vm.stack[vm.sp-1] = event.NullValue()
				if vm.Warnings != nil {
					vm.Warnings.Increment(warnNotOnNonBool)
				}
			}

		case OpJumpIfNull3VL:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			target := int(operand)
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.sp-- // pop
				ip = target
			}
			// else leave on stack (not popped) — the caller will use JumpIfFalse/JumpIfTrue next

		case OpAnd3VLNull:
			// Left was null; right is on stack.
			// If right is false → result is false (null AND false = false)
			// If right is true or null → result is null
			right := vm.stack[vm.sp-1]
			if right.Type() == event.FieldTypeBool && !right.AsBool() {
				vm.stack[vm.sp-1] = event.BoolValue(false)
			} else {
				vm.stack[vm.sp-1] = event.NullValue()
			}

		case OpOr3VLNull:
			// Left was null; right is on stack.
			// If right is true → result is true (null OR true = true)
			// If right is false or null → result is null
			right := vm.stack[vm.sp-1]
			if right.Type() == event.FieldTypeBool && right.AsBool() {
				vm.stack[vm.sp-1] = event.BoolValue(true)
			} else {
				vm.stack[vm.sp-1] = event.NullValue()
			}

		// --- strict arithmetic ---

		case OpAddStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			vm.stack[vm.sp-1] = addStrict(a, b, vm.Warnings)

		case OpSubStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			vm.stack[vm.sp-1] = subStrict(a, b, vm.Warnings)

		case OpMulStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			vm.stack[vm.sp-1] = mulStrict(a, b, vm.Warnings)

		case OpDivStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			vm.stack[vm.sp-1] = divStrict(a, b, vm.Warnings)

		case OpModStrict:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			if prog.NumericStrings {
				a, b = numericPair(a, b)
			}
			vm.sp--
			vm.stack[vm.sp-1] = modStrict(a, b, vm.Warnings)

		// --- field paths ---

		case OpLoadPath:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			constIdx, pathErr := readIndexSafe(ins, ip, len(prog.Constants), "constant")
			if pathErr != nil {
				return event.NullValue(), pathErr
			}
			ip += 2
			path := valueToString(prog.Constants[constIdx])
			vm.stack[vm.sp] = execLoadPath(fields, path)
			vm.sp++

		case OpFieldMissing:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			fieldIdx, fmErr := readIndexSafe(ins, ip, len(prog.FieldNames), "field")
			if fmErr != nil {
				return event.NullValue(), fmErr
			}
			ip += 2
			name := prog.FieldNames[fieldIdx]
			_, present := fields[name]
			vm.stack[vm.sp] = event.BoolValue(!present)
			vm.sp++

		case OpNegStrict:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = negStrict(a, vm.Warnings)

		case OpInStrict:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			if count == 0 {
				// Sentinel: RHS is a runtime value (expected to be an array).
				// Stack: [..., val, container]
				container := vm.stack[vm.sp-1]
				val := vm.stack[vm.sp-2]
				vm.sp--
				if container.IsNull() {
					vm.stack[vm.sp-1] = event.NullValue()
				} else if container.Type() == event.FieldTypeArray {
					vm.stack[vm.sp-1] = inStrictMode(val, container.AsArray(), vm.Warnings, prog.NumericStrings)
				} else {
					// Non-array container: treat as single-element check
					eq, isNull := strictEq(val, container, vm.Warnings)
					if isNull {
						vm.stack[vm.sp-1] = event.NullValue()
					} else {
						vm.stack[vm.sp-1] = event.BoolValue(eq)
					}
				}
			} else {
				val := vm.stack[vm.sp-count-1]
				items := make([]event.Value, count)
				copy(items, vm.stack[vm.sp-count:vm.sp])
				vm.sp -= count
				vm.stack[vm.sp-1] = inStrictMode(val, items, vm.Warnings, prog.NumericStrings)
			}

		// --- functions ---

		case OpHasToken:
			b := vm.stack[vm.sp-1] // term
			a := vm.stack[vm.sp-2] // field value
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(hasToken(valueToString(a), valueToString(b)))
			}

		case OpContainsCI:
			b := vm.stack[vm.sp-1] // substr
			a := vm.stack[vm.sp-2] // field value
			vm.sp--
			if a.IsNull() || b.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else {
				vm.stack[vm.sp-1] = event.BoolValue(
					strings.Contains(strings.ToLower(valueToString(a)), strings.ToLower(valueToString(b))),
				)
			}

		case OpHasAny:
			terms := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execHasAny(field, terms, false)

		case OpHasAll:
			terms := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execHasAny(field, terms, true)

		case OpContainsPhrase:
			phrase := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execContainsPhrase(field, phrase)

		case OpContainsAny:
			subs := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execContainsAny(field, subs, false)

		case OpContainsAnyCS:
			subs := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execContainsAny(field, subs, true)

		case OpMatchesAny:
			patterns := vm.stack[vm.sp-1]
			field := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execMatchesAny(field, patterns)

		case OpExtract:
			regIdx, reErr := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
			if reErr != nil {
				return event.NullValue(), reErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if vm.regexCache[regIdx] == nil {
				return event.NullValue(), fmt.Errorf("extract: invalid pattern %q", vm.lastProgRegexPattern(regIdx))
			} else {
				vm.stack[vm.sp-1] = extractFirst(vm.regexCache[regIdx], valueToString(a))
			}

		case OpExtractAll:
			regIdx, reErr := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
			if reErr != nil {
				return event.NullValue(), reErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if vm.regexCache[regIdx] == nil {
				return event.NullValue(), fmt.Errorf("extract_all: invalid pattern %q", vm.lastProgRegexPattern(regIdx))
			} else {
				vm.stack[vm.sp-1] = extractAllMatches(vm.regexCache[regIdx], valueToString(a))
			}

		case OpExtractGroups:
			regIdx, reErr := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
			if reErr != nil {
				return event.NullValue(), reErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() || a.Type() != event.FieldTypeString {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if vm.regexCache[regIdx] == nil {
				return event.NullValue(), fmt.Errorf("extract_groups: invalid pattern %q", vm.lastProgRegexPattern(regIdx))
			} else {
				vm.stack[vm.sp-1] = extractGroups(vm.regexCache[regIdx], a.AsString())
			}

		case OpCountMatches:
			regIdx, reErr := readIndexSafe(ins, ip, len(vm.regexCache), "regex")
			if reErr != nil {
				return event.NullValue(), reErr
			}
			ip += 2
			a := vm.stack[vm.sp-1]
			if a.IsNull() || a.Type() != event.FieldTypeString {
				vm.stack[vm.sp-1] = event.NullValue()
			} else if vm.regexCache[regIdx] == nil {
				return event.NullValue(), fmt.Errorf("count_matches: invalid pattern %q", vm.lastProgRegexPattern(regIdx))
			} else {
				vm.stack[vm.sp-1] = countMatchesValue(vm.regexCache[regIdx], a.AsString())
			}

		case OpSubstr0Based:
			length := vm.stack[vm.sp-1]
			start := vm.stack[vm.sp-2]
			str := vm.stack[vm.sp-3]
			vm.sp -= 2
			vm.stack[vm.sp-1] = substr0Based(str, start, length)

		case OpLoadLambdaParam:
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			depth := int(operand)
			ip += 2
			paramIdx := len(vm.lambdaParams) - 1 - depth
			if paramIdx < 0 || paramIdx >= len(vm.lambdaParams) {
				return event.NullValue(), fmt.Errorf("%w: lambda param depth %d out of range (stack size %d)", ErrInvalidBytecode, depth, len(vm.lambdaParams))
			}
			vm.stack[vm.sp] = vm.lambdaParams[paramIdx]
			vm.sp++

		case OpArrayAny:
			subIdx, opErr := readIndexSafe(ins, ip, len(prog.SubPrograms), "sub-program")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			arr := vm.stack[vm.sp-1]
			vm.sp--
			result, execErr := vm.execArrayAny(prog, subIdx, arr, fields)
			if execErr != nil {
				return event.NullValue(), execErr
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = result
			vm.sp++

		case OpArrayAll:
			subIdx, opErr := readIndexSafe(ins, ip, len(prog.SubPrograms), "sub-program")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			arr := vm.stack[vm.sp-1]
			vm.sp--
			result, execErr := vm.execArrayAll(prog, subIdx, arr, fields)
			if execErr != nil {
				return event.NullValue(), execErr
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = result
			vm.sp++

		case OpArrayFilter:
			subIdx, opErr := readIndexSafe(ins, ip, len(prog.SubPrograms), "sub-program")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			arr := vm.stack[vm.sp-1]
			vm.sp--
			result, execErr := vm.execArrayFilter(prog, subIdx, arr, fields)
			if execErr != nil {
				return event.NullValue(), execErr
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = result
			vm.sp++

		case OpArrayMap:
			subIdx, opErr := readIndexSafe(ins, ip, len(prog.SubPrograms), "sub-program")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			arr := vm.stack[vm.sp-1]
			vm.sp--
			result, execErr := vm.execArrayMap(prog, subIdx, arr, fields)
			if execErr != nil {
				return event.NullValue(), execErr
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = result
			vm.sp++

		case OpArrayReduce:
			subIdx, opErr := readIndexSafe(ins, ip, len(prog.SubPrograms), "sub-program")
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			init := vm.stack[vm.sp-1]
			arr := vm.stack[vm.sp-2]
			vm.sp -= 2
			result, execErr := vm.execArrayReduce(prog, subIdx, arr, init, fields)
			if execErr != nil {
				return event.NullValue(), execErr
			}
			if vm.sp >= StackSize {
				return event.NullValue(), ErrStackOverflow
			}
			vm.stack[vm.sp] = result
			vm.sp++

		case OpSlice:
			vm.execSlice()

		case OpArrayConcat:
			operand, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			count := int(operand)
			ip += 2
			vm.execArrayConcat(count)

		case OpArrayDistinct:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayDistinct(a)

		case OpArraySort:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArraySort(a)

		case OpArraySum:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArraySum(a)

		case OpArrayAvg:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayAvg(a)

		case OpArrayMin:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayExtrema(a, false, vm.Warnings)

		case OpArrayMax:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayExtrema(a, true, vm.Warnings)

		case OpArrayCompact:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayCompact(a)

		case OpArrayDeltas:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayDeltas(a, vm.Warnings)

		case OpArrayCumSum:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execArrayCumSum(a, vm.Warnings)

		case OpGenerateSeries:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if count != 2 && count != 3 {
				return event.NullValue(), fmt.Errorf("%w: generate_series count %d invalid", ErrInvalidBytecode, count)
			}
			if int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: generate_series count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := execGenerateSeries(args)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpZip:
			count, opErr := readOperandSafe(ins, ip)
			if opErr != nil {
				return event.NullValue(), opErr
			}
			ip += 2
			if count == 0 || int(count) > vm.sp {
				return event.NullValue(), fmt.Errorf("%w: zip count %d invalid for stack depth %d", ErrInvalidBytecode, count, vm.sp)
			}
			args := vm.stack[vm.sp-int(count) : vm.sp]
			result := execZip(args)
			vm.sp -= int(count)
			vm.stack[vm.sp] = result
			vm.sp++

		case OpArrayHasAny:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execArrayHasAny(a, b)

		case OpArrayHasAll:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execArrayHasAll(a, b)

		case OpArrayIntersect:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execArrayIntersect(a, b)

		case OpArrayExcept:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execArrayExcept(a, b)

		case OpFlatten:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execFlatten(a)

		case OpKeys:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execKeys(a)

		case OpValues:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execValues(a)

		case OpMerge:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execMerge(a, b)

		case OpHasKey:
			key := vm.stack[vm.sp-1]
			obj := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = execHasKey(obj, key)

		case OpEntries:
			obj := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execEntries(obj)

		case OpFromEntries:
			arr := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execFromEntries(arr)

		case OpURLParse:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execURLParse(a)

		case OpIPParseObj:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execIPParse(a)

		case OpIPToInt:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execIPToInt(a)

		case OpIPFromInt:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execIPFromInt(a)

		case OpFromJSONNative:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execFromJSONNative(a)

		case OpIsJSON:
			a := vm.stack[vm.sp-1]
			vm.stack[vm.sp-1] = execIsJSON(a)

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

// lastProgRegexPattern reports the source pattern for a regex-cache index, for
// error messages when a pattern failed to compile. The cache parallels the
// last program's RegexPatterns pool.
func (vm *VM) lastProgRegexPattern(idx int) string {
	if idx >= 0 && idx < len(vm.regexPatterns) {
		return vm.regexPatterns[idx]
	}
	return "<unknown>"
}

func (vm *VM) ensureRegexCache(prog *Program) {
	n := len(prog.RegexPatterns)
	if len(vm.regexCache) == n &&
		(n == 0 || (len(vm.regexPatterns) == n && &vm.regexPatterns[0] == &prog.RegexPatterns[0])) {
		return
	}
	vm.regexCache = make([]*regexp.Regexp, n)
	vm.regexPatterns = prog.RegexPatterns
	vm.replaceHints = make([]func(s, replacement string) string, n)
	vm.matchHints = make([]func(string) bool, n)
	for i, p := range prog.RegexPatterns {
		// Patterns are validated at compile time (addValidatedRegex); a nil
		// entry here would mean a non-regex slipped into the pool. Compile
		// instead of MustCompile so one bad pattern cannot panic the row and
		// leave the cache half-built with nils for every later index.
		re, err := regexp.Compile(p)
		if err != nil {
			continue
		}
		vm.regexCache[i] = re
		vm.replaceHints[i] = analyzeReplacePattern(p)
		vm.matchHints[i] = analyzeMatchPattern(p)
	}
}

// ensureGlobCache compiles the program's glob patterns into anchored regexes
// via internal/glob. Globs are compiled once per program instead of being
// re-interpreted per row; a nil entry means the pattern's character class is
// invalid and the opcode reports a runtime error.
func (vm *VM) ensureGlobCache(prog *Program) {
	n := len(prog.GlobPatterns)
	if len(vm.globCache) == n &&
		(n == 0 || (len(vm.globPatterns) == n && &vm.globPatterns[0] == &prog.GlobPatterns[0])) {
		return
	}
	vm.globCache = make([]*regexp.Regexp, n)
	vm.globPatterns = prog.GlobPatterns
	for i, p := range prog.GlobPatterns {
		re, err := glob.Compile(p, false)
		if err != nil {
			continue
		}
		vm.globCache[i] = re
	}
}

// ensureCIDRCache lazily copies CIDRNets from the program into the VM.
func (vm *VM) ensureCIDRCache(prog *Program) {
	if len(vm.cidrNets) == len(prog.CIDRNets) {
		return
	}
	vm.cidrNets = make([]*net.IPNet, len(prog.CIDRNets))
	copy(vm.cidrNets, prog.CIDRNets)
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
	case event.FieldTypeDuration:
		return v.AsDuration() != 0
	case event.FieldTypeArray:
		return len(v.AsArray()) > 0
	case event.FieldTypeObject:
		return len(v.AsObject()) > 0
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
	// Mixed: promote to float (int/float/string/bool scalars)
	af, aok := ValueToFloat(a)
	bf, bok := ValueToFloat(b)
	if aok && bok {
		return event.FloatValue(af + bf)
	}
	// timestamp +/- duration, duration + duration
	if a.Type() == event.FieldTypeTimestamp && b.Type() == event.FieldTypeDuration {
		return event.TimestampValue(a.AsTimestamp().Add(b.AsDuration()))
	}
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeTimestamp {
		return event.TimestampValue(b.AsTimestamp().Add(a.AsDuration()))
	}
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeDuration {
		return event.DurationValue(a.AsDuration() + b.AsDuration())
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
	// timestamp - timestamp → duration; timestamp - duration → timestamp;
	// duration - duration → duration
	if a.Type() == event.FieldTypeTimestamp && b.Type() == event.FieldTypeTimestamp {
		return event.DurationValue(a.AsTimestamp().Sub(b.AsTimestamp()))
	}
	if a.Type() == event.FieldTypeTimestamp && b.Type() == event.FieldTypeDuration {
		return event.TimestampValue(a.AsTimestamp().Add(-b.AsDuration()))
	}
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeDuration {
		return event.DurationValue(a.AsDuration() - b.AsDuration())
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
	// duration * number → duration (commutative)
	if a.Type() == event.FieldTypeDuration {
		if bf, ok := ValueToFloat(b); ok {
			return event.DurationValue(time.Duration(float64(a.AsDuration()) * bf))
		}
	}
	if b.Type() == event.FieldTypeDuration {
		if af, ok := ValueToFloat(a); ok {
			return event.DurationValue(time.Duration(af * float64(b.AsDuration())))
		}
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
	// duration / number → duration; duration / duration → float
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeDuration {
		bd := b.AsDuration()
		if bd == 0 {
			return event.NullValue()
		}
		return event.FloatValue(float64(a.AsDuration()) / float64(bd))
	}
	if a.Type() == event.FieldTypeDuration {
		if bf, ok := ValueToFloat(b); ok {
			if bf == 0 {
				return event.NullValue()
			}
			return event.DurationValue(time.Duration(float64(a.AsDuration()) / bf))
		}
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
	// duration % duration → duration
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeDuration {
		bd := b.AsDuration()
		if bd == 0 {
			return event.NullValue()
		}
		return event.DurationValue(a.AsDuration() % bd)
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
		case event.FieldTypeDuration:
			return a.AsDuration() == b.AsDuration()
		case event.FieldTypeArray:
			return arraysEqual(a.AsArray(), b.AsArray())
		case event.FieldTypeObject:
			return objectsEqual(a.AsObject(), b.AsObject())
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

// arraysEqual performs deep, order-sensitive equality on two arrays.
func arraysEqual(a, b []event.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !valuesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// objectsEqual performs deep, key-wise equality on two objects.
func objectsEqual(a, b map[string]event.Value) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok || !valuesEqual(av, bv) {
			return false
		}
	}
	return true
}

// compareArrays performs element-wise three-way comparison.
// Shorter arrays are less than longer ones when elements are equal up to the shorter length.
func compareArrays(a, b []event.Value) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		c := CompareValues(a[i], b[i])
		if c != 0 {
			return c
		}
	}
	if len(a) < len(b) {
		return -1
	} else if len(a) > len(b) {
		return 1
	}
	return 0
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
		case event.FieldTypeDuration:
			ad, bd := a.AsDuration(), b.AsDuration()
			if ad < bd {
				return -1
			} else if ad > bd {
				return 1
			}

			return 0
		case event.FieldTypeArray:
			return compareArrays(a.AsArray(), b.AsArray())
		case event.FieldTypeObject:
			// Objects are only equal or not; no natural ordering.
			// Return 0 for equal, -1 otherwise (consistent arbitrary order for sort stability).
			if objectsEqual(a.AsObject(), b.AsObject()) {
				return 0
			}
			// Deterministic tiebreak: compare their string renderings.
			return strings.Compare(valueToString(a), valueToString(b))
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

func toBoolValue(v event.Value) event.Value {
	switch v.Type() {
	case event.FieldTypeBool:
		return v
	case event.FieldTypeNull:
		return event.NullValue()
	case event.FieldTypeInt:
		return event.BoolValue(v.AsInt() != 0)
	case event.FieldTypeFloat:
		return event.BoolValue(v.AsFloat() != 0)
	case event.FieldTypeString:
		switch strings.ToLower(strings.TrimSpace(v.AsString())) {
		case "true", "t", "1", "yes", "y":
			return event.BoolValue(true)
		case "false", "f", "0", "no", "n", "":
			return event.BoolValue(false)
		default:
			return event.NullValue()
		}
	default:
		return event.NullValue()
	}
}

func substrValue(str, start, length event.Value) event.Value {
	if str.IsNull() || start.IsNull() || length.IsNull() {
		return event.NullValue()
	}
	s := valueToString(str)
	// substr is 1-indexed
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

type trimMode int

const (
	trimBoth trimMode = iota
	trimLeft
	trimRight
)

func trimValue(str, chars event.Value, mode trimMode) event.Value {
	if str.IsNull() || chars.IsNull() {
		return event.NullValue()
	}
	s := valueToString(str)
	cutset := valueToString(chars)
	switch mode {
	case trimLeft:
		return event.StringValue(strings.TrimLeft(s, cutset))
	case trimRight:
		return event.StringValue(strings.TrimRight(s, cutset))
	default:
		return event.StringValue(strings.Trim(s, cutset))
	}
}

func urlDecodeValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	decoded, err := url.QueryUnescape(valueToString(v))
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(decoded)
}

func urlEncodeValue(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}

	return event.StringValue(url.QueryEscape(v.AsString()))
}

func regexEscapeValue(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}

	return event.StringValue(regexp.QuoteMeta(v.AsString()))
}

func normalizeUTF8Value(args []event.Value) event.Value {
	if len(args) != 1 && len(args) != 2 {
		return event.NullValue()
	}
	if args[0].IsNull() || args[0].Type() != event.FieldTypeString {
		return event.NullValue()
	}
	form := "nfc"
	if len(args) == 2 {
		if args[1].IsNull() || args[1].Type() != event.FieldTypeString {
			return event.NullValue()
		}
		form = strings.ToLower(args[1].AsString())
	}

	switch form {
	case "nfc":
		return event.StringValue(norm.NFC.String(args[0].AsString()))
	case "nfd":
		return event.StringValue(norm.NFD.String(args[0].AsString()))
	case "nfkc":
		return event.StringValue(norm.NFKC.String(args[0].AsString()))
	case "nfkd":
		return event.StringValue(norm.NFKD.String(args[0].AsString()))
	default:
		return event.NullValue()
	}
}

func punycodeEncodeValue(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s, err := idna.ToASCII(v.AsString())
	if err != nil {
		return event.NullValue()
	}
	return event.StringValue(s)
}

func punycodeDecodeValue(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s, err := idna.ToUnicode(v.AsString())
	if err != nil {
		return event.NullValue()
	}
	return event.StringValue(s)
}

func countSubstrValue(s, sub event.Value) event.Value {
	if s.IsNull() || sub.IsNull() ||
		s.Type() != event.FieldTypeString ||
		sub.Type() != event.FieldTypeString ||
		sub.AsString() == "" {
		return event.NullValue()
	}

	return event.IntValue(int64(strings.Count(s.AsString(), sub.AsString())))
}

func countMatchesValue(re *regexp.Regexp, s string) event.Value {
	return event.IntValue(int64(len(re.FindAllStringIndex(s, -1))))
}

func splitRegexValue(re *regexp.Regexp, s string) event.Value {
	parts := re.Split(s, -1)
	values := make([]event.Value, 0, len(parts))
	for _, part := range parts {
		values = append(values, event.StringValue(part))
	}

	return event.ArrayValue(values)
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

func expValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(math.Exp(f))
}

func powValue(base, exponent event.Value) event.Value {
	if base.IsNull() || exponent.IsNull() {
		return event.NullValue()
	}
	b, ok := ValueToFloat(base)
	if !ok {
		return event.NullValue()
	}
	e, ok := ValueToFloat(exponent)
	if !ok {
		return event.NullValue()
	}

	return event.FloatValue(math.Pow(b, e))
}

func logValue(value, base event.Value) event.Value {
	if value.IsNull() || base.IsNull() {
		return event.NullValue()
	}
	v, ok := ValueToFloat(value)
	if !ok || v <= 0 {
		return event.NullValue()
	}
	b, ok := ValueToFloat(base)
	if !ok || b <= 0 || b == 1 {
		return event.NullValue()
	}

	return event.FloatValue(math.Log(v) / math.Log(b))
}

func unaryMathValue(v event.Value, fn int) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}
	switch fn {
	case mathFnAcos:
		return event.FloatValue(math.Acos(f))
	case mathFnAcosh:
		return event.FloatValue(math.Acosh(f))
	case mathFnAsin:
		return event.FloatValue(math.Asin(f))
	case mathFnAsinh:
		return event.FloatValue(math.Asinh(f))
	case mathFnAtan:
		return event.FloatValue(math.Atan(f))
	case mathFnAtanh:
		return event.FloatValue(math.Atanh(f))
	case mathFnCos:
		return event.FloatValue(math.Cos(f))
	case mathFnCosh:
		return event.FloatValue(math.Cosh(f))
	case mathFnSin:
		return event.FloatValue(math.Sin(f))
	case mathFnSinh:
		return event.FloatValue(math.Sinh(f))
	case mathFnTan:
		return event.FloatValue(math.Tan(f))
	case mathFnTanh:
		return event.FloatValue(math.Tanh(f))
	default:
		return event.NullValue()
	}
}

func binaryMathValue(a, b event.Value, fn int) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	af, ok := ValueToFloat(a)
	if !ok {
		return event.NullValue()
	}
	bf, ok := ValueToFloat(b)
	if !ok {
		return event.NullValue()
	}
	switch fn {
	case mathFnAtan2:
		return event.FloatValue(math.Atan2(af, bf))
	case mathFnHypot:
		return event.FloatValue(math.Hypot(af, bf))
	default:
		return event.NullValue()
	}
}

func isFiniteValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.BoolValue(!math.IsNaN(f) && !math.IsInf(f, 0))
}

func isNaNValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	f, ok := ValueToFloat(v)
	if !ok {
		return event.NullValue()
	}

	return event.BoolValue(math.IsNaN(f))
}

type bitOp int

const (
	bitOpAnd bitOp = iota
	bitOpOr
	bitOpXor
)

func bitBinaryValue(a, b event.Value, op bitOp) event.Value {
	if a.IsNull() || b.IsNull() ||
		a.Type() != event.FieldTypeInt ||
		b.Type() != event.FieldTypeInt {
		return event.NullValue()
	}
	av := a.AsInt()
	bv := b.AsInt()
	switch op {
	case bitOpAnd:
		return event.IntValue(av & bv)
	case bitOpOr:
		return event.IntValue(av | bv)
	case bitOpXor:
		return event.IntValue(av ^ bv)
	default:
		return event.NullValue()
	}
}

func bitShiftValue(a, count event.Value, left bool) event.Value {
	if a.IsNull() || count.IsNull() ||
		a.Type() != event.FieldTypeInt ||
		count.Type() != event.FieldTypeInt {
		return event.NullValue()
	}
	n := count.AsInt()
	if n < 0 || n > 63 {
		return event.NullValue()
	}
	if left {
		return event.IntValue(a.AsInt() << uint(n))
	}

	return event.IntValue(a.AsInt() >> uint(n))
}

func scalarExtremaValue(values []event.Value, greatest bool, warnings *WarningCounters) event.Value {
	if len(values) == 0 {
		return event.NullValue()
	}
	result := values[0]
	if result.IsNull() {
		return event.NullValue()
	}
	for _, value := range values[1:] {
		cmp, isNull := strictCompare(value, result, warnings)
		if isNull {
			return event.NullValue()
		}
		if (greatest && cmp > 0) || (!greatest && cmp < 0) {
			result = value
		}
	}

	return result
}

func remapValue(args []event.Value) event.Value {
	if len(args) != 3 && len(args) != 4 {
		return event.NullValue()
	}
	from, ok := args[1].TryAsArray()
	if !ok {
		return event.NullValue()
	}
	to, ok := args[2].TryAsArray()
	if !ok || len(from) != len(to) {
		return event.NullValue()
	}
	for i, candidate := range from {
		if valuesEqual(args[0], candidate) {
			return to[i]
		}
	}
	if len(args) == 4 {
		return args[3]
	}
	return event.NullValue()
}

func maxMinValue(values []event.Value, isMax bool) (event.Value, error) {
	if len(values) == 0 {
		return event.NullValue(), fmt.Errorf("%w: max/min requires at least one value", ErrInvalidBytecode)
	}
	result := values[0]
	for _, value := range values[1:] {
		cmp := CompareValues(value, result)
		if (isMax && cmp > 0) || (!isMax && cmp < 0) {
			result = value
		}
	}

	return result, nil
}

func strftimeValue(ts, format event.Value) event.Value {
	return strftimeValueInLocation(ts, format, time.UTC)
}

func strftimeValueTZ(ts, format, tz event.Value, w *WarningCounters) event.Value {
	loc, ok := locationFromValue(tz, w)
	if !ok {
		return event.NullValue()
	}
	return strftimeValueInLocation(ts, format, loc)
}

func strftimeValueInLocation(ts, format event.Value, loc *time.Location) event.Value {
	if ts.IsNull() || format.IsNull() {
		return event.NullValue()
	}
	var t time.Time
	if ts.Type() == event.FieldTypeTimestamp {
		t = ts.AsTimestamp().In(loc)
	} else if ts.Type() == event.FieldTypeInt {
		t = time.Unix(ts.AsInt(), 0).In(loc)
	} else if f, ok := ValueToFloat(ts); ok {
		t = time.Unix(int64(f), 0).In(loc)
	} else {
		return event.NullValue()
	}
	fmtStr := valueToString(format)
	goFmt := strftimeToGo(fmtStr)

	return event.StringValue(t.Format(goFmt))
}

func strptimeValue(ts, format event.Value) event.Value {
	if ts.IsNull() || format.IsNull() {
		return event.NullValue()
	}

	tsStr := valueToString(ts)
	parse := func(format event.Value) event.Value {
		if format.IsNull() || format.Type() != event.FieldTypeString {
			return event.NullValue()
		}
		layout := strftimeToGo(format.AsString())
		t, err := time.ParseInLocation(layout, tsStr, time.UTC)
		if err != nil {
			return event.NullValue()
		}
		return event.IntValue(t.Unix())
	}

	if format.Type() == event.FieldTypeString {
		return parse(format)
	}
	if format.Type() != event.FieldTypeArray {
		return event.NullValue()
	}

	for _, candidate := range format.AsArray() {
		if parsed := parse(candidate); !parsed.IsNull() {
			return parsed
		}
	}
	return event.NullValue()
}

// binValue implements bin(ts, dur) and bin(x, width).
func binValue(xVal, widthVal event.Value) event.Value {
	if xVal.IsNull() || widthVal.IsNull() {
		return event.NullValue()
	}
	if isNumericType(xVal.Type()) && isNumericType(widthVal.Type()) {
		return binNumberValue(xVal, widthVal)
	}
	return binTimestampValue(xVal, widthVal)
}

func binValueWithOrigin(xVal, widthVal, originVal event.Value) event.Value {
	if xVal.IsNull() || widthVal.IsNull() {
		return event.NullValue()
	}
	if originVal.IsNull() {
		return binValue(xVal, widthVal)
	}
	if isNumericType(xVal.Type()) && isNumericType(widthVal.Type()) && isNumericType(originVal.Type()) {
		return binNumberValueWithOrigin(xVal, widthVal, originVal)
	}
	return binTimestampValueWithOrigin(xVal, widthVal, originVal)
}

func binNumberValue(xVal, widthVal event.Value) event.Value {
	if xVal.Type() == event.FieldTypeInt && widthVal.Type() == event.FieldTypeInt {
		x := xVal.AsInt()
		width := widthVal.AsInt()
		if width <= 0 {
			return event.NullValue()
		}
		q := x / width
		if r := x % width; r != 0 && x < 0 {
			q--
		}
		return event.IntValue(q * width)
	}

	x, ok := numericValue(xVal)
	if !ok {
		return event.NullValue()
	}
	width, ok := numericValue(widthVal)
	if !ok || width <= 0 || math.IsNaN(width) || math.IsInf(width, 0) || math.IsNaN(x) || math.IsInf(x, 0) {
		return event.NullValue()
	}
	return event.FloatValue(math.Floor(x/width) * width)
}

func binNumberValueWithOrigin(xVal, widthVal, originVal event.Value) event.Value {
	if xVal.Type() == event.FieldTypeInt && widthVal.Type() == event.FieldTypeInt &&
		originVal.Type() == event.FieldTypeInt {
		x := xVal.AsInt()
		width := widthVal.AsInt()
		origin := originVal.AsInt()
		if width <= 0 {
			return event.NullValue()
		}
		offset := x - origin
		q := offset / width
		if r := offset % width; r != 0 && offset < 0 {
			q--
		}
		return event.IntValue(origin + q*width)
	}

	x, ok := numericValue(xVal)
	if !ok {
		return event.NullValue()
	}
	width, ok := numericValue(widthVal)
	if !ok || width <= 0 || math.IsNaN(width) || math.IsInf(width, 0) || math.IsNaN(x) || math.IsInf(x, 0) {
		return event.NullValue()
	}
	origin, ok := numericValue(originVal)
	if !ok || math.IsNaN(origin) || math.IsInf(origin, 0) {
		return event.NullValue()
	}
	return event.FloatValue(origin + math.Floor((x-origin)/width)*width)
}

func numericValue(v event.Value) (float64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return float64(v.AsInt()), true
	case event.FieldTypeFloat:
		return v.AsFloat(), true
	default:
		return 0, false
	}
}

// binTimestampValue implements bin(ts, dur): snap a timestamp to a duration boundary.
// Coercion rules:
//   - timestamp input → snap directly
//   - string input parseable as RFC3339 → parse, snap, return timestamp
//   - int input → treat as Unix nanoseconds, snap, return timestamp
//   - duration input → treat as nanosecond count, snap, return timestamp
//   - anything else → null
func binTimestampValue(tsVal, durVal event.Value) event.Value {
	if tsVal.IsNull() || durVal.IsNull() {
		return event.NullValue()
	}

	spanNanos, ok := spanNanosFromValue(durVal)
	if !ok {
		return event.NullValue()
	}

	tsNanos, ok := timestampNanosFromValue(tsVal)
	if !ok {
		return event.NullValue()
	}

	bucketNano := (tsNanos / spanNanos) * spanNanos
	return event.TimestampValue(time.Unix(0, bucketNano).UTC())
}

func binTimestampValueWithOrigin(tsVal, durVal, originVal event.Value) event.Value {
	spanNanos, ok := spanNanosFromValue(durVal)
	if !ok {
		return event.NullValue()
	}
	tsNanos, ok := timestampNanosFromValue(tsVal)
	if !ok {
		return event.NullValue()
	}
	originNanos, ok := timestampNanosFromValue(originVal)
	if !ok {
		return event.NullValue()
	}
	offset := tsNanos - originNanos
	q := offset / spanNanos
	if r := offset % spanNanos; r != 0 && offset < 0 {
		q--
	}
	bucketNano := originNanos + q*spanNanos
	return event.TimestampValue(time.Unix(0, bucketNano).UTC())
}

func spanNanosFromValue(v event.Value) (int64, bool) {
	var spanNanos int64
	switch v.Type() {
	case event.FieldTypeDuration:
		d, _ := v.TryAsDuration()
		spanNanos = d.Nanoseconds()
	case event.FieldTypeInt:
		spanNanos = v.AsInt()
	default:
		return 0, false
	}
	return spanNanos, spanNanos > 0
}

func timestampNanosFromValue(v event.Value) (int64, bool) {
	switch v.Type() {
	case event.FieldTypeTimestamp:
		ts, _ := v.TryAsTimestamp()
		return ts.UnixNano(), true
	case event.FieldTypeInt:
		return v.AsInt(), true
	case event.FieldTypeString:
		s, _ := v.TryAsString()
		parsed, ok := tryParseTimestampVM(s)
		if !ok {
			return 0, false
		}
		return parsed.UnixNano(), true
	case event.FieldTypeDuration:
		d, _ := v.TryAsDuration()
		return d.Nanoseconds(), true
	default:
		return 0, false
	}
}

// tryParseTimestampVM tries to parse s as a timestamp using common formats.
func tryParseTimestampVM(s string) (time.Time, bool) {
	for _, fmt := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
	} {
		t, err := time.Parse(fmt, s)
		if err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func timestampGuessValue(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	ts, ok := tryParseTimestampVM(v.AsString())
	if !ok {
		return event.NullValue()
	}
	return event.TimestampValue(ts.UTC())
}

func hashValue(v event.Value, algorithm string) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	data := []byte(valueToString(v))
	switch algorithm {
	case "md5":
		sum := md5.Sum(data)
		return event.StringValue(hex.EncodeToString(sum[:]))
	case "sha1":
		sum := sha1.Sum(data)
		return event.StringValue(hex.EncodeToString(sum[:]))
	case "sha256":
		sum := sha256.Sum256(data)
		return event.StringValue(hex.EncodeToString(sum[:]))
	case "sha512":
		sum := sha512.Sum512(data)
		return event.StringValue(hex.EncodeToString(sum[:]))
	default:
		return event.NullValue()
	}
}

func printfValue(values []event.Value) event.Value {
	if len(values) == 0 || values[0].IsNull() {
		return event.NullValue()
	}
	format := valueToString(values[0])
	args := make([]any, 0, len(values)-1)
	for _, value := range values[1:] {
		args = append(args, valueToInterface(value))
	}

	return event.StringValue(fmt.Sprintf(format, args...))
}

func valueToInterface(v event.Value) any {
	switch v.Type() {
	case event.FieldTypeNull:
		return nil
	case event.FieldTypeString:
		return v.AsString()
	case event.FieldTypeInt:
		return v.AsInt()
	case event.FieldTypeFloat:
		return v.AsFloat()
	case event.FieldTypeBool:
		return v.AsBool()
	case event.FieldTypeTimestamp:
		return v.AsTimestamp()
	default:
		return valueToString(v)
	}
}

// indexValue implements arr[i] (with negative-from-end), obj["k"], and null propagation.
func indexValue(container, idx event.Value) event.Value {
	if container.IsNull() {
		return event.NullValue()
	}
	switch container.Type() {
	case event.FieldTypeArray:
		arr := container.AsArray()
		if idx.IsNull() {
			return event.NullValue()
		}
		i, ok := valueToInt64(idx)
		if !ok {
			return event.NullValue()
		}
		n := int64(len(arr))
		// Negative indexing: -1 = last element
		if i < 0 {
			i += n
		}
		if i < 0 || i >= n {
			return event.NullValue()
		}
		return arr[i]
	case event.FieldTypeObject:
		obj := container.AsObject()
		if idx.IsNull() {
			return event.NullValue()
		}
		key := valueToString(idx)
		if v, ok := obj[key]; ok {
			return v
		}
		return event.NullValue()
	default:
		return event.NullValue()
	}
}

// memberValue implements obj.key with null propagation.
func memberValue(obj event.Value, key string) event.Value {
	if obj.IsNull() {
		return event.NullValue()
	}
	if obj.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	m := obj.AsObject()
	if v, ok := m[key]; ok {
		return v
	}
	return event.NullValue()
}

// lenValue returns the length of a string (rune count) or array (element count).
func lenValue(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	switch v.Type() {
	case event.FieldTypeString:
		return event.IntValue(int64(len([]rune(v.AsString()))))
	case event.FieldTypeArray:
		return event.IntValue(int64(len(v.AsArray())))
	default:
		return event.NullValue()
	}
}

// valueToInt64 converts a value to int64 for array indexing.
func valueToInt64(v event.Value) (int64, bool) {
	switch v.Type() {
	case event.FieldTypeInt:
		return v.AsInt(), true
	case event.FieldTypeFloat:
		f := v.AsFloat()
		i := int64(f)
		if float64(i) == f {
			return i, true
		}
		return 0, false
	default:
		return 0, false
	}
}

func isJSONKind(v event.Value, prefix byte) bool {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return false
	}
	s := strings.TrimSpace(v.AsString())
	if len(s) == 0 || s[0] != prefix {
		return false
	}

	return json.Valid([]byte(s))
}

func ipMaskValue(mask, ip event.Value) event.Value {
	if mask.IsNull() || ip.IsNull() {
		return event.NullValue()
	}
	maskIP := net.ParseIP(valueToString(mask)).To4()
	targetIP := net.ParseIP(valueToString(ip)).To4()
	if maskIP == nil || targetIP == nil {
		return event.NullValue()
	}
	masked := net.IPv4(
		targetIP[0]&maskIP[0],
		targetIP[1]&maskIP[1],
		targetIP[2]&maskIP[2],
		targetIP[3]&maskIP[3],
	)

	return event.StringValue(masked.String())
}

// timeOfDayValue returns the duration since midnight UTC for a timestamp.
// Non-timestamp input returns null and increments the type_error warning.
func timeOfDayValue(v event.Value, w *WarningCounters) event.Value {
	return timeOfDayValueInLocation(v, time.UTC, w)
}

func timeOfDayValueTZ(v, tz event.Value, w *WarningCounters) event.Value {
	loc, ok := locationFromValue(tz, w)
	if !ok {
		return event.NullValue()
	}
	return timeOfDayValueInLocation(v, loc, w)
}

func timeOfDayValueInLocation(v event.Value, loc *time.Location, w *WarningCounters) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	var ts time.Time
	switch v.Type() {
	case event.FieldTypeTimestamp:
		ts = v.AsTimestamp()
	case event.FieldTypeString:
		parsed, err := time.Parse(time.RFC3339Nano, v.AsString())
		if err != nil {
			if w != nil {
				w.Increment(warnTypeError)
			}
			return event.NullValue()
		}
		ts = parsed
	default:
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}
	local := ts.In(loc)
	midnight := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
	return event.DurationValue(local.Sub(midnight))
}

// dayOfWeekValue returns 0-6 (0=Sunday) for a timestamp.
// Non-timestamp input returns null and increments the type_error warning.
func dayOfWeekValue(v event.Value, w *WarningCounters) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	var ts time.Time
	switch v.Type() {
	case event.FieldTypeTimestamp:
		ts = v.AsTimestamp()
	case event.FieldTypeString:
		parsed, err := time.Parse(time.RFC3339Nano, v.AsString())
		if err != nil {
			if w != nil {
				w.Increment(warnTypeError)
			}
			return event.NullValue()
		}
		ts = parsed.UTC()
	default:
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}
	return event.IntValue(int64(ts.UTC().Weekday()))
}

func fromUnixValue(n, unit event.Value, w *WarningCounters) event.Value {
	if n.IsNull() || unit.IsNull() {
		return event.NullValue()
	}
	if n.Type() != event.FieldTypeInt || unit.Type() != event.FieldTypeString {
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}

	sec, nsec, ok := splitUnixEpoch(n.AsInt(), unit.AsString())
	if !ok {
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}
	return event.TimestampValue(time.Unix(sec, nsec).UTC())
}

func toUnixValue(ts, unit event.Value, w *WarningCounters) event.Value {
	if ts.IsNull() || unit.IsNull() {
		return event.NullValue()
	}
	if ts.Type() != event.FieldTypeTimestamp || unit.Type() != event.FieldTypeString {
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}

	t := ts.AsTimestamp()
	switch unit.AsString() {
	case "s":
		return event.IntValue(t.Unix())
	case "ms":
		return event.IntValue(t.Unix()*1_000 + int64(t.Nanosecond())/1_000_000)
	case "us":
		return event.IntValue(t.Unix()*1_000_000 + int64(t.Nanosecond())/1_000)
	case "ns":
		return event.IntValue(t.UnixNano())
	default:
		if w != nil {
			w.Increment(warnTypeError)
		}
		return event.NullValue()
	}
}

func splitUnixEpoch(n int64, unit string) (int64, int64, bool) {
	switch unit {
	case "s":
		return n, 0, true
	case "ms":
		return n / 1_000, (n % 1_000) * int64(time.Millisecond), true
	case "us":
		return n / 1_000_000, (n % 1_000_000) * int64(time.Microsecond), true
	case "ns":
		return n / int64(time.Second), n % int64(time.Second), true
	default:
		return 0, 0, false
	}
}

func dateTruncValue(args []event.Value, w *WarningCounters) event.Value {
	ts, loc, part, ok := calendarArgs(args, w)
	if !ok {
		return event.NullValue()
	}
	t := ts.In(loc)
	year, month, day := t.Date()
	hour, min, _ := t.Clock()

	var out time.Time
	switch part {
	case "minute":
		out = time.Date(year, month, day, hour, min, 0, 0, loc)
	case "hour":
		out = time.Date(year, month, day, hour, 0, 0, 0, loc)
	case "day":
		out = time.Date(year, month, day, 0, 0, 0, 0, loc)
	case "week":
		start := time.Date(year, month, day, 0, 0, 0, 0, loc)
		daysSinceMonday := (int(start.Weekday()) + 6) % 7
		out = start.AddDate(0, 0, -daysSinceMonday)
	case "month":
		out = time.Date(year, month, 1, 0, 0, 0, 0, loc)
	case "quarter":
		qMonth := time.Month(((int(month)-1)/3)*3 + 1)
		out = time.Date(year, qMonth, 1, 0, 0, 0, 0, loc)
	case "year":
		out = time.Date(year, 1, 1, 0, 0, 0, 0, loc)
	default:
		incrementTypeWarning(w)
		return event.NullValue()
	}
	return event.TimestampValue(out.UTC())
}

func datePartValue(args []event.Value, w *WarningCounters) event.Value {
	ts, loc, part, ok := calendarArgs(args, w)
	if !ok {
		return event.NullValue()
	}
	t := ts.In(loc)

	switch part {
	case "hour":
		return event.IntValue(int64(t.Hour()))
	case "minute":
		return event.IntValue(int64(t.Minute()))
	case "day":
		return event.IntValue(int64(t.Day()))
	case "month":
		return event.IntValue(int64(t.Month()))
	case "year":
		return event.IntValue(int64(t.Year()))
	case "dow":
		return event.IntValue(int64(t.Weekday()))
	case "week":
		_, week := t.ISOWeek()
		return event.IntValue(int64(week))
	default:
		incrementTypeWarning(w)
		return event.NullValue()
	}
}

func dateDiffValue(a, b, part event.Value, w *WarningCounters) event.Value {
	start, ok := timestampFromValue(a, w)
	if !ok {
		return event.NullValue()
	}
	end, ok := timestampFromValue(b, w)
	if !ok {
		return event.NullValue()
	}
	if part.IsNull() || part.Type() != event.FieldTypeString {
		incrementTypeWarning(w)
		return event.NullValue()
	}

	delta := end.Sub(start)
	switch strings.ToLower(part.AsString()) {
	case "second":
		return event.IntValue(int64(delta / time.Second))
	case "minute":
		return event.IntValue(int64(delta / time.Minute))
	case "hour":
		return event.IntValue(int64(delta / time.Hour))
	case "day":
		return event.IntValue(int64(delta / (24 * time.Hour)))
	case "week":
		return event.IntValue(int64(delta / (7 * 24 * time.Hour)))
	case "month":
		return event.IntValue(int64(monthDiff(start.UTC(), end.UTC())))
	case "quarter":
		return event.IntValue(int64(monthDiff(start.UTC(), end.UTC()) / 3))
	case "year":
		return event.IntValue(int64(monthDiff(start.UTC(), end.UTC()) / 12))
	default:
		incrementTypeWarning(w)
		return event.NullValue()
	}
}

func calendarArgs(args []event.Value, w *WarningCounters) (time.Time, *time.Location, string, bool) {
	if len(args) != 2 && len(args) != 3 {
		incrementTypeWarning(w)
		return time.Time{}, nil, "", false
	}
	ts, ok := timestampFromValue(args[0], w)
	if !ok {
		return time.Time{}, nil, "", false
	}
	if args[1].IsNull() || args[1].Type() != event.FieldTypeString {
		incrementTypeWarning(w)
		return time.Time{}, nil, "", false
	}
	loc := time.UTC
	if len(args) == 3 {
		var ok bool
		loc, ok = locationFromValue(args[2], w)
		if !ok {
			return time.Time{}, nil, "", false
		}
	}

	return ts, loc, strings.ToLower(args[1].AsString()), true
}

func locationFromValue(v event.Value, w *WarningCounters) (*time.Location, bool) {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		incrementTypeWarning(w)
		return nil, false
	}
	loc, err := time.LoadLocation(v.AsString())
	if err != nil {
		incrementTypeWarning(w)
		return nil, false
	}
	return loc, true
}

func timestampFromValue(v event.Value, w *WarningCounters) (time.Time, bool) {
	if v.IsNull() {
		return time.Time{}, false
	}
	switch v.Type() {
	case event.FieldTypeTimestamp:
		return v.AsTimestamp(), true
	case event.FieldTypeString:
		parsed, err := time.Parse(time.RFC3339Nano, v.AsString())
		if err != nil {
			incrementTypeWarning(w)
			return time.Time{}, false
		}
		return parsed.UTC(), true
	default:
		incrementTypeWarning(w)
		return time.Time{}, false
	}
}

func monthDiff(start, end time.Time) int {
	return (end.Year()-start.Year())*12 + int(end.Month()-start.Month())
}

func incrementTypeWarning(w *WarningCounters) {
	if w != nil {
		w.Increment(warnTypeError)
	}
}

// xxhash64Value returns the hex-encoded 64-bit xxhash of a string value.
// Non-string or null input returns null.
func xxhash64Value(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	s := valueToString(v)
	h := xxhash.Sum64String(s)
	return event.StringValue(strconv.FormatUint(h, 16))
}

// strftimeReplacer converts strftime format tokens to Go time layout tokens.
var strftimeReplacer = strings.NewReplacer(
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

// strftimeToGo converts a strftime format to a Go time layout.
func strftimeToGo(format string) string { return strftimeReplacer.Replace(format) }
