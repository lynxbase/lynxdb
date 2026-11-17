package vm

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/internal/glob"
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

// ErrVMTypeMismatch is returned when a deprecated As*() method panics due to
// a type mismatch. This indicates a compiler bug.
var ErrVMTypeMismatch = errors.New("vm: type mismatch (possible compiler bug)")

// VM is a stack machine that evaluates compiled IR expressions; one VM
// serves any number of events.
type VM struct {
	stack        [StackSize]event.Value
	sp           int
	globCache    []*regexp.Regexp // Program.GlobPatterns compiled; nil for an invalid pattern
	globPatterns []string

	// lambdaParams holds the parameters of the lambdas being evaluated,
	// the innermost last.
	lambdaParams []event.Value

	// Warnings counts runtime warnings such as incompatible comparisons;
	// nil disables counting.
	Warnings *WarningCounters
}

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

// Execute runs a compiled program over the fields of an event and returns
// the value it leaves on the stack.
func (vm *VM) Execute(prog *Program, fields map[string]event.Value) (result event.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrVMTypeMismatch, r)
		}
	}()

	vm.sp = 0
	vm.ensureGlobCache(prog)

	ins := prog.Instructions
	ip := 0

	for ip < len(ins) {
		op := Opcode(ins[ip])
		ip++

		switch op {
		case OpConstStr:
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

		case OpHasTokenGlob:
			newIP, err := vm.execHasTokenGlob(prog, ins, ip)
			if err != nil {
				return event.NullValue(), err
			}
			ip = newIP

		case OpEq:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(valuesEqual(a, b))

		case OpLt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) < 0)

		case OpGt:
			b := vm.stack[vm.sp-1]
			a := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = event.BoolValue(CompareValues(a, b) > 0)

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

		case OpIndex:
			// Stack: [..., container, index] → [..., result]
			idx := vm.stack[vm.sp-1]
			container := vm.stack[vm.sp-2]
			vm.sp--
			vm.stack[vm.sp-1] = indexValue(container, idx)

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

func boolToInt(b bool) int {
	if b {
		return 1
	}

	return 0
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
