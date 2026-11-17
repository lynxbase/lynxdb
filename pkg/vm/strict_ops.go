package vm

// Runtime of the strict opcodes the IR compiler emits: three-valued logic,
// typed comparison and arithmetic without implicit string conversion, and
// case-sensitive string comparison.

import (
	"encoding/binary"
	"math"
	"strings"
	"time"
	"unicode"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// strictCompare implements strict comparison:
// - null/missing operand → null
// - same type: typed comparison (string is lexical CS, numeric, bool ==/!= only, timestamp, duration)
// - int vs float: cross-promote (both are "number" — NOT a type error)
// - incompatible types → null + warning counter increment
//
// Returns (result int, isNull bool). result is -1/0/1; isNull means the
// comparison could not be performed (null operand or incompatible types).
func strictCompare(a, b event.Value, warnings *WarningCounters) (int, bool) {
	if a.IsNull() || b.IsNull() {
		return 0, true
	}

	at, bt := a.Type(), b.Type()

	// Same-type fast paths
	if at == bt {
		switch at {
		case event.FieldTypeString:
			return strings.Compare(a.AsString(), b.AsString()), false
		case event.FieldTypeInt:
			ai, bi := a.AsInt(), b.AsInt()
			if ai < bi {
				return -1, false
			} else if ai > bi {
				return 1, false
			}
			return 0, false
		case event.FieldTypeFloat:
			af, bf := a.AsFloat(), b.AsFloat()
			if af < bf {
				return -1, false
			} else if af > bf {
				return 1, false
			}
			return 0, false
		case event.FieldTypeBool:
			// Bool supports ==/!= only. For < > we treat as incompatible.
			// But the caller (eq/neq) will use this correctly.
			ab, bb := boolToInt(a.AsBool()), boolToInt(b.AsBool())
			return ab - bb, false
		case event.FieldTypeTimestamp:
			at, bt := a.AsTimestamp(), b.AsTimestamp()
			if at.Before(bt) {
				return -1, false
			} else if at.After(bt) {
				return 1, false
			}
			return 0, false
		case event.FieldTypeDuration:
			ad, bd := a.AsDuration(), b.AsDuration()
			if ad < bd {
				return -1, false
			} else if ad > bd {
				return 1, false
			}
			return 0, false
		case event.FieldTypeArray:
			return compareArrays(a.AsArray(), b.AsArray()), false
		case event.FieldTypeObject:
			if objectsEqual(a.AsObject(), b.AsObject()) {
				return 0, false
			}
			return strings.Compare(valueToString(a), valueToString(b)), false
		}
	}

	// int and float compare as numbers.
	if (at == event.FieldTypeInt && bt == event.FieldTypeFloat) ||
		(at == event.FieldTypeFloat && bt == event.FieldTypeInt) {
		af := toFloat64ForCompare(a)
		bf := toFloat64ForCompare(b)
		if af < bf {
			return -1, false
		} else if af > bf {
			return 1, false
		}
		return 0, false
	}

	// Incompatible types → null + warning
	if warnings != nil {
		warnings.Increment(warnIncompatibleTypes)
	}
	return 0, true
}

func toFloat64ForCompare(v event.Value) float64 {
	switch v.Type() {
	case event.FieldTypeInt:
		return float64(v.AsInt())
	case event.FieldTypeFloat:
		return v.AsFloat()
	default:
		return 0
	}
}

// strictEq implements strict equality. Returns (bool, isNull).
func strictEq(a, b event.Value, warnings *WarningCounters) (bool, bool) {
	cmp, isNull := strictCompare(a, b, warnings)
	if isNull {
		return false, true
	}
	return cmp == 0, false
}

// addStrict implements strict addition:
// - null propagation
// - string + string = concat ONLY
// - string + non-string = null + warning (sema catches known cases; runtime is lenient-null)
// - int + int = int
// - int + float or float + float = float
// - timestamp/duration algebra
func addStrict(a, b event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	at, bt := a.Type(), b.Type()

	// String + string = concat
	if at == event.FieldTypeString && bt == event.FieldTypeString {
		return event.StringValue(a.AsString() + b.AsString())
	}
	// String + non-string or non-string + string = null + warning
	if at == event.FieldTypeString || bt == event.FieldTypeString {
		if warnings != nil {
			warnings.Increment(warnStringArithmetic)
		}
		return event.NullValue()
	}
	// int + int
	if at == event.FieldTypeInt && bt == event.FieldTypeInt {
		return event.IntValue(a.AsInt() + b.AsInt())
	}
	// float/float, int/float, float/int → float
	if isNumericType(at) && isNumericType(bt) {
		return event.FloatValue(toFloat64ForCompare(a) + toFloat64ForCompare(b))
	}
	// Timestamp/duration algebra
	if at == event.FieldTypeTimestamp && bt == event.FieldTypeDuration {
		return event.TimestampValue(a.AsTimestamp().Add(b.AsDuration()))
	}
	if at == event.FieldTypeDuration && bt == event.FieldTypeTimestamp {
		return event.TimestampValue(b.AsTimestamp().Add(a.AsDuration()))
	}
	if at == event.FieldTypeDuration && bt == event.FieldTypeDuration {
		return event.DurationValue(a.AsDuration() + b.AsDuration())
	}
	// Incompatible
	if warnings != nil {
		warnings.Increment(warnIncompatibleTypes)
	}
	return event.NullValue()
}

func subStrict(a, b event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	at, bt := a.Type(), b.Type()

	if at == event.FieldTypeInt && bt == event.FieldTypeInt {
		return event.IntValue(a.AsInt() - b.AsInt())
	}
	if isNumericType(at) && isNumericType(bt) {
		return event.FloatValue(toFloat64ForCompare(a) - toFloat64ForCompare(b))
	}
	// ts - ts → dur
	if at == event.FieldTypeTimestamp && bt == event.FieldTypeTimestamp {
		return event.DurationValue(a.AsTimestamp().Sub(b.AsTimestamp()))
	}
	// ts - dur → ts
	if at == event.FieldTypeTimestamp && bt == event.FieldTypeDuration {
		return event.TimestampValue(a.AsTimestamp().Add(-b.AsDuration()))
	}
	// dur - dur → dur
	if at == event.FieldTypeDuration && bt == event.FieldTypeDuration {
		return event.DurationValue(a.AsDuration() - b.AsDuration())
	}
	if warnings != nil {
		warnings.Increment(warnIncompatibleTypes)
	}
	return event.NullValue()
}

func mulStrict(a, b event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	at, bt := a.Type(), b.Type()

	if at == event.FieldTypeInt && bt == event.FieldTypeInt {
		return event.IntValue(a.AsInt() * b.AsInt())
	}
	if isNumericType(at) && isNumericType(bt) {
		return event.FloatValue(toFloat64ForCompare(a) * toFloat64ForCompare(b))
	}
	// dur * number → dur (commutative)
	if at == event.FieldTypeDuration && isNumericType(bt) {
		return event.DurationValue(time.Duration(float64(a.AsDuration()) * toFloat64ForCompare(b)))
	}
	if isNumericType(at) && bt == event.FieldTypeDuration {
		return event.DurationValue(time.Duration(toFloat64ForCompare(a) * float64(b.AsDuration())))
	}
	if warnings != nil {
		warnings.Increment(warnIncompatibleTypes)
	}
	return event.NullValue()
}

func divStrict(a, b event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	at, bt := a.Type(), b.Type()

	// int / int truncates: 5/2 == 2.
	if at == event.FieldTypeInt && bt == event.FieldTypeInt {
		bv := b.AsInt()
		if bv == 0 {
			return event.NullValue()
		}
		return event.IntValue(a.AsInt() / bv)
	}
	// numeric / numeric → float
	if isNumericType(at) && isNumericType(bt) {
		bf := toFloat64ForCompare(b)
		if bf == 0 {
			return event.NullValue()
		}
		return event.FloatValue(toFloat64ForCompare(a) / bf)
	}
	// dur / dur → float
	if at == event.FieldTypeDuration && bt == event.FieldTypeDuration {
		bd := b.AsDuration()
		if bd == 0 {
			return event.NullValue()
		}
		return event.FloatValue(float64(a.AsDuration()) / float64(bd))
	}
	// dur / number → dur
	if at == event.FieldTypeDuration && isNumericType(bt) {
		bf := toFloat64ForCompare(b)
		if bf == 0 {
			return event.NullValue()
		}
		return event.DurationValue(time.Duration(float64(a.AsDuration()) / bf))
	}
	if warnings != nil {
		warnings.Increment(warnIncompatibleTypes)
	}
	return event.NullValue()
}

func modStrict(a, b event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	// % takes ints only; other types give NULL and a warning.
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		bv := b.AsInt()
		if bv == 0 {
			return event.NullValue()
		}
		return event.IntValue(a.AsInt() % bv)
	}
	// dur % dur → dur
	if a.Type() == event.FieldTypeDuration && b.Type() == event.FieldTypeDuration {
		bd := b.AsDuration()
		if bd == 0 {
			return event.NullValue()
		}
		return event.DurationValue(a.AsDuration() % bd)
	}
	if warnings != nil {
		warnings.Increment(warnModOnNonInt)
	}
	return event.NullValue()
}

func negStrict(a event.Value, warnings *WarningCounters) event.Value {
	if a.IsNull() {
		return event.NullValue()
	}
	switch a.Type() {
	case event.FieldTypeInt:
		return event.IntValue(-a.AsInt())
	case event.FieldTypeFloat:
		return event.FloatValue(-a.AsFloat())
	case event.FieldTypeDuration:
		return event.DurationValue(-a.AsDuration())
	default:
		if warnings != nil {
			warnings.Increment(warnIncompatibleTypes)
		}
		return event.NullValue()
	}
}

func isNumericType(t event.FieldType) bool {
	return t == event.FieldTypeInt || t == event.FieldTypeFloat
}

// OpLoadPath: flat-column first, then object walk, no _raw fallback (D25)

// LoadPath resolves a dotted path against fields: the flat column named by the
// full path first, then an object walk from the root member.
func LoadPath(fields map[string]event.Value, path string) event.Value {
	return execLoadPath(fields, path)
}

func execLoadPath(fields map[string]event.Value, path string) event.Value {
	// 1. Flat column lookup (the full dotted path as a literal column name)
	if val, ok := fields[path]; ok {
		return val
	}
	// 2. Object walk: split on '.', load root, walk members
	dot := strings.IndexByte(path, '.')
	if dot <= 0 {
		// No dot or leading dot — field simply missing
		return event.NullValue()
	}
	root := path[:dot]
	rest := path[dot+1:]
	rootVal, ok := fields[root]
	if !ok || rootVal.IsNull() {
		return event.NullValue()
	}
	// Walk the chain
	val := rootVal
	for _, part := range strings.Split(rest, ".") {
		if val.IsNull() {
			return event.NullValue()
		}
		if val.Type() != event.FieldTypeObject {
			return event.NullValue()
		}
		m := val.AsObject()
		next, found := m[part]
		if !found {
			return event.NullValue()
		}
		val = next
	}
	return val
}

// OpHasToken: case-insensitive whole-token match

// hasToken checks if the field string contains the search term as a whole token,
// case-insensitively. A token is a run of ASCII alphanumerics and Unicode
// letters/digits; everything else delimits. Multi-word terms are treated as
// conjunction: all tokens must be present.
func hasToken(field, term string) bool {
	if field == "" || term == "" {
		return false
	}
	fieldLower := strings.ToLower(field)
	termLower := strings.ToLower(term)

	// Tokenize the term to get the tokens we need to find
	termTokens := tokenize(termLower)
	if len(termTokens) == 0 {
		return false
	}

	// Tokenize the field
	fieldTokens := tokenize(fieldLower)
	fieldSet := make(map[string]struct{}, len(fieldTokens))
	for _, t := range fieldTokens {
		fieldSet[t] = struct{}{}
	}

	// All term tokens must be present
	for _, tt := range termTokens {
		if _, ok := fieldSet[tt]; !ok {
			return false
		}
	}
	return true
}

// tokenize splits a string into index tokens:
// runs of ASCII alphanumerics and Unicode letters/digits.
func tokenize(s string) []string {
	var tokens []string
	start := -1
	for i, r := range s {
		isToken := unicode.IsLetter(r) || unicode.IsDigit(r)
		if isToken {
			if start < 0 {
				start = i
			}
		} else {
			if start >= 0 {
				tokens = append(tokens, s[start:i])
				start = -1
			}
		}
	}
	if start >= 0 {
		tokens = append(tokens, s[start:])
	}
	return tokens
}

// OpExtract / OpExtractAll

// OpInStrict: strict equality in-list check with null awareness

// inStrict checks if val is in the list using strict equality.
// If val is null → result is null.
// If any item comparison is null (incompatible types), accumulate null.
// If any item matches → true.
// If no match and no null comparison → false.
// If no match and some null comparison → null (3VL).
func inStrict(val event.Value, items []event.Value, warnings *WarningCounters) event.Value {
	if val.IsNull() {
		return event.NullValue()
	}
	hasNullComparison := false
	for _, item := range items {
		eq, isNull := strictEq(val, item, warnings)
		if isNull {
			hasNullComparison = true
			continue
		}
		if eq {
			return event.BoolValue(true)
		}
	}
	if hasNullComparison {
		return event.NullValue()
	}
	return event.BoolValue(false)
}

// Lambda execution: any, all, filter, map

// execLambdaForElement runs a sub-program against the current row with a
// lambda parameter value pushed onto the lambda param stack. Pool lookups
// (constants, field names, regex, sub-programs) use the root program.
func (vm *VM) execLambdaForElement(rootProg *Program, subIdx int, elem event.Value, fields map[string]event.Value) (event.Value, error) {
	return vm.execLambdaWithParams(rootProg, subIdx, []event.Value{elem}, fields)
}

func (vm *VM) execLambdaWithParams(rootProg *Program, subIdx int, params []event.Value, fields map[string]event.Value) (event.Value, error) {
	sub := rootProg.SubPrograms[subIdx]
	// Build an execution program that uses the sub's instructions but the
	// root's pools. This avoids copying pools to every sub-program.
	execProg := &Program{
		Instructions:   sub.Instructions,
		Constants:      rootProg.Constants,
		FieldNames:     rootProg.FieldNames,
		GlobPatterns:   rootProg.GlobPatterns,
		SubPrograms:    rootProg.SubPrograms,
		Natives:        rootProg.Natives,
		NumericStrings: rootProg.NumericStrings,
	}
	vm.lambdaParams = append(vm.lambdaParams, params...)
	// The sub-program runs from the bottom of the stack, so the caller's
	// operands are saved and restored around it.
	savedSP := vm.sp
	var saved []event.Value
	if savedSP > 0 {
		saved = make([]event.Value, savedSP)
		copy(saved, vm.stack[:savedSP])
	}
	result, err := vm.Execute(execProg, fields)
	copy(vm.stack[:savedSP], saved)
	vm.sp = savedSP
	vm.lambdaParams = vm.lambdaParams[:len(vm.lambdaParams)-len(params)]
	return result, err
}

// execArrayAny implements any(arr, lambda) with 3VL semantics:
// - Empty array: false
// - If any element pred is true: true
// - If no true and any null: null
// - Otherwise: false
func (vm *VM) execArrayAny(prog *Program, subIdx int, arrVal event.Value, fields map[string]event.Value) (event.Value, error) {
	if arrVal.IsNull() {
		return event.NullValue(), nil
	}
	if arrVal.Type() != event.FieldTypeArray {
		return event.NullValue(), nil
	}
	arr := arrVal.AsArray()
	if len(arr) == 0 {
		return event.BoolValue(false), nil
	}
	hasNull := false
	for _, elem := range arr {
		result, err := vm.execLambdaForElement(prog, subIdx, elem, fields)
		if err != nil {
			return event.NullValue(), err
		}
		if result.IsNull() {
			hasNull = true
			continue
		}
		if result.Type() == event.FieldTypeBool && result.AsBool() {
			return event.BoolValue(true), nil
		}
	}
	if hasNull {
		return event.NullValue(), nil
	}
	return event.BoolValue(false), nil
}

// execArrayAll implements all(arr, lambda) with 3VL semantics:
// - Empty array: true
// - If any element pred is false: false
// - If no false and any null: null
// - Otherwise: true
func (vm *VM) execArrayAll(prog *Program, subIdx int, arrVal event.Value, fields map[string]event.Value) (event.Value, error) {
	if arrVal.IsNull() {
		return event.NullValue(), nil
	}
	if arrVal.Type() != event.FieldTypeArray {
		return event.NullValue(), nil
	}
	arr := arrVal.AsArray()
	if len(arr) == 0 {
		return event.BoolValue(true), nil
	}
	hasNull := false
	for _, elem := range arr {
		result, err := vm.execLambdaForElement(prog, subIdx, elem, fields)
		if err != nil {
			return event.NullValue(), err
		}
		if result.IsNull() {
			hasNull = true
			continue
		}
		if result.Type() == event.FieldTypeBool && !result.AsBool() {
			return event.BoolValue(false), nil
		}
	}
	if hasNull {
		return event.NullValue(), nil
	}
	return event.BoolValue(true), nil
}

// execArrayFilter implements filter(arr, lambda):
// - Keeps elements where pred is true (null/false dropped)
// - Empty array: []
func (vm *VM) execArrayFilter(prog *Program, subIdx int, arrVal event.Value, fields map[string]event.Value) (event.Value, error) {
	if arrVal.IsNull() {
		return event.NullValue(), nil
	}
	if arrVal.Type() != event.FieldTypeArray {
		return event.NullValue(), nil
	}
	arr := arrVal.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil), nil
	}
	result := make([]event.Value, 0, len(arr))
	for _, elem := range arr {
		pred, err := vm.execLambdaForElement(prog, subIdx, elem, fields)
		if err != nil {
			return event.NullValue(), err
		}
		if pred.Type() == event.FieldTypeBool && pred.AsBool() {
			result = append(result, elem)
		}
		// null/false: dropped
	}
	return event.ArrayValue(result), nil
}

// execArrayMap implements map(arr, lambda):
// - Applies lambda to each element, and its 0-based index if it takes two
// - null results kept as null elements
// - Empty array: []
func (vm *VM) execArrayMap(prog *Program, subIdx int, arrVal event.Value, fields map[string]event.Value) (event.Value, error) {
	if arrVal.IsNull() {
		return event.NullValue(), nil
	}
	if arrVal.Type() != event.FieldTypeArray {
		return event.NullValue(), nil
	}
	arr := arrVal.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil), nil
	}
	indexed := prog.SubPrograms[subIdx].Params == 2
	result := make([]event.Value, len(arr))
	var params [2]event.Value
	for i, elem := range arr {
		params[0] = elem
		n := 1
		if indexed {
			params[1], n = event.IntValue(int64(i)), 2
		}
		mapped, err := vm.execLambdaWithParams(prog, subIdx, params[:n], fields)
		if err != nil {
			return event.NullValue(), err
		}
		result[i] = mapped
	}
	return event.ArrayValue(result), nil
}

func (vm *VM) execArrayReduce(prog *Program, subIdx int, arrVal, init event.Value, fields map[string]event.Value) (event.Value, error) {
	if arrVal.IsNull() {
		return event.NullValue(), nil
	}
	if arrVal.Type() != event.FieldTypeArray {
		return event.NullValue(), nil
	}
	acc := init
	for _, elem := range arrVal.AsArray() {
		next, err := vm.execLambdaWithParams(prog, subIdx, []event.Value{acc, elem}, fields)
		if err != nil {
			return event.NullValue(), err
		}
		acc = next
	}
	return acc, nil
}

// Slice, ArrayConcat, ArrayDistinct, ArraySort, Flatten

// execArrayDistinct returns order-preserving first-wins deduplicated array.
func execArrayDistinct(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := v.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil)
	}
	// Order-preserving dedup using deep equality (valuesEqual).
	// O(n^2) in worst case but arrays in log analytics are typically small.
	result := make([]event.Value, 0, len(arr))
	for _, elem := range arr {
		found := false
		for _, existing := range result {
			if valuesEqual(elem, existing) {
				found = true
				break
			}
		}
		if !found {
			result = append(result, elem)
		}
	}
	return event.ArrayValue(result)
}

// Object functions: keys, values, merge, has_key

// url_parse, ip_parse, from_json (native)

// Ensure unused imports are referenced.
var _ = binary.BigEndian
var _ = math.Pi
