package vm

// RFC-002 LynxFlow strict-semantics opcode runtime implementations.
//
// These functions implement the new opcodes added for the LynxFlow v2 compiler.
// They follow RFC-002 §5.2 (3VL logic), §5.4 (arithmetic/comparison rules),
// and §5.5 (case sensitivity). The old SPL2 opcodes are untouched.

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/publicsuffix"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// Strict comparison (RFC-002 §5.4)

// strictCompare implements RFC-002 strict comparison semantics:
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

	// Cross-type numeric: int vs float (RFC-002: both are "number", cross-promote IS allowed)
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

// strictEq implements RFC-002 strict equality. Returns (bool, isNull).
func strictEq(a, b event.Value, warnings *WarningCounters) (bool, bool) {
	cmp, isNull := strictCompare(a, b, warnings)
	if isNull {
		return false, true
	}
	return cmp == 0, false
}

// Strict arithmetic (RFC-002 §5.4)

// addStrict implements RFC-002 §5.4 addition:
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

	// int / int → TRUNCATING int (5/2 == 2 per §5.4)
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
	// RFC-002 §5.4: % is int-only (other types → null + warning)
	if a.Type() == event.FieldTypeInt && b.Type() == event.FieldTypeInt {
		bv := b.AsInt()
		if bv == 0 {
			return event.NullValue()
		}
		return event.IntValue(a.AsInt() % bv)
	}
	// dur % dur → dur (also valid per §5.4)
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

// OpHasToken: case-insensitive whole-token match per §6.1/6.2

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

func execHasAny(field, terms event.Value, requireAll bool) event.Value {
	if field.IsNull() || terms.IsNull() {
		return event.NullValue()
	}
	items, ok := stringArray(terms)
	if !ok {
		return event.NullValue()
	}
	if len(items) == 0 {
		return event.BoolValue(requireAll)
	}

	s := valueToString(field)
	for _, term := range items {
		matched := hasToken(s, term)
		if requireAll && !matched {
			return event.BoolValue(false)
		}
		if !requireAll && matched {
			return event.BoolValue(true)
		}
	}
	return event.BoolValue(requireAll)
}

func execContainsPhrase(field, phrase event.Value) event.Value {
	if field.IsNull() || phrase.IsNull() {
		return event.NullValue()
	}
	if phrase.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	p := strings.ToLower(phrase.AsString())
	if p == "" {
		return event.BoolValue(false)
	}
	return event.BoolValue(strings.Contains(strings.ToLower(valueToString(field)), p))
}

func execContainsAny(field, subs event.Value, caseSensitive bool) event.Value {
	if field.IsNull() || subs.IsNull() {
		return event.NullValue()
	}
	items, ok := stringArray(subs)
	if !ok {
		return event.NullValue()
	}

	haystack := valueToString(field)
	if !caseSensitive {
		haystack = strings.ToLower(haystack)
	}
	for _, sub := range items {
		if sub == "" {
			continue
		}
		needle := sub
		if !caseSensitive {
			needle = strings.ToLower(needle)
		}
		if strings.Contains(haystack, needle) {
			return event.BoolValue(true)
		}
	}
	return event.BoolValue(false)
}

func execMatchesAny(field, patterns event.Value) event.Value {
	if field.IsNull() || patterns.IsNull() {
		return event.NullValue()
	}
	items, ok := stringArray(patterns)
	if !ok {
		return event.NullValue()
	}

	s := valueToString(field)
	for _, pattern := range items {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return event.NullValue()
		}
		if re.MatchString(s) {
			return event.BoolValue(true)
		}
	}
	return event.BoolValue(false)
}

func stringArray(v event.Value) ([]string, bool) {
	if v.Type() != event.FieldTypeArray {
		return nil, false
	}
	arr := v.AsArray()
	out := make([]string, len(arr))
	for i, elem := range arr {
		if elem.IsNull() || elem.Type() != event.FieldTypeString {
			return nil, false
		}
		out[i] = elem.AsString()
	}
	return out, true
}

// tokenize splits a string into tokens per the tokenizer contract (§6.1):
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

func execTokens(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	rawTokens := tokenize(strings.ToLower(v.AsString()))
	values := make([]event.Value, 0, len(rawTokens))
	for _, token := range rawTokens {
		values = append(values, event.StringValue(token))
	}

	return event.ArrayValue(values)
}

// OpSubstr0Based: 0-based start per RFC-002

func substr0Based(str, start, length event.Value) event.Value {
	if str.IsNull() || start.IsNull() {
		return event.NullValue()
	}
	s := valueToString(str)
	runes := []rune(s)
	n := int64(len(runes))

	si, ok := valueToInt64(start)
	if !ok {
		return event.NullValue()
	}
	// Negative counts from end
	if si < 0 {
		si += n
	}
	if si < 0 {
		si = 0
	}
	if si >= n {
		return event.StringValue("")
	}

	var li int64
	if length.IsNull() {
		li = n - si // to end
	} else {
		l, lok := valueToInt64(length)
		if !lok {
			return event.NullValue()
		}
		li = l
	}
	if li < 0 {
		return event.StringValue("")
	}
	end := si + li
	if end > n {
		end = n
	}
	return event.StringValue(string(runes[si:end]))
}

// OpExtract / OpExtractAll

func extractFirst(re *regexp.Regexp, s string) event.Value {
	if re.NumSubexp() == 0 {
		// No capture groups, return the full match
		m := re.FindString(s)
		if m == "" {
			return event.NullValue()
		}
		return event.StringValue(m)
	}
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return event.NullValue()
	}
	if len(matches) > 1 {
		return event.StringValue(matches[1])
	}
	return event.NullValue()
}

func extractAllMatches(re *regexp.Regexp, s string) event.Value {
	if re.NumSubexp() == 0 {
		matches := re.FindAllString(s, -1)
		if matches == nil {
			return event.NullValue()
		}
		elems := make([]event.Value, len(matches))
		for i, m := range matches {
			elems[i] = event.StringValue(m)
		}
		return event.ArrayValue(elems)
	}
	all := re.FindAllStringSubmatch(s, -1)
	if all == nil {
		return event.NullValue()
	}
	elems := make([]event.Value, len(all))
	for i, m := range all {
		if len(m) > 1 {
			elems[i] = event.StringValue(m[1])
		} else {
			elems[i] = event.NullValue()
		}
	}
	return event.ArrayValue(elems)
}

func extractGroups(re *regexp.Regexp, s string) event.Value {
	matches := re.FindStringSubmatch(s)
	if matches == nil {
		return event.NullValue()
	}
	names := re.SubexpNames()
	fields := make(map[string]event.Value)
	for i := 1; i < len(matches) && i < len(names); i++ {
		if names[i] == "" {
			continue
		}
		fields[names[i]] = event.StringValue(matches[i])
	}

	return event.ObjectValue(fields)
}

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

// Strict-cast bang error

// ErrStrictCast is returned when a strict-cast function (e.g. int!()) fails.
// The error includes the function name and the value that could not be cast,
// providing row context for debugging. The VM returns this error, halting the
// query — this is the designed behavior for bang variants per RFC-002 §5.4.
type ErrStrictCast struct {
	Func  string // e.g. "int!"
	Value string // string representation of the offending value
	Type  string // type of the offending value
}

func (e *ErrStrictCast) Error() string {
	return fmt.Sprintf("strict cast %s failed: cannot convert %s (type %s)", e.Func, e.Value, e.Type)
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
		RegexPatterns:  rootProg.RegexPatterns,
		GlobPatterns:   rootProg.GlobPatterns,
		CIDRNets:       rootProg.CIDRNets,
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
	ctx := vm.predicateCtx
	result, err := vm.ExecuteWithContext(execProg, fields, ctx)
	copy(vm.stack[:savedSP], saved)
	vm.sp = savedSP
	vm.predicateCtx = ctx
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
// - Applies lambda to each element, collecting results
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
	result := make([]event.Value, len(arr))
	for i, elem := range arr {
		mapped, err := vm.execLambdaForElement(prog, subIdx, elem, fields)
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

func execArrayAggReduce(name string, arrVal event.Value) event.Value {
	if arrVal.IsNull() || arrVal.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := arrVal.AsArray()
	switch strings.ToLower(name) {
	case "p50":
		return arrayPercentile(arr, 50)
	case "p75":
		return arrayPercentile(arr, 75)
	case "p90":
		return arrayPercentile(arr, 90)
	case "p95":
		return arrayPercentile(arr, 95)
	case "p99":
		return arrayPercentile(arr, 99)
	case "dc":
		return event.IntValue(arrayDistinctCount(arr))
	case "value_counts":
		return arrayValueCounts(arr)
	case "entropy":
		return arrayEntropy(arr)
	case "mode":
		return arrayMode(arr)
	case "stdev":
		return arrayVariance(arr, true)
	case "var":
		return arrayVariance(arr, false)
	case "mad":
		return arrayMAD(arr)
	default:
		return event.NullValue()
	}
}

func arrayPercentile(arr []event.Value, pct float64) event.Value {
	values := numericArrayValues(arr)
	if len(values) == 0 {
		return event.NullValue()
	}
	sort.Float64s(values)
	idx := pct / 100 * float64(len(values)-1)
	lower := int(idx)
	if lower >= len(values)-1 {
		return event.FloatValue(values[len(values)-1])
	}
	frac := idx - float64(lower)
	return event.FloatValue(values[lower] + frac*(values[lower+1]-values[lower]))
}

func arrayDistinctCount(arr []event.Value) int64 {
	seen := map[string]struct{}{}
	for _, elem := range arr {
		if elem.IsNull() {
			continue
		}
		seen[elem.String()] = struct{}{}
	}
	return int64(len(seen))
}

type arrayCountItem struct {
	value event.Value
	count int64
}

func arrayValueCounts(arr []event.Value) event.Value {
	counts := map[string]arrayCountItem{}
	for _, elem := range arr {
		if elem.IsNull() {
			continue
		}
		key := arrayAggKey(elem)
		item := counts[key]
		if item.count == 0 {
			item.value = elem
		}
		item.count++
		counts[key] = item
	}
	if len(counts) == 0 {
		return event.ArrayValue(nil)
	}
	items := make([]arrayCountItem, 0, len(counts))
	for _, item := range counts {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		return items[i].value.String() < items[j].value.String()
	})

	result := make([]event.Value, 0, len(items))
	for _, item := range items {
		result = append(result, event.ObjectValue(map[string]event.Value{
			"value": item.value,
			"count": event.IntValue(item.count),
		}))
	}
	return event.ArrayValue(result)
}

func arrayEntropy(arr []event.Value) event.Value {
	counts := map[string]int64{}
	var total int64
	for _, elem := range arr {
		if elem.IsNull() {
			continue
		}
		counts[arrayAggKey(elem)]++
		total++
	}
	if total == 0 {
		return event.NullValue()
	}
	var entropy float64
	for _, count := range counts {
		p := float64(count) / float64(total)
		entropy -= p * math.Log2(p)
	}
	return event.FloatValue(entropy)
}

func arrayMode(arr []event.Value) event.Value {
	counts := map[string]int64{}
	for _, elem := range arr {
		if elem.IsNull() {
			continue
		}
		counts[elem.String()]++
	}

	var best string
	var bestCount int64
	hasBest := false
	for value, count := range counts {
		if !hasBest || count > bestCount || (count == bestCount && value < best) {
			best = value
			bestCount = count
			hasBest = true
		}
	}
	if !hasBest {
		return event.NullValue()
	}
	return event.StringValue(best)
}

func arrayVariance(arr []event.Value, root bool) event.Value {
	values := numericArrayValues(arr)
	if len(values) < 2 {
		return event.NullValue()
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	mean := sum / float64(len(values))

	var sumSq float64
	for _, value := range values {
		diff := value - mean
		sumSq += diff * diff
	}
	variance := sumSq / float64(len(values)-1)
	if root {
		return event.FloatValue(math.Sqrt(variance))
	}
	return event.FloatValue(variance)
}

func arrayMAD(arr []event.Value) event.Value {
	values := numericArrayValues(arr)
	if len(values) == 0 {
		return event.NullValue()
	}
	sort.Float64s(values)
	median := medianSorted(values)

	deviations := make([]float64, len(values))
	for i, value := range values {
		deviations[i] = math.Abs(value - median)
	}
	sort.Float64s(deviations)
	return event.FloatValue(medianSorted(deviations))
}

func numericArrayValues(arr []event.Value) []float64 {
	values := make([]float64, 0, len(arr))
	for _, elem := range arr {
		if f, ok := ValueToFloat(elem); ok {
			values = append(values, f)
		}
	}
	return values
}

func medianSorted(values []float64) float64 {
	mid := len(values) / 2
	if len(values)%2 == 1 {
		return values[mid]
	}
	return (values[mid-1] + values[mid]) / 2
}

func arrayAggKey(value event.Value) string {
	return value.Type().String() + "\x00" + value.String()
}

// Slice, ArrayConcat, ArrayDistinct, ArraySort, Flatten

// execSlice implements slice(arr|string, start[, end]).
// Stack: [..., x, start, end] or [..., x, start] (end=null means to end).
// 0-based indexing, negative from end, clamped.
func (vm *VM) execSlice() {
	endVal := vm.stack[vm.sp-1]
	startVal := vm.stack[vm.sp-2]
	container := vm.stack[vm.sp-3]
	vm.sp -= 2

	if container.IsNull() || startVal.IsNull() {
		vm.stack[vm.sp-1] = event.NullValue()
		return
	}
	if container.Type() != event.FieldTypeArray && container.Type() != event.FieldTypeString {
		vm.stack[vm.sp-1] = event.NullValue()
		return
	}

	var arr []event.Value
	var runes []rune
	var n int64
	if container.Type() == event.FieldTypeString {
		runes = []rune(container.AsString())
		n = int64(len(runes))
	} else {
		arr = container.AsArray()
		n = int64(len(arr))
	}

	si, ok := valueToInt64(startVal)
	if !ok {
		vm.stack[vm.sp-1] = event.NullValue()
		return
	}
	if si < 0 {
		si += n
	}
	if si < 0 {
		si = 0
	}
	if si > n {
		si = n
	}

	var ei int64
	if endVal.IsNull() {
		ei = n
	} else {
		e, eok := valueToInt64(endVal)
		if !eok {
			vm.stack[vm.sp-1] = event.NullValue()
			return
		}
		ei = e
		if ei < 0 {
			ei += n
		}
		if ei < 0 {
			ei = 0
		}
		if ei > n {
			ei = n
		}
	}

	if si >= ei {
		if container.Type() == event.FieldTypeString {
			vm.stack[vm.sp-1] = event.StringValue("")
		} else {
			vm.stack[vm.sp-1] = event.ArrayValue(nil)
		}
		return
	}

	if container.Type() == event.FieldTypeString {
		vm.stack[vm.sp-1] = event.StringValue(string(runes[si:ei]))
		return
	}

	result := make([]event.Value, ei-si)
	copy(result, arr[si:ei])
	vm.stack[vm.sp-1] = event.ArrayValue(result)
}

// execArrayConcat implements array_concat(variadic).
func (vm *VM) execArrayConcat(count int) {
	if count == 0 {
		vm.stack[vm.sp] = event.ArrayValue(nil)
		vm.sp++
		return
	}
	var result []event.Value
	for i := vm.sp - count; i < vm.sp; i++ {
		v := vm.stack[i]
		if v.IsNull() {
			continue
		}
		if v.Type() == event.FieldTypeArray {
			result = append(result, v.AsArray()...)
		}
	}
	vm.sp -= count
	if result == nil {
		result = []event.Value{}
	}
	vm.stack[vm.sp] = event.ArrayValue(result)
	vm.sp++
}

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

// execArraySort sorts using CompareValues order; nulls last.
func execArraySort(v event.Value) event.Value {
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
	// Copy to avoid mutating the original
	sorted := make([]event.Value, len(arr))
	copy(sorted, arr)
	sort.SliceStable(sorted, func(i, j int) bool {
		ai, aj := sorted[i], sorted[j]
		// Nulls last
		if ai.IsNull() && aj.IsNull() {
			return false
		}
		if ai.IsNull() {
			return false
		}
		if aj.IsNull() {
			return true
		}
		return CompareValues(ai, aj) < 0
	})
	return event.ArrayValue(sorted)
}

func execArraySum(v event.Value) event.Value {
	arr, ok := numericArray(v)
	if !ok {
		return event.NullValue()
	}
	if len(arr) == 0 {
		return event.IntValue(0)
	}

	var sumInt int64
	var sumFloat float64
	hasFloat := false
	for _, elem := range arr {
		switch elem.Type() {
		case event.FieldTypeInt:
			if hasFloat {
				sumFloat += float64(elem.AsInt())
			} else {
				sumInt += elem.AsInt()
			}
		case event.FieldTypeFloat:
			if !hasFloat {
				sumFloat = float64(sumInt)
				hasFloat = true
			}
			sumFloat += elem.AsFloat()
		}
	}
	if hasFloat {
		return event.FloatValue(sumFloat)
	}
	return event.IntValue(sumInt)
}

func execArrayAvg(v event.Value) event.Value {
	arr, ok := numericArray(v)
	if !ok || len(arr) == 0 {
		return event.NullValue()
	}

	var sum float64
	for _, elem := range arr {
		if elem.Type() == event.FieldTypeInt {
			sum += float64(elem.AsInt())
		} else {
			sum += elem.AsFloat()
		}
	}
	return event.FloatValue(sum / float64(len(arr)))
}

func execArrayExtrema(v event.Value, greatest bool, warnings *WarningCounters) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := v.AsArray()
	if len(arr) == 0 {
		return event.NullValue()
	}

	return scalarExtremaValue(arr, greatest, warnings)
}

func numericArray(v event.Value) ([]event.Value, bool) {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return nil, false
	}
	arr := v.AsArray()
	for _, elem := range arr {
		switch elem.Type() {
		case event.FieldTypeInt, event.FieldTypeFloat:
		default:
			return nil, false
		}
	}
	return arr, true
}

func execArrayCompact(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := v.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil)
	}

	result := make([]event.Value, 0, len(arr))
	result = append(result, arr[0])
	for _, elem := range arr[1:] {
		if !valuesEqual(elem, result[len(result)-1]) {
			result = append(result, elem)
		}
	}
	return event.ArrayValue(result)
}

func execArrayDeltas(v event.Value, warnings *WarningCounters) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := v.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil)
	}

	result := make([]event.Value, len(arr))
	result[0] = event.NullValue()
	for i := 1; i < len(arr); i++ {
		delta := subStrict(arr[i], arr[i-1], warnings)
		if delta.IsNull() {
			return event.NullValue()
		}
		result[i] = delta
	}
	return event.ArrayValue(result)
}

func execArrayCumSum(v event.Value, warnings *WarningCounters) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	arr := v.AsArray()
	if len(arr) == 0 {
		return event.ArrayValue(nil)
	}
	if arr[0].IsNull() {
		return event.NullValue()
	}

	result := make([]event.Value, len(arr))
	total := arr[0]
	result[0] = total
	for i := 1; i < len(arr); i++ {
		total = addStrict(total, arr[i], warnings)
		if total.IsNull() {
			return event.NullValue()
		}
		result[i] = total
	}
	return event.ArrayValue(result)
}

func execGenerateSeries(values []event.Value) event.Value {
	if len(values) != 2 && len(values) != 3 {
		return event.NullValue()
	}
	if values[0].Type() == event.FieldTypeTimestamp || values[1].Type() == event.FieldTypeTimestamp {
		return execTimestampGenerateSeries(values)
	}
	start, ok := values[0].TryAsInt()
	if !ok {
		return event.NullValue()
	}
	stop, ok := values[1].TryAsInt()
	if !ok {
		return event.NullValue()
	}
	step := int64(1)
	if len(values) == 3 {
		step, ok = values[2].TryAsInt()
		if !ok || step == 0 {
			return event.NullValue()
		}
	}
	if step > 0 && start >= stop || step < 0 && start <= stop {
		return event.ArrayValue(nil)
	}

	const maxInt64 = int64(^uint64(0) >> 1)
	const minInt64 = -maxInt64 - 1

	result := make([]event.Value, 0)
	for current := start; ; {
		if step > 0 {
			if current >= stop {
				break
			}
		} else if current <= stop {
			break
		}
		if len(result) >= maxGenerateSeriesElements {
			return event.NullValue()
		}
		result = append(result, event.IntValue(current))
		if step > 0 && current > maxInt64-step {
			return event.NullValue()
		}
		if step < 0 && current < minInt64-step {
			return event.NullValue()
		}
		current += step
	}

	return event.ArrayValue(result)
}

func execTimestampGenerateSeries(values []event.Value) event.Value {
	start, ok := values[0].TryAsTimestamp()
	if !ok {
		return event.NullValue()
	}
	stop, ok := values[1].TryAsTimestamp()
	if !ok {
		return event.NullValue()
	}
	if len(values) != 3 {
		return event.NullValue()
	}
	step, ok := values[2].TryAsDuration()
	if !ok || step == 0 {
		return event.NullValue()
	}
	if step > 0 && !start.Before(stop) || step < 0 && !start.After(stop) {
		return event.ArrayValue(nil)
	}

	result := make([]event.Value, 0)
	for current := start; ; {
		if step > 0 {
			if !current.Before(stop) {
				break
			}
		} else if !current.After(stop) {
			break
		}
		if len(result) >= maxGenerateSeriesElements {
			return event.NullValue()
		}
		result = append(result, event.TimestampValue(current))

		next := current.Add(step)
		if step > 0 && !next.After(current) || step < 0 && !next.Before(current) {
			return event.NullValue()
		}
		current = next
	}

	return event.ArrayValue(result)
}

func execZip(values []event.Value) event.Value {
	if len(values) == 0 {
		return event.ArrayValue(nil)
	}

	arrays := make([][]event.Value, len(values))
	length := -1
	for i, value := range values {
		if value.IsNull() || value.Type() != event.FieldTypeArray {
			return event.NullValue()
		}
		arr := value.AsArray()
		if length == -1 {
			length = len(arr)
		} else if len(arr) != length {
			return event.NullValue()
		}
		arrays[i] = arr
	}

	result := make([]event.Value, length)
	for i := 0; i < length; i++ {
		row := make([]event.Value, len(arrays))
		for j, arr := range arrays {
			row[j] = arr[i]
		}
		result[i] = event.ArrayValue(row)
	}
	return event.ArrayValue(result)
}

func execEntries(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	obj := v.AsObject()
	keys := make([]string, 0, len(obj))
	for key := range obj {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	entries := make([]event.Value, len(keys))
	for i, key := range keys {
		entries[i] = event.ObjectValue(map[string]event.Value{
			"key":   event.StringValue(key),
			"value": obj[key],
		})
	}
	return event.ArrayValue(entries)
}

func execFromEntries(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	result := make(map[string]event.Value)
	for _, entry := range v.AsArray() {
		if entry.IsNull() || entry.Type() != event.FieldTypeObject {
			return event.NullValue()
		}
		obj := entry.AsObject()
		key, ok := obj["key"]
		if !ok || key.Type() != event.FieldTypeString {
			return event.NullValue()
		}
		value, ok := obj["value"]
		if !ok {
			return event.NullValue()
		}
		result[key.AsString()] = value
	}
	return event.ObjectValue(result)
}

func execArrayHasAny(a, b event.Value) event.Value {
	aArr, bArr, ok := arrayPair(a, b)
	if !ok {
		return event.NullValue()
	}
	for _, required := range bArr {
		if arrayContains(aArr, required) {
			return event.BoolValue(true)
		}
	}

	return event.BoolValue(false)
}

func execArrayHasAll(a, b event.Value) event.Value {
	aArr, bArr, ok := arrayPair(a, b)
	if !ok {
		return event.NullValue()
	}
	for _, required := range bArr {
		if !arrayContains(aArr, required) {
			return event.BoolValue(false)
		}
	}

	return event.BoolValue(true)
}

func execArrayIntersect(a, b event.Value) event.Value {
	aArr, bArr, ok := arrayPair(a, b)
	if !ok {
		return event.NullValue()
	}
	result := make([]event.Value, 0, len(aArr))
	for _, item := range aArr {
		if arrayContains(bArr, item) {
			result = append(result, item)
		}
	}

	return event.ArrayValue(result)
}

func execArrayExcept(a, b event.Value) event.Value {
	aArr, bArr, ok := arrayPair(a, b)
	if !ok {
		return event.NullValue()
	}
	result := make([]event.Value, 0, len(aArr))
	for _, item := range aArr {
		if !arrayContains(bArr, item) {
			result = append(result, item)
		}
	}

	return event.ArrayValue(result)
}

func arrayPair(a, b event.Value) ([]event.Value, []event.Value, bool) {
	if a.IsNull() || b.IsNull() || a.Type() != event.FieldTypeArray || b.Type() != event.FieldTypeArray {
		return nil, nil, false
	}

	return a.AsArray(), b.AsArray(), true
}

func arrayContains(arr []event.Value, needle event.Value) bool {
	for _, item := range arr {
		if valuesEqual(item, needle) {
			return true
		}
	}

	return false
}

// execFlatten flattens one level of nested arrays.
func execFlatten(v event.Value) event.Value {
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
	var result []event.Value
	for _, elem := range arr {
		if elem.Type() == event.FieldTypeArray {
			result = append(result, elem.AsArray()...)
		} else {
			result = append(result, elem)
		}
	}
	return event.ArrayValue(result)
}

// Object functions: keys, values, merge, has_key

// execKeys returns sorted array of key strings.
func execKeys(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	obj := v.AsObject()
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := make([]event.Value, len(keys))
	for i, k := range keys {
		result[i] = event.StringValue(k)
	}
	return event.ArrayValue(result)
}

// execValues returns array of values in key-sorted order (deterministic).
func execValues(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	obj := v.AsObject()
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := make([]event.Value, len(keys))
	for i, k := range keys {
		result[i] = obj[k]
	}
	return event.ArrayValue(result)
}

// execMerge merges two objects (right wins on key collision).
func execMerge(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() {
		return event.NullValue()
	}
	if a.Type() != event.FieldTypeObject || b.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	aObj := a.AsObject()
	bObj := b.AsObject()
	result := make(map[string]event.Value, len(aObj)+len(bObj))
	for k, v := range aObj {
		result[k] = v
	}
	for k, v := range bObj {
		result[k] = v // right wins
	}
	return event.ObjectValue(result)
}

// execHasKey checks if an object has a key.
func execHasKey(obj, key event.Value) event.Value {
	if obj.IsNull() || key.IsNull() {
		return event.NullValue()
	}
	if obj.Type() != event.FieldTypeObject {
		return event.NullValue()
	}
	k := valueToString(key)
	_, ok := obj.AsObject()[k]
	return event.BoolValue(ok)
}

// url_parse, ip_parse, from_json (native)

// execURLParse parses a URL into an object.
func execURLParse(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s := v.AsString()
	u, err := url.Parse(s)
	if err != nil {
		return event.NullValue()
	}
	result := map[string]event.Value{
		"scheme":   event.StringValue(u.Scheme),
		"host":     event.StringValue(u.Hostname()),
		"path":     event.StringValue(u.Path),
		"fragment": event.StringValue(u.Fragment),
	}
	// Port: int if numeric, else null
	if portStr := u.Port(); portStr != "" {
		if port, pErr := strconv.ParseInt(portStr, 10, 64); pErr == nil {
			result["port"] = event.IntValue(port)
		} else {
			result["port"] = event.NullValue()
		}
	} else {
		result["port"] = event.NullValue()
	}
	// Query: object of string key-value pairs
	queryObj := make(map[string]event.Value, len(u.Query()))
	for k, vals := range u.Query() {
		if len(vals) > 0 {
			queryObj[k] = event.StringValue(vals[0])
		}
	}
	result["query"] = event.ObjectValue(queryObj)
	return event.ObjectValue(result)
}

func execURLParam(rawURL, name event.Value) event.Value {
	if rawURL.IsNull() || name.IsNull() ||
		rawURL.Type() != event.FieldTypeString ||
		name.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	u, err := url.Parse(rawURL.AsString())
	if err != nil {
		return event.NullValue()
	}
	vals, ok := u.Query()[name.AsString()]
	if !ok || len(vals) == 0 {
		return event.NullValue()
	}

	return event.StringValue(vals[0])
}

func execURLStripQuery(rawURL, fragment event.Value) event.Value {
	if rawURL.IsNull() || fragment.IsNull() ||
		rawURL.Type() != event.FieldTypeString ||
		fragment.Type() != event.FieldTypeBool {
		return event.NullValue()
	}
	u, err := url.Parse(rawURL.AsString())
	if err != nil {
		return event.NullValue()
	}
	u.RawQuery = ""
	u.ForceQuery = false
	if fragment.AsBool() {
		u.Fragment = ""
		u.RawFragment = ""
	}

	return event.StringValue(u.String())
}

func execURLETLD1(rawURL event.Value) event.Value {
	host, ok := hostOrDomain(rawURL)
	if !ok {
		return event.NullValue()
	}
	registrable, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return event.NullValue()
	}
	return event.StringValue(registrable)
}

func execURLTLD(rawURL event.Value) event.Value {
	host, ok := hostOrDomain(rawURL)
	if !ok {
		return event.NullValue()
	}
	suffix, _ := publicsuffix.PublicSuffix(host)
	if suffix == "" || (suffix == host && !strings.Contains(host, ".")) {
		return event.NullValue()
	}
	return event.StringValue(suffix)
}

func hostOrDomain(v event.Value) (string, bool) {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return "", false
	}
	raw := strings.TrimSpace(v.AsString())
	if raw == "" {
		return "", false
	}
	if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
		return normalizeDomainHost(u.Hostname())
	}
	if strings.Contains(raw, "/") {
		return "", false
	}
	if host, _, err := net.SplitHostPort(raw); err == nil {
		raw = host
	}
	return normalizeDomainHost(raw)
}

func normalizeDomainHost(host string) (string, bool) {
	host = strings.ToLower(strings.TrimSuffix(strings.Trim(host, "[]"), "."))
	if host == "" || net.ParseIP(host) != nil {
		return "", false
	}
	return host, true
}

// execIPParse parses an IP address into an object.
func execIPParse(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s := v.AsString()
	ip := net.ParseIP(s)
	if ip == nil {
		return event.NullValue()
	}
	var version int64
	if ip.To4() != nil {
		version = 4
	} else {
		version = 6
	}
	isPrivate := isPrivateIP(ip)
	isLoopback := ip.IsLoopback()
	return event.ObjectValue(map[string]event.Value{
		"version":  event.IntValue(version),
		"private":  event.BoolValue(isPrivate),
		"loopback": event.BoolValue(isLoopback),
	})
}

func execIPToInt(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	ip := net.ParseIP(v.AsString()).To4()
	if ip == nil {
		return event.NullValue()
	}

	return event.IntValue(int64(binary.BigEndian.Uint32(ip)))
}

func execIPFromInt(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeInt {
		return event.NullValue()
	}
	const maxIPv4Int = 1<<32 - 1
	n := v.AsInt()
	if n < 0 || n > maxIPv4Int {
		return event.NullValue()
	}
	ip := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(ip, uint32(n))

	return event.StringValue(ip.String())
}

func execBucket(x, bounds event.Value) event.Value {
	if x.IsNull() || bounds.IsNull() || bounds.Type() != event.FieldTypeArray {
		return event.NullValue()
	}
	xNum, ok := ValueToFloat(x)
	if !ok {
		return event.NullValue()
	}
	var best event.Value
	var bestNum float64
	found := false
	for _, bound := range bounds.AsArray() {
		if bound.IsNull() {
			return event.NullValue()
		}
		boundNum, ok := ValueToFloat(bound)
		if !ok {
			return event.NullValue()
		}
		if boundNum <= xNum && (!found || boundNum > bestNum) {
			best = bound
			bestNum = boundNum
			found = true
		}
	}
	if !found {
		return event.NullValue()
	}

	return best
}

func execPathNormalize(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s := strings.ReplaceAll(v.AsString(), "\\", "/")
	cleaned := path.Clean(s)
	if cleaned == "." && s == "" {
		cleaned = ""
	}

	return event.StringValue(cleaned)
}

func execUserAgentParse(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	raw := strings.TrimSpace(v.AsString())
	if raw == "" {
		return event.NullValue()
	}
	name, version := parseUserAgentProduct(raw)

	return event.ObjectValue(map[string]event.Value{
		"name":    event.StringValue(name),
		"version": event.StringValue(version),
		"raw":     event.StringValue(raw),
	})
}

func parseUserAgentProduct(raw string) (string, string) {
	product := strings.Fields(raw)[0]
	name, version, ok := strings.Cut(product, "/")
	if !ok {
		return product, ""
	}

	return name, version
}

func execHumanizeBytes(v event.Value) event.Value {
	n, ok := ValueToFloat(v)
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
		return event.NullValue()
	}

	return event.StringValue(formatScaledNumber(n, 1024, []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}, " "))
}

func execHumanizeCount(v event.Value) event.Value {
	n, ok := ValueToFloat(v)
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
		return event.NullValue()
	}

	return event.StringValue(formatScaledNumber(n, 1000, []string{"", "K", "M", "B", "T", "Q"}, ""))
}

func execHumanizeDuration(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeDuration {
		return event.NullValue()
	}
	d := v.AsDuration()
	if d == 0 {
		return event.StringValue("0 seconds")
	}
	sign := ""
	if d < 0 {
		sign = "-"
		d = -d
	}
	type durationUnit struct {
		name string
		dur  time.Duration
	}
	units := []durationUnit{
		{name: "day", dur: 24 * time.Hour},
		{name: "hour", dur: time.Hour},
		{name: "minute", dur: time.Minute},
		{name: "second", dur: time.Second},
		{name: "millisecond", dur: time.Millisecond},
		{name: "microsecond", dur: time.Microsecond},
		{name: "nanosecond", dur: time.Nanosecond},
	}
	parts := make([]string, 0, 2)
	for _, unit := range units {
		if d < unit.dur {
			continue
		}
		n := d / unit.dur
		d -= n * unit.dur
		parts = append(parts, formatDurationPart(n, unit.name))
		if len(parts) == 2 {
			break
		}
	}

	return event.StringValue(sign + strings.Join(parts, ", "))
}

func execBar(x, lo, hi, width event.Value) event.Value {
	xNum, ok := ValueToFloat(x)
	if !ok || math.IsNaN(xNum) || math.IsInf(xNum, 0) {
		return event.NullValue()
	}
	loNum, ok := ValueToFloat(lo)
	if !ok || math.IsNaN(loNum) || math.IsInf(loNum, 0) {
		return event.NullValue()
	}
	hiNum, ok := ValueToFloat(hi)
	if !ok || math.IsNaN(hiNum) || math.IsInf(hiNum, 0) || hiNum <= loNum {
		return event.NullValue()
	}
	widthNum, ok := ValueToFloat(width)
	if !ok || math.IsNaN(widthNum) || math.IsInf(widthNum, 0) {
		return event.NullValue()
	}
	w := int(math.Round(widthNum))
	if w <= 0 || w > 1000 {
		return event.NullValue()
	}
	frac := (xNum - loNum) / (hiNum - loNum)
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	filled := int(math.Round(frac * float64(w)))

	return event.StringValue(strings.Repeat("#", filled) + strings.Repeat(".", w-filled))
}

func execIndexOf(container, needle event.Value) event.Value {
	if container.IsNull() || needle.IsNull() {
		return event.NullValue()
	}
	switch container.Type() {
	case event.FieldTypeString:
		if needle.Type() != event.FieldTypeString {
			return event.NullValue()
		}
		byteIdx := strings.Index(container.AsString(), needle.AsString())
		if byteIdx < 0 {
			return event.NullValue()
		}
		return event.IntValue(int64(utf8.RuneCountInString(container.AsString()[:byteIdx])))
	case event.FieldTypeArray:
		for i, elem := range container.AsArray() {
			if valuesEqual(elem, needle) {
				return event.IntValue(int64(i))
			}
		}
		return event.NullValue()
	default:
		return event.NullValue()
	}
}

func execSplitPart(s, sep, n event.Value) event.Value {
	if s.IsNull() || sep.IsNull() || n.IsNull() ||
		s.Type() != event.FieldTypeString ||
		sep.Type() != event.FieldTypeString ||
		n.Type() != event.FieldTypeInt {
		return event.NullValue()
	}
	idx := n.AsInt()
	if idx < 0 || sep.AsString() == "" {
		return event.NullValue()
	}
	parts := strings.Split(s.AsString(), sep.AsString())
	if idx >= int64(len(parts)) {
		return event.NullValue()
	}

	return event.StringValue(parts[idx])
}

func execBase64Decode(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	decoded, err := base64.StdEncoding.DecodeString(v.AsString())
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(decoded))
}

func execBase64Encode(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}

	return event.StringValue(base64.StdEncoding.EncodeToString([]byte(v.AsString())))
}

func execBase64URLDecode(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	decoded, err := base64.RawURLEncoding.DecodeString(v.AsString())
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(decoded))
}

func execBase64URLEncode(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}

	return event.StringValue(base64.RawURLEncoding.EncodeToString([]byte(v.AsString())))
}

func execHex(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}

	return event.StringValue(hex.EncodeToString([]byte(v.AsString())))
}

func execUnhex(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	decoded, err := hex.DecodeString(v.AsString())
	if err != nil {
		return event.NullValue()
	}

	return event.StringValue(string(decoded))
}

const maxStringFunctionOutputRunes = 1_000_000

func execRepeat(s, count event.Value) event.Value {
	if s.IsNull() || count.IsNull() ||
		s.Type() != event.FieldTypeString ||
		count.Type() != event.FieldTypeInt {
		return event.NullValue()
	}
	n := count.AsInt()
	if n < 0 {
		return event.NullValue()
	}
	text := s.AsString()
	if n == 0 || text == "" {
		return event.StringValue("")
	}
	runeLen := utf8.RuneCountInString(text)
	if runeLen > 0 && n > maxStringFunctionOutputRunes/int64(runeLen) {
		return event.NullValue()
	}

	return event.StringValue(strings.Repeat(text, int(n)))
}

func execPad(s, width, pad event.Value, left bool) event.Value {
	if s.IsNull() || width.IsNull() || pad.IsNull() ||
		s.Type() != event.FieldTypeString ||
		width.Type() != event.FieldTypeInt ||
		pad.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	target := width.AsInt()
	if target < 0 || target > maxStringFunctionOutputRunes {
		return event.NullValue()
	}
	text := s.AsString()
	textWidth := int64(utf8.RuneCountInString(text))
	if textWidth >= target {
		return event.StringValue(text)
	}
	padRunes := []rune(pad.AsString())
	if len(padRunes) == 0 {
		return event.NullValue()
	}
	fill := buildPadString(padRunes, int(target-textWidth))
	if left {
		return event.StringValue(fill + text)
	}

	return event.StringValue(text + fill)
}

func buildPadString(pad []rune, needed int) string {
	var b strings.Builder
	b.Grow(needed)
	written := 0
	for written < needed {
		for _, r := range pad {
			if written == needed {
				break
			}
			b.WriteRune(r)
			written++
		}
	}

	return b.String()
}

type translateReplacement struct {
	r    rune
	drop bool
}

func execTranslate(s, from, to event.Value) event.Value {
	if s.IsNull() || from.IsNull() || to.IsNull() ||
		s.Type() != event.FieldTypeString ||
		from.Type() != event.FieldTypeString ||
		to.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	fromRunes := []rune(from.AsString())
	toRunes := []rune(to.AsString())
	replacements := make(map[rune]translateReplacement, len(fromRunes))
	for i, r := range fromRunes {
		if _, exists := replacements[r]; exists {
			continue
		}
		if i >= len(toRunes) {
			replacements[r] = translateReplacement{drop: true}
			continue
		}
		replacements[r] = translateReplacement{r: toRunes[i]}
	}

	var b strings.Builder
	for _, r := range s.AsString() {
		replacement, ok := replacements[r]
		if !ok {
			b.WriteRune(r)
			continue
		}
		if !replacement.drop {
			b.WriteRune(replacement.r)
		}
	}

	return event.StringValue(b.String())
}

const maxFuzzyCells = 4_000_000

func execLevenshtein(args []event.Value) event.Value {
	if len(args) != 2 && len(args) != 3 {
		return event.NullValue()
	}
	if args[0].IsNull() || args[1].IsNull() ||
		args[0].Type() != event.FieldTypeString ||
		args[1].Type() != event.FieldTypeString {
		return event.NullValue()
	}
	damerau := false
	if len(args) == 3 {
		if args[2].IsNull() || args[2].Type() != event.FieldTypeBool {
			return event.NullValue()
		}
		damerau = args[2].AsBool()
	}

	a := []rune(args[0].AsString())
	b := []rune(args[1].AsString())
	if len(a) == 0 {
		return event.IntValue(int64(len(b)))
	}
	if len(b) == 0 {
		return event.IntValue(int64(len(a)))
	}
	if len(a)*len(b) > maxFuzzyCells {
		return event.NullValue()
	}

	return event.IntValue(int64(levenshteinDistance(a, b, damerau)))
}

func levenshteinDistance(a, b []rune, damerau bool) int {
	prevPrev := make([]int, len(b)+1)
	prev := make([]int, len(b)+1)
	cur := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			best := minEdit3(cur[j-1]+1, prev[j]+1, prev[j-1]+cost)
			if damerau && i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				if transposed := prevPrev[j-2] + 1; transposed < best {
					best = transposed
				}
			}
			cur[j] = best
		}
		prevPrev, prev, cur = prev, cur, prevPrev
	}
	return prev[len(b)]
}

func minEdit3(a, b, c int) int {
	if b < a {
		a = b
	}
	if c < a {
		a = c
	}
	return a
}

func execJaroWinkler(a, b event.Value) event.Value {
	if a.IsNull() || b.IsNull() ||
		a.Type() != event.FieldTypeString ||
		b.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	return event.FloatValue(jaroWinklerSimilarity([]rune(a.AsString()), []rune(b.AsString())))
}

func jaroWinklerSimilarity(a, b []rune) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1
	}
	if len(a) == 0 || len(b) == 0 {
		return 0
	}

	matchDistance := len(a)
	if len(b) > matchDistance {
		matchDistance = len(b)
	}
	matchDistance = matchDistance/2 - 1
	if matchDistance < 0 {
		matchDistance = 0
	}

	aMatches := make([]bool, len(a))
	bMatches := make([]bool, len(b))
	matches := 0
	for i := range a {
		start := i - matchDistance
		if start < 0 {
			start = 0
		}
		end := i + matchDistance + 1
		if end > len(b) {
			end = len(b)
		}
		for j := start; j < end; j++ {
			if bMatches[j] || a[i] != b[j] {
				continue
			}
			aMatches[i] = true
			bMatches[j] = true
			matches++
			break
		}
	}
	if matches == 0 {
		return 0
	}

	transpositions := 0
	k := 0
	for i := range a {
		if !aMatches[i] {
			continue
		}
		for !bMatches[k] {
			k++
		}
		if a[i] != b[k] {
			transpositions++
		}
		k++
	}

	m := float64(matches)
	jaro := (m/float64(len(a)) + m/float64(len(b)) + (m-float64(transpositions)/2)/m) / 3
	prefix := 0
	for prefix < 4 && prefix < len(a) && prefix < len(b) && a[prefix] == b[prefix] {
		prefix++
	}
	return jaro + float64(prefix)*0.1*(1-jaro)
}

func formatScaledNumber(n, base float64, units []string, sep string) string {
	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}
	unit := units[0]
	for i := 1; i < len(units) && n >= base; i++ {
		n /= base
		unit = units[i]
	}
	if unit == "" {
		return sign + formatScaledMagnitude(n)
	}

	return sign + formatScaledMagnitude(n) + sep + unit
}

func formatScaledMagnitude(n float64) string {
	rounded := math.Round(n*10) / 10
	if math.Abs(rounded-math.Round(rounded)) < 1e-9 {
		return strconv.FormatInt(int64(math.Round(rounded)), 10)
	}

	return strconv.FormatFloat(rounded, 'f', 1, 64)
}

func formatDurationPart(n time.Duration, unit string) string {
	if n == 1 {
		return "1 " + unit
	}

	return strconv.FormatInt(int64(n), 10) + " " + unit + "s"
}

// isPrivateIP checks if an IP address is in a private range (RFC 1918 / RFC 4193).
func isPrivateIP(ip net.IP) bool {
	privateRanges := []struct {
		network *net.IPNet
	}{
		{network: mustParseCIDR("10.0.0.0/8")},
		{network: mustParseCIDR("172.16.0.0/12")},
		{network: mustParseCIDR("192.168.0.0/16")},
		{network: mustParseCIDR("fc00::/7")},
	}
	for _, r := range privateRanges {
		if r.network.Contains(ip) {
			return true
		}
	}
	return false
}

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}

// execFromJSONNative parses a JSON string into native Values (recursive).
func execFromJSONNative(v event.Value) event.Value {
	if v.IsNull() {
		return event.NullValue()
	}
	if v.Type() != event.FieldTypeString {
		return event.NullValue()
	}
	s := v.AsString()
	return parseJSONToValue([]byte(s))
}

func execIsJSON(v event.Value) event.Value {
	if v.IsNull() || v.Type() != event.FieldTypeString {
		return event.BoolValue(false)
	}

	return event.BoolValue(json.Valid([]byte(v.AsString())))
}

// parseJSONToValue recursively converts JSON bytes to event.Value.
func parseJSONToValue(data []byte) event.Value {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return event.NullValue()
	}
	switch data[0] {
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(data, &obj); err != nil {
			return event.NullValue()
		}
		result := make(map[string]event.Value, len(obj))
		for k, raw := range obj {
			result[k] = parseJSONToValue(raw)
		}
		return event.ObjectValue(result)
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal(data, &arr); err != nil {
			return event.NullValue()
		}
		result := make([]event.Value, len(arr))
		for i, raw := range arr {
			result[i] = parseJSONToValue(raw)
		}
		return event.ArrayValue(result)
	case '"':
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return event.NullValue()
		}
		return event.StringValue(s)
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(data, &b); err != nil {
			return event.NullValue()
		}
		return event.BoolValue(b)
	case 'n':
		return event.NullValue()
	default:
		// Number
		s := string(data)
		// Try int first
		if !strings.Contains(s, ".") && !strings.Contains(s, "e") && !strings.Contains(s, "E") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return event.IntValue(i)
			}
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return event.FloatValue(f)
		}
		return event.NullValue()
	}
}

// Ensure unused imports are referenced.
var _ = binary.BigEndian
var _ = math.Pi
