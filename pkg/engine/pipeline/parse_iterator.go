package pipeline

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// OnErrorMode controls how the ParseIterator handles per-row parse failures.
type OnErrorMode int

const (
	// OnErrorPropagate keeps the row, sets _error and _error_detail, and
	// populates whatever fields the parser managed to extract before failing.
	OnErrorPropagate OnErrorMode = iota

	// OnErrorNull keeps the row but does NOT populate extracted fields or
	// error columns on failure. The row passes through with its original
	// columns only.
	OnErrorNull

	// OnErrorDrop removes the row from the output batch entirely.
	OnErrorDrop

	// OnErrorStrict fails the entire query on the first parse failure,
	// including a truncated sample of the offending raw text.
	OnErrorStrict
)

// ParseOnErrorFromString converts a string (from the logical plan) to an
// OnErrorMode. The empty string maps to OnErrorPropagate (default per D1).
func ParseOnErrorFromString(s string) OnErrorMode {
	switch strings.ToLower(s) {
	case "null":
		return OnErrorNull
	case "drop":
		return OnErrorDrop
	case "strict":
		return OnErrorStrict
	default:
		return OnErrorPropagate
	}
}

// CaptureSpec describes a typed capture field for the parse stage.
// After extraction, the field is coerced to the given type. An empty Type
// means no coercion (value kept as-is from the parser).
type CaptureSpec struct {
	Name string
	Type string // "int", "float", "string", "bool", "timestamp", "duration", or ""
}

// ParseIterator extracts fields with format parsers, with per-row error
// handling, first_of chains, typed captures, and no-overwrite merge.
type ParseIterator struct {
	child Iterator

	// parsers is the ordered list of format parsers to try. When len > 1
	// this is a first_of chain; the first parser that succeeds wins.
	parsers []unpack.FormatParser

	// formatNames caches Name() calls on parsers, used in error messages.
	formatNames []string

	// firstOfLabel is the human-readable label for error messages when using
	// first_of. Empty when len(parsers) == 1.
	firstOfLabel string

	sourceField string              // field to parse from (default "_raw")
	captures    []CaptureSpec       // typed capture list; nil means all fields
	captureSet  map[string]struct{} // quick lookup for capture names (nil = accept all)
	prefix      string              // namespace prefix for extracted fields
	onError     OnErrorMode

	// prefixCache avoids repeated string concatenation for prefix + key.
	prefixCache map[string]string

	// interner caches low-cardinality string values within a batch.
	interner map[string]string

	// noOverwriteCount tracks how many field collisions were suppressed
	// (existing non-null value wins). Atomic because it may be read from
	// another goroutine via NoOverwriteCount() while the iterator runs.
	noOverwriteCount int64

	// Per-call state used by the emit callback to avoid closure allocation.
	curBatch    *Batch
	curRowIdx   int
	curEmitErr  error    // set if emit detects a short-circuit need
	curEmitted  []string // tracks which fields were written for this row (undo set)
	curProduced int      // fields the parser produced for this row, including no-overwrite skips
}

// NewParseIterator creates a parse operator.
//
// Parameters:
//   - child: input iterator
//   - parsers: one or more FormatParser instances to try in order (first_of chain)
//   - sourceField: field to parse from ("" defaults to "_raw")
//   - captures: typed capture list (nil = extract all fields from parser)
//   - prefix: namespace prefix for extracted field names ("" = no prefix)
//   - onError: how to handle per-row parse failures
func NewParseIterator(
	child Iterator,
	parsers []unpack.FormatParser,
	sourceField string,
	captures []CaptureSpec,
	prefix string,
	onError OnErrorMode,
) *ParseIterator {
	if sourceField == "" {
		sourceField = "_raw"
	}

	names := make([]string, len(parsers))
	for i, p := range parsers {
		names[i] = p.Name()
	}

	var firstOfLabel string
	if len(parsers) > 1 {
		firstOfLabel = "first_of(" + strings.Join(names, ", ") + ")"
	}

	var captureSet map[string]struct{}
	if len(captures) > 0 {
		captureSet = make(map[string]struct{}, len(captures))
		for _, c := range captures {
			captureSet[c.Name] = struct{}{}
		}
	}

	var prefixCache map[string]string
	if prefix != "" {
		prefixCache = make(map[string]string, 16)
	}

	return &ParseIterator{
		child:        child,
		parsers:      parsers,
		formatNames:  names,
		firstOfLabel: firstOfLabel,
		sourceField:  sourceField,
		captures:     captures,
		captureSet:   captureSet,
		prefix:       prefix,
		onError:      onError,
		prefixCache:  prefixCache,
		interner:     make(map[string]string, 64),
	}
}

func (p *ParseIterator) Init(ctx context.Context) error {
	return p.child.Init(ctx)
}

func (p *ParseIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		batch, err := p.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		result, err := p.processBatch(batch)
		if err != nil {
			return nil, err
		}
		if result != nil && result.Len > 0 {
			return result, nil
		}
		// If on_error=drop removed all rows, pull next batch.
	}
}

// processBatch processes a single batch, applying parse to each row.
func (p *ParseIterator) processBatch(batch *Batch) (*Batch, error) {
	srcCol := batch.Columns[p.sourceField]
	if srcCol == nil {
		return batch, nil
	}

	// Clear interner per batch.
	for k := range p.interner {
		delete(p.interner, k)
	}

	p.curBatch = batch

	if p.onError == OnErrorDrop {
		return p.processBatchWithDrop(batch, srcCol)
	}

	// Non-drop modes: process in place.
	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			break
		}
		src := srcCol[i]
		if src.IsNull() {
			continue
		}

		p.curRowIdx = i
		if err := p.parseRow(batch, i, src.String()); err != nil {
			return nil, err
		}
	}

	return batch, nil
}

// processBatchWithDrop handles on_error=drop by building a new batch with
// only the rows that parsed successfully.
func (p *ParseIterator) processBatchWithDrop(batch *Batch, srcCol []event.Value) (*Batch, error) {
	// Track which rows survive.
	survive := make([]bool, batch.Len)
	survivedCount := 0

	// We need a temporary batch to collect parse results before filtering.
	// Process in place first, then filter.
	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			survive[i] = true
			survivedCount++
			continue
		}
		src := srcCol[i]
		if src.IsNull() {
			survive[i] = true
			survivedCount++
			continue
		}

		p.curRowIdx = i
		p.curBatch = batch
		ok, err := p.tryParseRow(batch, i, src.String())
		if err != nil {
			return nil, err
		}
		survive[i] = ok
		if ok {
			survivedCount++
		}
	}

	if survivedCount == batch.Len {
		return batch, nil
	}

	out := &Batch{
		Columns: make(map[string][]event.Value, len(batch.Columns)),
		Len:     survivedCount,
	}
	for col, vals := range batch.Columns {
		filtered := make([]event.Value, 0, survivedCount)
		for i, v := range vals {
			if i < batch.Len && survive[i] {
				filtered = append(filtered, v)
			}
		}
		out.Columns[col] = filtered
	}

	return out, nil
}

// parseRow attempts to parse a single row. On failure, applies the on_error
// disposition. Returns an error only for on_error=strict.
func (p *ParseIterator) parseRow(batch *Batch, rowIdx int, input string) error {
	if len(p.parsers) == 1 {
		return p.parseSingleFormat(batch, rowIdx, input)
	}
	return p.parseFirstOf(batch, rowIdx, input)
}

// tryParseRow is like parseRow but returns (success, error) for drop mode.
func (p *ParseIterator) tryParseRow(batch *Batch, rowIdx int, input string) (bool, error) {
	if len(p.parsers) == 1 {
		return p.trySingleFormat(batch, rowIdx, input)
	}
	return p.tryFirstOf(batch, rowIdx, input)
}

// parseSingleFormat parses with a single format parser.
func (p *ParseIterator) parseSingleFormat(batch *Batch, rowIdx int, input string) error {
	p.curEmitted = p.curEmitted[:0]
	p.curProduced = 0
	p.curEmitErr = nil
	p.curRowIdx = rowIdx
	p.curBatch = batch

	parseErr := p.parsers[0].Parse(input, p.emitField)
	if p.curEmitErr != nil {
		return p.curEmitErr
	}

	// Check if parse "succeeded" — the parser returned nil error AND at least
	// one field was emitted. Many parsers (like JSON) return nil even on
	// malformed input (they just emit nothing).
	if parseErr == nil && p.curProduced > 0 {
		// Success: apply typed captures coercion.
		return p.applyCaptures(batch, rowIdx)
	}

	return p.handleParseFailure(batch, rowIdx, input, p.formatNames[0], parseErr)
}

// trySingleFormat is like parseSingleFormat but returns (success, error).
func (p *ParseIterator) trySingleFormat(batch *Batch, rowIdx int, input string) (bool, error) {
	p.curEmitted = p.curEmitted[:0]
	p.curProduced = 0
	p.curEmitErr = nil
	p.curRowIdx = rowIdx
	p.curBatch = batch

	parseErr := p.parsers[0].Parse(input, p.emitField)
	if p.curEmitErr != nil {
		return false, p.curEmitErr
	}

	if parseErr == nil && p.curProduced > 0 {
		if err := p.applyCaptures(batch, rowIdx); err != nil {
			return false, err
		}
		return true, nil
	}

	// Drop mode: parse failed → row dropped.
	return false, nil
}

// parseFirstOf tries parsers in order; first success wins.
func (p *ParseIterator) parseFirstOf(batch *Batch, rowIdx int, input string) error {
	var stageErrors []stageError

	for i, parser := range p.parsers {
		p.curEmitted = p.curEmitted[:0]
		p.curProduced = 0
		p.curEmitErr = nil
		p.curRowIdx = rowIdx
		p.curBatch = batch

		parseErr := parser.Parse(input, p.emitField)
		if p.curEmitErr != nil {
			return p.curEmitErr
		}

		if parseErr == nil && p.curProduced > 0 {
			// Success with this format.
			return p.applyCaptures(batch, rowIdx)
		}

		// Record this format's failure.
		msg := "no fields extracted"
		if parseErr != nil {
			msg = parseErr.Error()
		}
		stageErrors = append(stageErrors, stageError{
			format:  p.formatNames[i],
			code:    "parse_failed",
			message: msg,
		})

		// Undo any partial fields emitted by this failed parser.
		for _, key := range p.curEmitted {
			outKey := p.prefixedKey(key)
			if col, ok := batch.Columns[outKey]; ok && rowIdx < len(col) {
				col[rowIdx] = event.NullValue()
				batch.MarkAbsent(outKey, rowIdx)
			}
		}
	}

	// All formats failed.
	formatLabel := p.firstOfLabel
	firstMsg := "all formats failed"
	if len(stageErrors) > 0 {
		firstMsg = stageErrors[0].message
	}

	// Build _error_detail with stages array.
	return p.handleFirstOfFailure(batch, rowIdx, input, formatLabel, firstMsg, stageErrors)
}

// tryFirstOf is like parseFirstOf but returns (success, error) for drop mode.
func (p *ParseIterator) tryFirstOf(batch *Batch, rowIdx int, input string) (bool, error) {
	for _, parser := range p.parsers {
		p.curEmitted = p.curEmitted[:0]
		p.curProduced = 0
		p.curEmitErr = nil
		p.curRowIdx = rowIdx
		p.curBatch = batch

		parseErr := parser.Parse(input, p.emitField)
		if p.curEmitErr != nil {
			return false, p.curEmitErr
		}

		if parseErr == nil && p.curProduced > 0 {
			if err := p.applyCaptures(batch, rowIdx); err != nil {
				return false, err
			}
			return true, nil
		}

		// Undo partial fields.
		for _, key := range p.curEmitted {
			outKey := p.prefixedKey(key)
			if col, ok := batch.Columns[outKey]; ok && rowIdx < len(col) {
				col[rowIdx] = event.NullValue()
				batch.MarkAbsent(outKey, rowIdx)
			}
		}
	}

	return false, nil
}

// handleParseFailure applies the on_error disposition for a single-format failure.
func (p *ParseIterator) handleParseFailure(batch *Batch, rowIdx int, input, format string, parseErr error) error {
	switch p.onError {
	case OnErrorPropagate:
		msg := "no fields extracted"
		if parseErr != nil {
			msg = parseErr.Error()
		}
		errStr := fmt.Sprintf("parse:%s: %s", format, msg)
		p.setColumn(batch, rowIdx, "_error", event.StringValue(errStr))

		detail := map[string]event.Value{
			"stage":   event.IntValue(1),
			"format":  event.StringValue(format),
			"code":    event.StringValue("parse_failed"),
			"message": event.StringValue(msg),
		}
		p.setColumn(batch, rowIdx, "_error_detail", event.ObjectValue(detail))
		return nil

	case OnErrorNull:
		// Row survives, no fields, no error columns. Nothing to do.
		return nil

	case OnErrorStrict:
		msg := "no fields extracted"
		if parseErr != nil {
			msg = parseErr.Error()
		}
		truncated := truncateRaw(input, 200)
		return fmt.Errorf("parse:%s: %s (row %d, raw: %s)", format, msg, rowIdx, truncated)

	default:
		// OnErrorDrop handled by caller (tryParseRow path).
		return nil
	}
}

// stageError records a per-format failure within a first_of chain.
type stageError struct {
	format  string
	code    string
	message string
}

// handleFirstOfFailure applies the on_error disposition when all first_of formats fail.
func (p *ParseIterator) handleFirstOfFailure(batch *Batch, rowIdx int, input, formatLabel, firstMsg string, stageErrors []stageError) error {
	switch p.onError {
	case OnErrorPropagate:
		errStr := fmt.Sprintf("parse:%s: %s", formatLabel, firstMsg)
		p.setColumn(batch, rowIdx, "_error", event.StringValue(errStr))

		stages := make([]event.Value, len(stageErrors))
		for i, se := range stageErrors {
			stages[i] = event.ObjectValue(map[string]event.Value{
				"stage":   event.IntValue(int64(i + 1)),
				"format":  event.StringValue(se.format),
				"code":    event.StringValue(se.code),
				"message": event.StringValue(se.message),
			})
		}
		detail := map[string]event.Value{
			"stages": event.ArrayValue(stages),
		}
		p.setColumn(batch, rowIdx, "_error_detail", event.ObjectValue(detail))
		return nil

	case OnErrorNull:
		return nil

	case OnErrorStrict:
		truncated := truncateRaw(input, 200)
		return fmt.Errorf("parse:%s: %s (row %d, raw: %s)", formatLabel, firstMsg, rowIdx, truncated)

	default:
		return nil
	}
}

// emitField is the per-field callback for the format parser. It is a method
// (not a closure) to eliminate per-row closure allocation. State is carried
// via curBatch and curRowIdx.
func (p *ParseIterator) emitField(key string, val event.Value) bool {
	// Filter to captures if specified.
	if p.captureSet != nil {
		if _, ok := p.captureSet[key]; !ok {
			return true // skip, continue parsing
		}
	}

	// The parser produced a field the query cares about — count it before the
	// no-overwrite check. Success of a parse is judged on produced fields, not
	// written ones: re-parsing rows whose fields already exist must not be
	// reported as "no fields extracted".
	p.curProduced++

	outKey := p.prefixedKey(key)

	col, exists := p.curBatch.Columns[outKey]
	if !exists {
		col = make([]event.Value, p.curBatch.Len)
		p.curBatch.Columns[outKey] = col
		p.curBatch.MarkColumnAbsent(outKey)
	} else if len(col) < p.curBatch.Len {
		extended := make([]event.Value, p.curBatch.Len)
		copy(extended, col)
		col = extended
		p.curBatch.Columns[outKey] = col
	}

	// No-overwrite merge: existing non-null value wins.
	if !col[p.curRowIdx].IsNull() {
		atomic.AddInt64(&p.noOverwriteCount, 1)
		return true
	}

	// String interning (same as UnpackIterator).
	if val.Type() == event.FieldTypeString {
		s := val.String()
		if interned, ok := p.interner[s]; ok {
			col[p.curRowIdx] = event.StringValue(interned)
		} else if len(p.interner) < unpackInternerMaxSize {
			cloned := strings.Clone(s)
			p.interner[s] = cloned
			col[p.curRowIdx] = event.StringValue(cloned)
		} else {
			col[p.curRowIdx] = val
		}
	} else {
		col[p.curRowIdx] = val
	}
	p.curBatch.MarkPresent(outKey, p.curRowIdx)

	p.curEmitted = append(p.curEmitted, key)
	return true
}

// prefixedKey returns prefix+key, using the cache to avoid repeated concatenation.
func (p *ParseIterator) prefixedKey(key string) string {
	if p.prefix == "" {
		return key
	}
	if cached, ok := p.prefixCache[key]; ok {
		return cached
	}
	out := p.prefix + key
	p.prefixCache[key] = out
	return out
}

// setColumn sets a value in a batch column, creating the column if needed.
// Used for _error and _error_detail columns.
func (p *ParseIterator) setColumn(batch *Batch, rowIdx int, colName string, val event.Value) {
	// Error columns never get a prefix.
	col, exists := batch.Columns[colName]
	if !exists {
		col = make([]event.Value, batch.Len)
		batch.Columns[colName] = col
		batch.MarkColumnAbsent(colName)
	} else if len(col) < batch.Len {
		extended := make([]event.Value, batch.Len)
		copy(extended, col)
		col = extended
		batch.Columns[colName] = col
	}

	// No-overwrite applies to error columns too.
	if !col[rowIdx].IsNull() {
		atomic.AddInt64(&p.noOverwriteCount, 1)
		return
	}
	col[rowIdx] = val
	batch.MarkPresent(colName, rowIdx)
}

// applyCaptures coerces extracted fields to their declared types.
// On coercion failure, follows the on_error disposition.
func (p *ParseIterator) applyCaptures(batch *Batch, rowIdx int) error {
	if len(p.captures) == 0 {
		return nil
	}

	for _, c := range p.captures {
		if c.Type == "" {
			continue // no coercion needed
		}

		outKey := p.prefixedKey(c.Name)
		col, ok := batch.Columns[outKey]
		if !ok || rowIdx >= len(col) {
			continue // field not extracted
		}

		val := col[rowIdx]
		if val.IsNull() {
			continue // nothing to coerce
		}

		coerced, ok := coerceValue(val, c.Type)
		if ok {
			col[rowIdx] = coerced
			continue
		}

		// Coercion failed.
		switch p.onError {
		case OnErrorPropagate:
			col[rowIdx] = event.NullValue()
			errStr := fmt.Sprintf("parse:capture: cannot coerce %q to %s", val.String(), c.Type)
			p.setColumn(batch, rowIdx, "_error", event.StringValue(errStr))
			detail := map[string]event.Value{
				"stage":   event.IntValue(1),
				"format":  event.StringValue("capture"),
				"code":    event.StringValue("coerce_failed"),
				"message": event.StringValue(fmt.Sprintf("field %q: cannot coerce %q to %s", c.Name, val.String(), c.Type)),
			}
			p.setColumn(batch, rowIdx, "_error_detail", event.ObjectValue(detail))

		case OnErrorNull:
			col[rowIdx] = event.NullValue()

		case OnErrorStrict:
			return fmt.Errorf("parse:capture: cannot coerce field %q value %q to %s (row %d)",
				c.Name, val.String(), c.Type, rowIdx)

		case OnErrorDrop:
			// Should not reach here in drop mode (handled by tryParseRow).
			col[rowIdx] = event.NullValue()
		}
	}

	return nil
}

// coerceValue converts a Value to the target type.
// Returns (coerced, true) on success, (zero, false) on failure.
func coerceValue(val event.Value, targetType string) (event.Value, bool) {
	switch strings.ToLower(targetType) {
	case "int":
		return coerceToInt(val)
	case "float":
		return coerceToFloat(val)
	case "string":
		return event.StringValue(val.String()), true
	case "bool":
		return coerceToBool(val)
	case "timestamp":
		return coerceToTimestamp(val)
	case "duration":
		return coerceToDuration(val)
	default:
		// Unknown type: no coercion.
		return val, true
	}
}

func coerceToInt(val event.Value) (event.Value, bool) {
	switch val.Type() {
	case event.FieldTypeInt:
		return val, true
	case event.FieldTypeFloat:
		f, _ := val.TryAsFloat()
		return event.IntValue(int64(f)), true
	case event.FieldTypeString:
		s := val.String()
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return event.IntValue(n), true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return event.IntValue(int64(f)), true
		}
		return event.NullValue(), false
	case event.FieldTypeBool:
		b, _ := val.TryAsBool()
		if b {
			return event.IntValue(1), true
		}
		return event.IntValue(0), true
	default:
		return event.NullValue(), false
	}
}

func coerceToFloat(val event.Value) (event.Value, bool) {
	switch val.Type() {
	case event.FieldTypeFloat:
		return val, true
	case event.FieldTypeInt:
		n, _ := val.TryAsInt()
		return event.FloatValue(float64(n)), true
	case event.FieldTypeString:
		s := val.String()
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return event.FloatValue(f), true
		}
		return event.NullValue(), false
	default:
		return event.NullValue(), false
	}
}

func coerceToBool(val event.Value) (event.Value, bool) {
	switch val.Type() {
	case event.FieldTypeBool:
		return val, true
	case event.FieldTypeString:
		s := strings.ToLower(val.String())
		switch s {
		case "true", "1", "yes":
			return event.BoolValue(true), true
		case "false", "0", "no":
			return event.BoolValue(false), true
		}
		return event.NullValue(), false
	case event.FieldTypeInt:
		n, _ := val.TryAsInt()
		return event.BoolValue(n != 0), true
	default:
		return event.NullValue(), false
	}
}

func coerceToTimestamp(val event.Value) (event.Value, bool) {
	switch val.Type() {
	case event.FieldTypeTimestamp:
		return val, true
	case event.FieldTypeString:
		s := val.String()
		// Try common formats.
		for _, layout := range []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			if t, err := time.Parse(layout, s); err == nil {
				return event.TimestampValue(t), true
			}
		}
		// Try Unix timestamp as number string.
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			sec := int64(f)
			nsec := int64((f - float64(sec)) * 1e9)
			return event.TimestampValue(time.Unix(sec, nsec).UTC()), true
		}
		return event.NullValue(), false
	case event.FieldTypeInt:
		n, _ := val.TryAsInt()
		return event.TimestampValue(time.Unix(n, 0).UTC()), true
	case event.FieldTypeFloat:
		f, _ := val.TryAsFloat()
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return event.TimestampValue(time.Unix(sec, nsec).UTC()), true
	default:
		return event.NullValue(), false
	}
}

func coerceToDuration(val event.Value) (event.Value, bool) {
	switch val.Type() {
	case event.FieldTypeDuration:
		return val, true
	case event.FieldTypeString:
		s := val.String()
		if d, err := time.ParseDuration(s); err == nil {
			return event.DurationValue(d), true
		}
		// Try suffixed forms: "100ms", "5s", "1h", "7d".
		if len(s) > 1 && s[len(s)-1] == 'd' {
			if f, err := strconv.ParseFloat(s[:len(s)-1], 64); err == nil {
				return event.DurationValue(time.Duration(f * float64(24*time.Hour))), true
			}
		}
		return event.NullValue(), false
	case event.FieldTypeInt:
		// Interpret as nanoseconds.
		n, _ := val.TryAsInt()
		return event.DurationValue(time.Duration(n)), true
	case event.FieldTypeFloat:
		// Interpret as seconds.
		f, _ := val.TryAsFloat()
		return event.DurationValue(time.Duration(f * float64(time.Second))), true
	default:
		return event.NullValue(), false
	}
}

// NoOverwriteCount returns the cumulative count of field collisions where
// the existing non-null value was preserved. Thread-safe.
func (p *ParseIterator) NoOverwriteCount() int64 {
	return atomic.LoadInt64(&p.noOverwriteCount)
}

func (p *ParseIterator) Close() error { return p.child.Close() }

func (p *ParseIterator) Schema() []FieldInfo { return p.child.Schema() }

// truncateRaw truncates a raw string to maxLen characters for error messages.
func truncateRaw(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// Verify interface compliance.
var _ Iterator = (*ParseIterator)(nil)

// Ensure math is used (for float bits in coercion).
var _ = math.Float64frombits
