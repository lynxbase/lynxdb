package pipeline

import (
	"context"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// unpackInternerMaxSize is the maximum number of entries in the string interner.
// Fields with cardinality below this threshold (severity, user, database) are fully
// cached; high-cardinality fields (message, statement) overflow and are cloned normally.
const unpackInternerMaxSize = 4096

// UnpackIterator performs format-specific field extraction per batch.
// Uses a callback-based FormatParser to avoid intermediate map allocations.
type UnpackIterator struct {
	child        Iterator
	parser       unpack.FormatParser
	sourceField  string              // field to read from (default: "_raw")
	fields       map[string]struct{} // if non-nil, only extract these fields
	prefix       string              // prefix for output field names
	keepOriginal bool                // don't overwrite existing non-null fields
	prefixCache  map[string]string   // caches prefix+key → prefixed key to avoid repeated concatenation
	interner     map[string]string   // caches cloned strings for reuse within a batch

	// Pre-allocated per-call state used by emitField to avoid closure allocation.
	curBatch  *Batch // set at start of Next(), used by emitField
	curRowIdx int
}

// NewUnpackIterator creates a format-specific field extraction operator.
func NewUnpackIterator(
	child Iterator,
	parser unpack.FormatParser,
	sourceField string,
	fields []string,
	prefix string,
	keepOriginal bool,
) *UnpackIterator {
	var fieldSet map[string]struct{}
	if len(fields) > 0 {
		fieldSet = make(map[string]struct{}, len(fields))
		for _, f := range fields {
			fieldSet[f] = struct{}{}
		}
	}

	var prefixCache map[string]string
	if prefix != "" {
		prefixCache = make(map[string]string, 16) // typical format parsers produce ~8-16 fields
	}

	return &UnpackIterator{
		child:        child,
		parser:       parser,
		sourceField:  sourceField,
		fields:       fieldSet,
		prefix:       prefix,
		keepOriginal: keepOriginal,
		prefixCache:  prefixCache,
		interner:     make(map[string]string, 64),
	}
}

func (u *UnpackIterator) Init(ctx context.Context) error {
	return u.child.Init(ctx)
}

func (u *UnpackIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := u.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	srcCol := batch.Columns[u.sourceField]
	if srcCol == nil {
		return batch, nil
	}

	// Clear interner per-batch: low-cardinality fields (severity, user,
	// database) are re-cached within the batch; high-cardinality fields
	// (message, statement) use zero-copy subslices bounded by batch lifetime.
	for k := range u.interner {
		delete(u.interner, k)
	}

	u.curBatch = batch
	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			break
		}
		src := srcCol[i]
		if src.IsNull() {
			continue
		}

		u.curRowIdx = i
		// Method value — no closure allocation. emitField reads curBatch/curRowIdx.
		if err := u.parser.Parse(src.String(), u.emitField); err != nil {
			return batch, err
		}
	}

	return batch, nil
}

// emitField is the per-field callback for the format parser. It is a method
// (not a closure) to eliminate the per-row closure allocation that previously
// dominated the Unpack hot path. State is carried via curBatch and curRowIdx.
func (u *UnpackIterator) emitField(key string, val event.Value) bool {
	if u.fields != nil {
		if _, ok := u.fields[key]; !ok {
			return true // skip this field, continue parsing
		}
	}

	outKey := key
	if u.prefix != "" {
		if cached, ok := u.prefixCache[key]; ok {
			outKey = cached
		} else {
			outKey = u.prefix + key
			u.prefixCache[key] = outKey
		}
	}

	col, exists := u.curBatch.Columns[outKey]
	if !exists {
		col = make([]event.Value, u.curBatch.Len)
		u.curBatch.Columns[outKey] = col
	} else if len(col) < u.curBatch.Len {
		extended := make([]event.Value, u.curBatch.Len)
		copy(extended, col)
		col = extended
		u.curBatch.Columns[outKey] = col
	}

	// Don't overwrite existing non-null values in keep_original mode.
	if u.keepOriginal && !col[u.curRowIdx].IsNull() {
		return true
	}

	// String handling with two tiers:
	//   1. Interned (low-cardinality): cached across rows within this batch.
	//   2. Zero-copy (high-cardinality): subslice of source string, bounded
	//      by batch lifetime. No strings.Clone — avoids ~92MB of allocs for
	//      typical postgres log workloads where message is unique per row.
	if val.Type() == event.FieldTypeString {
		s := val.String()
		if interned, ok := u.interner[s]; ok {
			col[u.curRowIdx] = event.StringValue(interned)
		} else if len(u.interner) < unpackInternerMaxSize {
			cloned := strings.Clone(s)
			u.interner[s] = cloned
			col[u.curRowIdx] = event.StringValue(cloned)
		} else {
			col[u.curRowIdx] = val // zero-copy: batch lifetime bounds memory
		}
	} else {
		col[u.curRowIdx] = val
	}

	return true
}

func (u *UnpackIterator) Close() error { return u.child.Close() }

func (u *UnpackIterator) Schema() []FieldInfo { return u.child.Schema() }
