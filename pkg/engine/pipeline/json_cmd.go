package pipeline

import (
	"context"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/engine/unpack"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

// JsonCmdIterator implements the `| json` pipe command — an ergonomic shorthand
// for JSON field extraction. When paths are specified, only those dot-paths are
// extracted. When no paths are specified, all top-level and nested keys are
// extracted with dot-notation flattening (same as unpack_json).
//
// Paths may use bracket notation for array indexing: "items[0].name",
// "items[-1]", "items[*].id". Bracket paths are resolved using jsonpath
// extraction directly on the JSON source string (not via the flattened
// parser output), while simple dot-only paths use the fast flattening path.
type JsonCmdIterator struct {
	child        Iterator
	parser       *unpack.JSONParser
	sourceField  string            // field to read from (default: "_raw")
	paths        map[string]string // path → output name (alias or path itself); nil = all
	bracketPaths map[string]string // bracket-containing paths → output name; nil if none
}

// NewJsonCmdIterator creates a JSON extraction operator.
// paths is a list of JsonPath entries; each may have an optional alias.
func NewJsonCmdIterator(child Iterator, sourceField string, paths []spl2.JsonPath) *JsonCmdIterator {
	var pathMap map[string]string
	var bracketMap map[string]string

	if len(paths) > 0 {
		pathMap = make(map[string]string, len(paths))
		for _, jp := range paths {
			if strings.ContainsRune(jp.Path, '[') {
				// Bracket path — needs jsonpath extraction.
				if bracketMap == nil {
					bracketMap = make(map[string]string, len(paths))
				}
				bracketMap[jp.Path] = jp.OutputName()
			} else {
				pathMap[jp.Path] = jp.OutputName()
			}
		}
	}

	return &JsonCmdIterator{
		child:        child,
		parser:       &unpack.JSONParser{},
		sourceField:  sourceField,
		paths:        pathMap,
		bracketPaths: bracketMap,
	}
}

func (j *JsonCmdIterator) Init(ctx context.Context) error {
	return j.child.Init(ctx)
}

func (j *JsonCmdIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := j.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	srcCol := batch.Columns[j.sourceField]
	if srcCol == nil {
		return batch, nil
	}

	for i := 0; i < batch.Len; i++ {
		if i >= len(srcCol) {
			break
		}
		src := srcCol[i]
		if src.IsNull() {
			continue
		}

		rowIdx := i // capture for closure
		srcStr := src.String()

		// Fast path: use the flattening parser for dot-only paths (or all paths
		// when no bracket paths are present and no specific paths are requested).
		if j.paths != nil || j.bracketPaths == nil {
			j.parser.Parse(srcStr, func(key string, val event.Value) bool {
				outName := key
				if j.paths != nil {
					alias, ok := j.paths[key]
					if !ok {
						return true // skip, continue parsing
					}
					outName = alias
				}

				setBatchValue(batch, outName, rowIdx, val)
				return true
			})
		}

		// Bracket path extraction: use vm.JsonExtractByPath for each bracket
		// path directly on the source JSON string. This handles array indexing
		// like "items[0].name", "items[-1]", "items[*].id".
		for path, outName := range j.bracketPaths {
			val := vm.JsonExtractByPath(srcStr, path)
			if !val.IsNull() {
				setBatchValue(batch, outName, rowIdx, val)
			}
		}
	}

	return batch, nil
}

// setBatchValue sets a value in a batch column, creating or extending the
// column as needed. String values are cloned to prevent memory retention.
func setBatchValue(batch *Batch, colName string, rowIdx int, val event.Value) {
	col, exists := batch.Columns[colName]
	if !exists {
		col = make([]event.Value, batch.Len)
		batch.Columns[colName] = col
	} else if len(col) < batch.Len {
		extended := make([]event.Value, batch.Len)
		copy(extended, col)
		col = extended
		batch.Columns[colName] = col
	}

	// strings.Clone: prevent memory retention of the source string.
	if val.Type() == event.FieldTypeString {
		col[rowIdx] = event.StringValue(strings.Clone(val.String()))
	} else {
		col[rowIdx] = val
	}
}

func (j *JsonCmdIterator) Close() error { return j.child.Close() }

func (j *JsonCmdIterator) Schema() []FieldInfo { return j.child.Schema() }
