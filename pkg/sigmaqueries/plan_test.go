package sigmaqueries

import (
	"context"
	"reflect"
	"testing"
	"unsafe"

	"github.com/lynxbase/lynxdb/pkg/engine/pipeline"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

var allowedPlanOperators = map[reflect.Type]bool{
	reflect.TypeOf((*pipeline.EvalIterator)(nil)):       true,
	reflect.TypeOf((*pipeline.FilterIterator)(nil)):     true,
	reflect.TypeOf((*pipeline.ProjectIterator)(nil)):    true,
	reflect.TypeOf((*pipeline.ScanIterator)(nil)):       true,
	reflect.TypeOf((*pipeline.SearchExprIterator)(nil)): true,
}

func TestPlanEveryGoldenLine(t *testing.T) {
	walkGoldenFixtures(t, func(t *testing.T, fixture, line string, lineNo int) {
		prog, err := spl2.ParseProgram(line)
		if err != nil {
			t.Fatalf("ParseProgram(%s:%d): %v\nSPL2: %s", fixture, lineNo, err, line)
		}

		iter, err := pipeline.BuildProgram(context.Background(), prog, &pipeline.ServerIndexStore{}, 0)
		if err != nil {
			t.Fatalf("BuildProgram(%s:%d): %v\nSPL2: %s", fixture, lineNo, err, line)
		}
		t.Cleanup(func() {
			if err := iter.Close(); err != nil {
				t.Fatalf("close plan for %s:%d: %v", fixture, lineNo, err)
			}
		})

		walkPlanOperators(t, iter, fixture, lineNo)
	})
}

func walkPlanOperators(t *testing.T, iter pipeline.Iterator, fixture string, lineNo int) {
	t.Helper()

	seen := map[uintptr]bool{}
	var walk func(pipeline.Iterator)
	walk = func(current pipeline.Iterator) {
		if current == nil {
			return
		}

		value := reflect.ValueOf(current)
		if value.Kind() == reflect.Pointer && !value.IsNil() {
			ptr := value.Pointer()
			if seen[ptr] {
				return
			}
			seen[ptr] = true
		}

		operatorType := reflect.TypeOf(current)
		if !allowedPlanOperators[operatorType] {
			t.Fatalf("plan introduced new operator %T for fixture %s/%d -- update allowlist or investigate regression", current, fixture, lineNo)
		}

		walkIteratorFields(value, walk)
	}

	walk(iter)
}

func walkIteratorFields(value reflect.Value, walk func(pipeline.Iterator)) {
	if value.Kind() == reflect.Interface {
		value = value.Elem()
	}
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return
	}
	value = value.Elem()
	if value.Kind() != reflect.Struct {
		return
	}

	iteratorType := reflect.TypeOf((*pipeline.Iterator)(nil)).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		if field.Type().Implements(iteratorType) {
			if child, ok := fieldInterface(field).(pipeline.Iterator); ok {
				walk(child)
			}
			continue
		}
		if field.Kind() == reflect.Slice && field.Type().Elem().Implements(iteratorType) {
			for j := 0; j < field.Len(); j++ {
				if child, ok := fieldInterface(field.Index(j)).(pipeline.Iterator); ok {
					walk(child)
				}
			}
		}
	}
}

func fieldInterface(field reflect.Value) interface{} {
	if field.CanInterface() {
		return field.Interface()
	}

	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
}
