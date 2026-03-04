package vm

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestVM_JsonExtract(t *testing.T) {
	// Compile: json_extract(payload, "user.id")
	prog := &Program{}
	fieldIdx := prog.AddFieldName("payload")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("user.id"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonExtract)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"payload": event.StringValue(`{"user":{"id":42,"name":"alice"}}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsInt() != 42 {
		t.Errorf("json_extract: got %v, want 42", result)
	}
}

func TestVM_JsonExtract_String(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("_raw")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("level"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonExtract)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"_raw": event.StringValue(`{"level":"error","status":500}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "error" {
		t.Errorf("json_extract: got %q, want %q", result.String(), "error")
	}
}

func TestVM_JsonExtract_NullField(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("missing")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("a"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonExtract)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_extract(null field): got %v, want null", result)
	}
}

func TestVM_JsonExtract_MissingPath(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("nonexistent"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonExtract)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_extract(missing path): got %v, want null", result)
	}
}

func TestVM_JsonValid_True(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("_raw")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpJsonValid)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"_raw": event.StringValue(`{"valid":"json"}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsBool() != true {
		t.Errorf("json_valid: got %v, want true", result)
	}
}

func TestVM_JsonValid_False(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("_raw")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpJsonValid)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"_raw": event.StringValue(`not json`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsBool() != false {
		t.Errorf("json_valid: got %v, want false", result)
	}
}

func TestVM_JsonValid_Null(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("missing")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpJsonValid)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsBool() != false {
		t.Errorf("json_valid(null): got %v, want false", result)
	}
}

func TestDotNotation_DirectField(t *testing.T) {
	// If field "a.b" exists directly in the fields map, use it (no JSON extraction).
	prog := &Program{}
	fieldIdx := prog.AddFieldName("a.b")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"a.b": event.StringValue("direct"),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "direct" {
		t.Errorf("dot-notation direct: got %q, want %q", result.String(), "direct")
	}
}

func TestDotNotation_JSONExtractFromRoot(t *testing.T) {
	// Field "a" has JSON, access "a.b.c" → extract "b.c" from field "a".
	prog := &Program{}
	fieldIdx := prog.AddFieldName("a.b.c")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"a": event.StringValue(`{"b":{"c":"nested"}}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "nested" {
		t.Errorf("dot-notation from root: got %q, want %q", result.String(), "nested")
	}
}

func TestDotNotation_JSONExtractFromRaw(t *testing.T) {
	// Neither root "request" nor "request.method" exist, fall back to _raw.
	prog := &Program{}
	fieldIdx := prog.AddFieldName("request.method")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"_raw": event.StringValue(`{"request":{"method":"POST","duration_ms":5012}}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "POST" {
		t.Errorf("dot-notation from _raw: got %q, want %q", result.String(), "POST")
	}
}

func TestDotNotation_MissingField(t *testing.T) {
	// Neither root nor _raw has the dotted field → null.
	prog := &Program{}
	fieldIdx := prog.AddFieldName("x.y.z")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"other": event.StringValue("irrelevant"),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("dot-notation missing: got %v, want null", result)
	}
}

func TestDotNotation_NonJSON(t *testing.T) {
	// Root field is a plain string (not JSON) → null.
	prog := &Program{}
	fieldIdx := prog.AddFieldName("msg.level")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"msg": event.StringValue("plain text, not json"),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("dot-notation non-JSON: got %v, want null", result)
	}
}

func TestDotNotation_NumericExtract(t *testing.T) {
	// Dot-notation should extract numeric values correctly.
	prog := &Program{}
	fieldIdx := prog.AddFieldName("response.status")
	prog.EmitOp(OpLoadField, fieldIdx)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"response": event.StringValue(`{"status":500,"latency_ms":3.14}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsInt() != 500 {
		t.Errorf("dot-notation numeric: got %v, want 500", result)
	}
}

func TestVM_JsonKeys(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonKeys)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1,"b":2,"c":3}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	// Keys should be a JSON array containing "a", "b", "c" (order may vary).
	for _, key := range []string{`"a"`, `"b"`, `"c"`} {
		if !contains(s, key) {
			t.Errorf("json_keys: expected %s in %q", key, s)
		}
	}
}

func TestVM_JsonKeys_WithPath(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("nested"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonKeys)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"nested":{"x":1,"y":2}}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	for _, key := range []string{`"x"`, `"y"`} {
		if !contains(s, key) {
			t.Errorf("json_keys with path: expected %s in %q", key, s)
		}
	}
}

func TestVM_JsonKeys_Null(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("missing")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonKeys)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_keys(null): got %v, want null", result)
	}
}

func TestVM_JsonArrayLength(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("items"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonArrayLen)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"items":[1,2,3,4,5]}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsInt() != 5 {
		t.Errorf("json_array_length: got %d, want 5", result.AsInt())
	}
}

func TestVM_JsonArrayLength_Root(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonArrayLen)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`[1,2,3]`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.AsInt() != 3 {
		t.Errorf("json_array_length root: got %d, want 3", result.AsInt())
	}
}

func TestVM_JsonArrayLength_NotArray(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonArrayLen)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"not":"array"}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_array_length(object): got %v, want null", result)
	}
}

func TestVM_JsonObject(t *testing.T) {
	prog := &Program{}
	k1 := prog.AddConstant(event.StringValue("name"))
	prog.EmitOp(OpConstStr, k1)
	v1 := prog.AddConstant(event.StringValue("alice"))
	prog.EmitOp(OpConstStr, v1)
	k2 := prog.AddConstant(event.StringValue("age"))
	prog.EmitOp(OpConstStr, k2)
	v2 := prog.AddConstant(event.IntValue(30))
	prog.EmitOp(OpConstInt, v2)
	prog.EmitOp(OpJsonObject, 4)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if !contains(s, `"name"`) || !contains(s, `"alice"`) || !contains(s, `"age"`) {
		t.Errorf("json_object: got %q", s)
	}
}

func TestVM_JsonObject_Empty(t *testing.T) {
	prog := &Program{}
	prog.EmitOp(OpJsonObject, 0)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "{}" {
		t.Errorf("json_object empty: got %q, want {}", result.String())
	}
}

func TestVM_JsonArray(t *testing.T) {
	prog := &Program{}
	v1 := prog.AddConstant(event.IntValue(1))
	prog.EmitOp(OpConstInt, v1)
	v2 := prog.AddConstant(event.StringValue("two"))
	prog.EmitOp(OpConstStr, v2)
	v3 := prog.AddConstant(event.IntValue(3))
	prog.EmitOp(OpConstInt, v3)
	prog.EmitOp(OpJsonArray, 3)
	prog.EmitOp(OpReturn)

	vm := &VM{}
	result, err := vm.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if s != `[1,"two",3]` {
		t.Errorf("json_array: got %q, want %q", s, `[1,"two",3]`)
	}
}

func TestVM_JsonArray_Empty(t *testing.T) {
	prog := &Program{}
	prog.EmitOp(OpJsonArray, 0)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "[]" {
		t.Errorf("json_array empty: got %q, want []", result.String())
	}
}

func TestVM_JsonType(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonType)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "object" {
		t.Errorf("json_type: got %q, want %q", result.String(), "object")
	}
}

func TestVM_JsonType_WithPath(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("items"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonType)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"items":[1,2,3]}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.String() != "array" {
		t.Errorf("json_type with path: got %q, want %q", result.String(), "array")
	}
}

func TestVM_JsonType_Null(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("missing")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue(""))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonType)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_type(null): got %v, want null", result)
	}
}

func TestVM_JsonSet(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("b"))
	prog.EmitOp(OpConstStr, pathIdx)
	valIdx := prog.AddConstant(event.IntValue(2))
	prog.EmitOp(OpConstInt, valIdx)
	prog.EmitOp(OpJsonSet)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if !contains(s, `"a"`) || !contains(s, `"b"`) {
		t.Errorf("json_set: got %q", s)
	}
}

func TestVM_JsonSet_Nested(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("x.y"))
	prog.EmitOp(OpConstStr, pathIdx)
	valIdx := prog.AddConstant(event.StringValue("deep"))
	prog.EmitOp(OpConstStr, valIdx)
	prog.EmitOp(OpJsonSet)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if !contains(s, `"x"`) || !contains(s, `"y"`) {
		t.Errorf("json_set nested: got %q", s)
	}
}

func TestVM_JsonSet_Null(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("missing")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("a"))
	prog.EmitOp(OpConstStr, pathIdx)
	valIdx := prog.AddConstant(event.IntValue(1))
	prog.EmitOp(OpConstInt, valIdx)
	prog.EmitOp(OpJsonSet)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_set(null): got %v, want null", result)
	}
}

func TestVM_JsonRemove(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("b"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonRemove)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1,"b":2}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if contains(s, `"b"`) {
		t.Errorf("json_remove: got %q, should not contain b", s)
	}
	if !contains(s, `"a"`) {
		t.Errorf("json_remove: got %q, should contain a", s)
	}
}

func TestVM_JsonRemove_MissingPath(t *testing.T) {
	prog := &Program{}
	fieldIdx := prog.AddFieldName("data")
	prog.EmitOp(OpLoadField, fieldIdx)
	pathIdx := prog.AddConstant(event.StringValue("nonexistent"))
	prog.EmitOp(OpConstStr, pathIdx)
	prog.EmitOp(OpJsonRemove)
	prog.EmitOp(OpReturn)

	fields := map[string]event.Value{
		"data": event.StringValue(`{"a":1}`),
	}

	v := &VM{}
	result, err := v.Execute(prog, fields)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// Should return original unchanged.
	if !contains(result.String(), `"a"`) {
		t.Errorf("json_remove(missing): got %q", result.String())
	}
}

func TestVM_JsonMerge(t *testing.T) {
	prog := &Program{}
	// Push json1.
	j1Idx := prog.AddConstant(event.StringValue(`{"a":1}`))
	prog.EmitOp(OpConstStr, j1Idx)
	// Push json2.
	j2Idx := prog.AddConstant(event.StringValue(`{"b":2}`))
	prog.EmitOp(OpConstStr, j2Idx)
	prog.EmitOp(OpJsonMerge)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	s := result.String()
	if !contains(s, `"a"`) || !contains(s, `"b"`) {
		t.Errorf("json_merge: got %q", s)
	}
}

func TestVM_JsonMerge_NonObject(t *testing.T) {
	prog := &Program{}
	j1Idx := prog.AddConstant(event.StringValue(`[1,2]`))
	prog.EmitOp(OpConstStr, j1Idx)
	j2Idx := prog.AddConstant(event.StringValue(`{"a":1}`))
	prog.EmitOp(OpConstStr, j2Idx)
	prog.EmitOp(OpJsonMerge)
	prog.EmitOp(OpReturn)

	v := &VM{}
	result, err := v.Execute(prog, map[string]event.Value{})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.IsNull() {
		t.Errorf("json_merge(non-object): got %v, want null", result)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && stringContains(s, substr))
}

func stringContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
