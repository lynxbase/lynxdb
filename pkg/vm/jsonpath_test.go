package vm

import (
	"testing"
)

func TestJsonExtractByPath_Simple(t *testing.T) {
	input := `{"level":"error","status":500}`

	v := jsonExtractByPath(input, "level")
	if v.String() != "error" {
		t.Errorf("level: got %q, want %q", v.String(), "error")
	}

	v = jsonExtractByPath(input, "status")
	got := v.AsInt()
	if got != 500 {
		t.Errorf("status: got %d, want 500", got)
	}
}

func TestJsonExtractByPath_Nested(t *testing.T) {
	input := `{"user":{"id":42,"name":"alice"}}`

	v := jsonExtractByPath(input, "user.id")
	if v.AsInt() != 42 {
		t.Errorf("user.id: got %d, want 42", v.AsInt())
	}

	v = jsonExtractByPath(input, "user.name")
	if v.String() != "alice" {
		t.Errorf("user.name: got %q, want %q", v.String(), "alice")
	}
}

func TestJsonExtractByPath_DeepNested(t *testing.T) {
	input := `{"a":{"b":{"c":{"d":"deep"}}}}`

	v := jsonExtractByPath(input, "a.b.c.d")
	if v.String() != "deep" {
		t.Errorf("a.b.c.d: got %q, want %q", v.String(), "deep")
	}
}

func TestJsonExtractByPath_Array(t *testing.T) {
	input := `{"items":[1,2,3]}`

	v := jsonExtractByPath(input, "items")
	if v.String() != "[1,2,3]" {
		t.Errorf("items: got %q, want %q", v.String(), "[1,2,3]")
	}
}

func TestJsonExtractByPath_Object(t *testing.T) {
	input := `{"data":{"key":"val"}}`

	v := jsonExtractByPath(input, "data")
	// Should return the JSON string representation of the object.
	if v.String() != `{"key":"val"}` {
		t.Errorf("data: got %q, want %q", v.String(), `{"key":"val"}`)
	}
}

func TestJsonExtractByPath_MissingKey(t *testing.T) {
	input := `{"a":1}`

	v := jsonExtractByPath(input, "b")
	if !v.IsNull() {
		t.Errorf("missing key: got %v, want null", v)
	}
}

func TestJsonExtractByPath_MissingNestedKey(t *testing.T) {
	input := `{"a":{"b":1}}`

	v := jsonExtractByPath(input, "a.c")
	if !v.IsNull() {
		t.Errorf("missing nested key: got %v, want null", v)
	}
}

func TestJsonExtractByPath_BoolAndNull(t *testing.T) {
	input := `{"active":true,"deleted":false,"ref":null}`

	v := jsonExtractByPath(input, "active")
	if v.AsBool() != true {
		t.Errorf("active: got %v, want true", v.AsBool())
	}

	v = jsonExtractByPath(input, "deleted")
	if v.AsBool() != false {
		t.Errorf("deleted: got %v, want false", v.AsBool())
	}

	v = jsonExtractByPath(input, "ref")
	if !v.IsNull() {
		t.Errorf("ref: got %v, want null", v)
	}
}

func TestJsonExtractByPath_Float(t *testing.T) {
	input := `{"ratio":3.14}`

	v := jsonExtractByPath(input, "ratio")
	f := v.AsFloat()
	if f < 3.13 || f > 3.15 {
		t.Errorf("ratio: got %f, want ~3.14", f)
	}
}

func TestJsonExtractByPath_EmptyInput(t *testing.T) {
	v := jsonExtractByPath("", "a")
	if !v.IsNull() {
		t.Errorf("empty input: got %v, want null", v)
	}
}

func TestJsonExtractByPath_NotJSON(t *testing.T) {
	v := jsonExtractByPath("not json", "a")
	if !v.IsNull() {
		t.Errorf("not json: got %v, want null", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_First(t *testing.T) {
	input := `{"items":[10,20,30]}`
	v := jsonExtractByPath(input, "items[0]")
	if v.AsInt() != 10 {
		t.Errorf("items[0]: got %v, want 10", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_Last(t *testing.T) {
	input := `{"items":[10,20,30]}`
	v := jsonExtractByPath(input, "items[-1]")
	if v.AsInt() != 30 {
		t.Errorf("items[-1]: got %v, want 30", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_NegativeTwo(t *testing.T) {
	input := `{"items":[10,20,30]}`
	v := jsonExtractByPath(input, "items[-2]")
	if v.AsInt() != 20 {
		t.Errorf("items[-2]: got %v, want 20", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_OutOfBounds(t *testing.T) {
	input := `{"items":[10,20,30]}`
	v := jsonExtractByPath(input, "items[5]")
	if !v.IsNull() {
		t.Errorf("items[5]: got %v, want null", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_NegativeOutOfBounds(t *testing.T) {
	input := `{"items":[10,20,30]}`
	v := jsonExtractByPath(input, "items[-10]")
	if !v.IsNull() {
		t.Errorf("items[-10]: got %v, want null", v)
	}
}

func TestJsonExtractByPath_ArrayIndex_NestedObject(t *testing.T) {
	input := `{"data":{"items":[{"id":"a1"},{"id":"b2"},{"id":"c3"}]}}`
	v := jsonExtractByPath(input, "data.items[0].id")
	if v.String() != "a1" {
		t.Errorf("data.items[0].id: got %q, want %q", v.String(), "a1")
	}
}

func TestJsonExtractByPath_ArrayWildcard(t *testing.T) {
	input := `{"users":[{"name":"alice"},{"name":"bob"}]}`
	v := jsonExtractByPath(input, "users[*].name")
	got := v.String()
	if got != `["alice","bob"]` {
		t.Errorf("users[*].name: got %q, want %q", got, `["alice","bob"]`)
	}
}

func TestJsonExtractByPath_ArrayIndex_NonArray(t *testing.T) {
	input := `{"name":"alice"}`
	v := jsonExtractByPath(input, "name[0]")
	if !v.IsNull() {
		t.Errorf("name[0] on non-array: got %v, want null", v)
	}
}

func TestJsonExtractByPath_ArrayWildcard_NoRemaining(t *testing.T) {
	input := `{"tags":["go","rust","zig"]}`
	v := jsonExtractByPath(input, "tags[*]")
	got := v.String()
	if got != `["go","rust","zig"]` {
		t.Errorf("tags[*]: got %q, want %q", got, `["go","rust","zig"]`)
	}
}

func BenchmarkJsonExtractByPath_DotOnly(b *testing.B) {
	input := `{"user":{"id":42,"name":"alice"}}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		jsonExtractByPath(input, "user.id")
	}
}

func BenchmarkJsonExtractByPath_ArrayIndex(b *testing.B) {
	input := `{"items":[{"id":"a1"},{"id":"b2"},{"id":"c3"}]}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		jsonExtractByPath(input, "items[0].id")
	}
}

func TestJsonKeys_Root(t *testing.T) {
	v := jsonKeys(`{"a":1,"b":2,"c":3}`, "")
	s := v.String()
	for _, key := range []string{`"a"`, `"b"`, `"c"`} {
		if !stringContainsHelper(s, key) {
			t.Errorf("jsonKeys root: expected %s in %q", key, s)
		}
	}
}

func TestJsonKeys_WithPath(t *testing.T) {
	v := jsonKeys(`{"nested":{"x":1,"y":2}}`, "nested")
	s := v.String()
	for _, key := range []string{`"x"`, `"y"`} {
		if !stringContainsHelper(s, key) {
			t.Errorf("jsonKeys with path: expected %s in %q", key, s)
		}
	}
}

func TestJsonKeys_NotObject(t *testing.T) {
	v := jsonKeys(`[1,2,3]`, "")
	if !v.IsNull() {
		t.Errorf("jsonKeys(array): got %v, want null", v)
	}
}

func TestJsonKeys_Null(t *testing.T) {
	v := jsonKeys("", "")
	if !v.IsNull() {
		t.Errorf("jsonKeys(empty): got %v, want null", v)
	}
}

func TestJsonArrayLength_Root(t *testing.T) {
	v := jsonArrayLength(`[1,2,3,4,5]`, "")
	if v.AsInt() != 5 {
		t.Errorf("jsonArrayLength root: got %d, want 5", v.AsInt())
	}
}

func TestJsonArrayLength_WithPath(t *testing.T) {
	v := jsonArrayLength(`{"items":[10,20,30]}`, "items")
	if v.AsInt() != 3 {
		t.Errorf("jsonArrayLength with path: got %d, want 3", v.AsInt())
	}
}

func TestJsonArrayLength_NotArray(t *testing.T) {
	v := jsonArrayLength(`{"a":1}`, "")
	if !v.IsNull() {
		t.Errorf("jsonArrayLength(object): got %v, want null", v)
	}
}

func TestJsonArrayLength_Empty(t *testing.T) {
	v := jsonArrayLength(`[]`, "")
	if v.AsInt() != 0 {
		t.Errorf("jsonArrayLength(empty): got %d, want 0", v.AsInt())
	}
}

// stringContainsHelper checks if s contains substr.
func stringContainsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestJsonIsValid(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{`{"a":1}`, true},
		{`[1,2,3]`, true},
		{`"hello"`, true},
		{`42`, true},
		{`true`, true},
		{`null`, true},
		{``, false},
		{`{invalid`, false},
		{`{"unclosed":`, false},
		{`not json at all`, false},
	}

	for _, tt := range tests {
		got := jsonIsValid(tt.input)
		if got != tt.want {
			t.Errorf("jsonIsValid(%q): got %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestJsonType_Object(t *testing.T) {
	v := jsonType(`{"a":1}`, "")
	if v.String() != "object" {
		t.Errorf("jsonType(object): got %q, want %q", v.String(), "object")
	}
}

func TestJsonType_Array(t *testing.T) {
	v := jsonType(`[1,2,3]`, "")
	if v.String() != "array" {
		t.Errorf("jsonType(array): got %q, want %q", v.String(), "array")
	}
}

func TestJsonType_String(t *testing.T) {
	v := jsonType(`{"name":"alice"}`, "name")
	if v.String() != "string" {
		t.Errorf("jsonType(string): got %q, want %q", v.String(), "string")
	}
}

func TestJsonType_Number(t *testing.T) {
	v := jsonType(`{"count":42}`, "count")
	if v.String() != "number" {
		t.Errorf("jsonType(number): got %q, want %q", v.String(), "number")
	}
}

func TestJsonType_Boolean(t *testing.T) {
	v := jsonType(`{"active":true}`, "active")
	if v.String() != "boolean" {
		t.Errorf("jsonType(boolean): got %q, want %q", v.String(), "boolean")
	}
}

func TestJsonType_Null(t *testing.T) {
	v := jsonType(`{"ref":null}`, "ref")
	if v.String() != "null" {
		t.Errorf("jsonType(null): got %q, want %q", v.String(), "null")
	}
}

func TestJsonType_NestedPath(t *testing.T) {
	v := jsonType(`{"user":{"tags":[1,2]}}`, "user.tags")
	if v.String() != "array" {
		t.Errorf("jsonType(nested): got %q, want %q", v.String(), "array")
	}
}

func TestJsonType_InvalidInput(t *testing.T) {
	v := jsonType("not json", "")
	if !v.IsNull() {
		t.Errorf("jsonType(invalid): got %v, want null", v)
	}
}

func TestJsonType_MissingPath(t *testing.T) {
	v := jsonType(`{"a":1}`, "missing")
	if !v.IsNull() {
		t.Errorf("jsonType(missing path): got %v, want null", v)
	}
}

func TestJsonType_RootNumber(t *testing.T) {
	v := jsonType(`42`, "")
	if v.String() != "number" {
		t.Errorf("jsonType(root number): got %q, want %q", v.String(), "number")
	}
}

func TestJsonType_NegativeNumber(t *testing.T) {
	v := jsonType(`{"val":-3.14}`, "val")
	if v.String() != "number" {
		t.Errorf("jsonType(negative number): got %q, want %q", v.String(), "number")
	}
}

func TestJsonSet_SimpleKey(t *testing.T) {
	v := jsonSet(`{"a":1}`, "b", "2")
	s := v.String()
	if !stringContainsHelper(s, `"a"`) || !stringContainsHelper(s, `"b"`) {
		t.Errorf("jsonSet simple: got %q", s)
	}
}

func TestJsonSet_NestedPath(t *testing.T) {
	v := jsonSet(`{"a":1}`, "b.c", `"deep"`)
	s := v.String()
	if !stringContainsHelper(s, `"b"`) || !stringContainsHelper(s, `"c"`) || !stringContainsHelper(s, `"deep"`) {
		t.Errorf("jsonSet nested: got %q", s)
	}
}

func TestJsonSet_OverwriteExisting(t *testing.T) {
	v := jsonSet(`{"a":1}`, "a", "2")
	s := v.String()
	if !stringContainsHelper(s, `"a":2`) {
		t.Errorf("jsonSet overwrite: got %q, expected a:2", s)
	}
}

func TestJsonSet_InvalidInput(t *testing.T) {
	v := jsonSet("not json", "a", "1")
	if !v.IsNull() {
		t.Errorf("jsonSet invalid: got %v, want null", v)
	}
}

func TestJsonSet_BoolValue(t *testing.T) {
	v := jsonSet(`{"a":1}`, "enabled", "true")
	s := v.String()
	if !stringContainsHelper(s, `"enabled":true`) {
		t.Errorf("jsonSet bool: got %q", s)
	}
}

func TestJsonSet_StringValue(t *testing.T) {
	v := jsonSet(`{"a":1}`, "name", `"alice"`)
	s := v.String()
	if !stringContainsHelper(s, `"name":"alice"`) {
		t.Errorf("jsonSet string: got %q", s)
	}
}

func TestJsonRemove_TopLevel(t *testing.T) {
	v := jsonRemove(`{"a":1,"b":2}`, "b")
	s := v.String()
	if stringContainsHelper(s, `"b"`) {
		t.Errorf("jsonRemove top: got %q, should not contain b", s)
	}
	if !stringContainsHelper(s, `"a"`) {
		t.Errorf("jsonRemove top: got %q, should contain a", s)
	}
}

func TestJsonRemove_Nested(t *testing.T) {
	v := jsonRemove(`{"user":{"name":"alice","email":"a@b.com"}}`, "user.email")
	s := v.String()
	if stringContainsHelper(s, `"email"`) {
		t.Errorf("jsonRemove nested: got %q, should not contain email", s)
	}
	if !stringContainsHelper(s, `"name"`) {
		t.Errorf("jsonRemove nested: got %q, should contain name", s)
	}
}

func TestJsonRemove_MissingKey(t *testing.T) {
	input := `{"a":1}`
	v := jsonRemove(input, "missing")
	if v.String() != input {
		t.Errorf("jsonRemove missing: got %q, want %q", v.String(), input)
	}
}

func TestJsonRemove_InvalidInput(t *testing.T) {
	v := jsonRemove("not json", "a")
	if !v.IsNull() {
		t.Errorf("jsonRemove invalid: got %v, want null", v)
	}
}

func TestJsonMerge_Simple(t *testing.T) {
	v := jsonMerge(`{"a":1}`, `{"b":2}`)
	s := v.String()
	if !stringContainsHelper(s, `"a"`) || !stringContainsHelper(s, `"b"`) {
		t.Errorf("jsonMerge simple: got %q", s)
	}
}

func TestJsonMerge_OverlappingKeys(t *testing.T) {
	v := jsonMerge(`{"a":1,"b":2}`, `{"b":3,"c":4}`)
	s := v.String()
	// b should be 3 (from second object).
	if !stringContainsHelper(s, `"b":3`) {
		t.Errorf("jsonMerge overlap: got %q, expected b:3", s)
	}
	if !stringContainsHelper(s, `"a"`) || !stringContainsHelper(s, `"c"`) {
		t.Errorf("jsonMerge overlap: got %q, missing a or c", s)
	}
}

func TestJsonMerge_EmptyObjects(t *testing.T) {
	v := jsonMerge(`{}`, `{}`)
	if v.String() != "{}" {
		t.Errorf("jsonMerge empty: got %q, want {}", v.String())
	}
}

func TestJsonMerge_NonObject(t *testing.T) {
	v := jsonMerge(`[1,2]`, `{"a":1}`)
	if !v.IsNull() {
		t.Errorf("jsonMerge non-object: got %v, want null", v)
	}

	v = jsonMerge(`{"a":1}`, `"not object"`)
	if !v.IsNull() {
		t.Errorf("jsonMerge non-object2: got %v, want null", v)
	}
}
