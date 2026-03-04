package unpack

import (
	"testing"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestJSONParser_FlatObject(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"level":"error","service":"api","duration_ms":1234}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "level", "error")
	assertStringValue(t, fields, "service", "api")
	assertIntValue(t, fields, "duration_ms", 1234)
}

func TestJSONParser_NestedObject(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"request":{"method":"GET","uri":"/api/v1"},"status":200}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "request.method", "GET")
	assertStringValue(t, fields, "request.uri", "/api/v1")
	assertIntValue(t, fields, "status", 200)
}

func TestJSONParser_ArrayAsString(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"tags":["web","prod"],"count":5}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// Arrays should be emitted as JSON string values.
	v, ok := fields["tags"]
	if !ok {
		t.Fatal("missing field: tags")
	}
	if v.Type() != event.FieldTypeString {
		t.Fatalf("tags type: got %s, want string", v.Type())
	}
	if v.String() != `["web","prod"]` {
		t.Errorf("tags: got %q, want %q", v.String(), `["web","prod"]`)
	}

	assertIntValue(t, fields, "count", 5)
}

func TestJSONParser_TypeInference(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"active":true,"score":3.14,"name":"test","count":42,"nothing":null}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	// bool
	if v, ok := fields["active"]; !ok || v.Type() != event.FieldTypeBool || !v.AsBool() {
		t.Errorf("active: got %v, want true", fields["active"])
	}

	// float
	if v, ok := fields["score"]; !ok || v.Type() != event.FieldTypeFloat {
		t.Errorf("score: got type %s, want float", v.Type())
	}

	// string
	assertStringValue(t, fields, "name", "test")

	// int
	assertIntValue(t, fields, "count", 42)

	// null
	if v, ok := fields["nothing"]; !ok || !v.IsNull() {
		t.Errorf("nothing: got %v, want null", v)
	}
}

func TestJSONParser_MalformedInput(t *testing.T) {
	p := &JSONParser{}
	// Should not return error for malformed JSON — schema-on-read
	err := p.Parse(`not json at all`, func(k string, v event.Value) bool {
		t.Errorf("should not emit for malformed input: key=%s", k)
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error for malformed input: %v", err)
	}
}

func TestJSONParser_EmptyInput(t *testing.T) {
	p := &JSONParser{}
	err := p.Parse(``, func(k string, v event.Value) bool {
		t.Errorf("should not emit for empty input: key=%s", k)
		return true
	})
	if err != nil {
		t.Fatalf("Parse returned error for empty input: %v", err)
	}
}

func TestJSONParser_ShortCircuit(t *testing.T) {
	p := &JSONParser{}
	count := 0
	err := p.Parse(`{"a":"1","b":"2","c":"3"}`, func(k string, v event.Value) bool {
		count++
		return false // stop after first field
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if count != 1 {
		t.Errorf("emit count: got %d, want 1 (short-circuit)", count)
	}
}

func TestJSONParser_DeepNesting(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"a":{"b":{"c":"deep"}}}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	assertStringValue(t, fields, "a.b.c", "deep")
}

func TestJSONParser_Float64Number(t *testing.T) {
	p := &JSONParser{}
	fields := make(map[string]event.Value)
	err := p.Parse(`{"lat":37.7749,"lng":-122.4194}`, func(k string, v event.Value) bool {
		fields[k] = v
		return true
	})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	v, ok := fields["lat"]
	if !ok || v.Type() != event.FieldTypeFloat {
		t.Errorf("lat: got type %s, want float", v.Type())
	}
}

func TestJSONParser_Name(t *testing.T) {
	p := &JSONParser{}
	if p.Name() != "json" {
		t.Errorf("Name: got %q, want %q", p.Name(), "json")
	}
}

func assertStringValue(t *testing.T, fields map[string]event.Value, key, want string) {
	t.Helper()
	v, ok := fields[key]
	if !ok {
		t.Errorf("missing field: %s", key)
		return
	}
	if v.Type() != event.FieldTypeString {
		t.Errorf("%s type: got %s, want string", key, v.Type())
		return
	}
	if v.String() != want {
		t.Errorf("%s: got %q, want %q", key, v.String(), want)
	}
}

func assertIntValue(t *testing.T, fields map[string]event.Value, key string, want int64) {
	t.Helper()
	v, ok := fields[key]
	if !ok {
		t.Errorf("missing field: %s", key)
		return
	}
	if v.Type() != event.FieldTypeInt {
		t.Errorf("%s type: got %s, want int", key, v.Type())
		return
	}
	if v.AsInt() != want {
		t.Errorf("%s: got %d, want %d", key, v.AsInt(), want)
	}
}
