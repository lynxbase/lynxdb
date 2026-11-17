package event

import (
	"testing"
	"time"
)

func TestDurationValue(t *testing.T) {
	v := DurationValue(90 * time.Second)
	if v.Type() != FieldTypeDuration {
		t.Fatalf("type = %s, want duration", v.Type())
	}
	if d := v.AsDuration(); d != 90*time.Second {
		t.Errorf("AsDuration = %v", d)
	}
	if d, err := v.AsDurationE(); err != nil || d != 90*time.Second {
		t.Errorf("AsDurationE = %v, %v", d, err)
	}
	if d, ok := v.TryAsDuration(); !ok || d != 90*time.Second {
		t.Errorf("TryAsDuration = %v, %v", d, ok)
	}
	if s := v.String(); s != "1m30s" {
		t.Errorf("String = %q, want 1m30s", s)
	}
	if _, err := v.AsIntE(); err == nil {
		t.Error("AsIntE on duration: want type mismatch error")
	}
	if _, err := IntValue(1).AsDurationE(); err == nil {
		t.Error("AsDurationE on int: want type mismatch error")
	}
	if got := v.Interface(); got != 90*time.Second {
		t.Errorf("Interface = %#v", got)
	}
	if rt := ValueFromInterface(90 * time.Second); rt.Type() != FieldTypeDuration || rt.AsDuration() != 90*time.Second {
		t.Errorf("ValueFromInterface round-trip = %#v", rt)
	}
}

func TestArrayValue(t *testing.T) {
	v := ArrayValue([]Value{IntValue(1), StringValue("x"), BoolValue(true)})
	if v.Type() != FieldTypeArray {
		t.Fatalf("type = %s, want array", v.Type())
	}
	arr := v.AsArray()
	if len(arr) != 3 || arr[0].AsInt() != 1 || arr[1].AsString() != "x" || !arr[2].AsBool() {
		t.Errorf("AsArray = %v", arr)
	}
	if _, err := v.AsArrayE(); err != nil {
		t.Errorf("AsArrayE: %v", err)
	}
	if _, ok := v.TryAsArray(); !ok {
		t.Error("TryAsArray: want ok")
	}
	if _, err := StringValue("x").AsArrayE(); err == nil {
		t.Error("AsArrayE on string: want type mismatch error")
	}
	if s := v.String(); s != `[1,"x",true]` {
		t.Errorf("String = %q", s)
	}

	iface := v.Interface()
	slice, ok := iface.([]interface{})
	if !ok || len(slice) != 3 || slice[0] != int64(1) || slice[1] != "x" || slice[2] != true {
		t.Errorf("Interface = %#v", iface)
	}
	rt := ValueFromInterface(slice)
	if rt.Type() != FieldTypeArray || rt.String() != v.String() {
		t.Errorf("round-trip = %s", rt.String())
	}
}

func TestObjectValue(t *testing.T) {
	v := ObjectValue(map[string]Value{
		"service": StringValue("api"),
		"retry":   BoolValue(true),
		"n":       IntValue(2),
	})
	if v.Type() != FieldTypeObject {
		t.Fatalf("type = %s, want object", v.Type())
	}
	obj := v.AsObject()
	if len(obj) != 3 || obj["service"].AsString() != "api" {
		t.Errorf("AsObject = %v", obj)
	}
	if _, err := IntValue(1).AsObjectE(); err == nil {
		t.Error("AsObjectE on int: want type mismatch error")
	}
	// Keys must render sorted: object Strings are used as group keys.
	if s := v.String(); s != `{"n":2,"retry":true,"service":"api"}` {
		t.Errorf("String = %q", s)
	}

	iface := v.Interface()
	m, ok := iface.(map[string]interface{})
	if !ok || m["service"] != "api" || m["retry"] != true || m["n"] != int64(2) {
		t.Errorf("Interface = %#v", iface)
	}
	rt := ValueFromInterface(m)
	if rt.Type() != FieldTypeObject || rt.String() != v.String() {
		t.Errorf("round-trip = %s", rt.String())
	}
}

func TestNestedRendering(t *testing.T) {
	v := ObjectValue(map[string]Value{
		"tags": ArrayValue([]Value{
			ObjectValue(map[string]Value{"name": StringValue("vip"), "tier": IntValue(1)}),
		}),
		"ok":  NullValue(),
		"dur": DurationValue(5 * time.Minute),
	})
	want := `{"dur":"5m0s","ok":null,"tags":[{"name":"vip","tier":1}]}`
	if s := v.String(); s != want {
		t.Errorf("String = %q, want %q", s, want)
	}
}

func TestFieldTypeStringsAndStability(t *testing.T) {
	// The tag values are persisted in spill/segment encodings; they must
	// never be renumbered.
	stable := map[FieldType]uint8{
		FieldTypeNull: 0, FieldTypeString: 1, FieldTypeInt: 2, FieldTypeFloat: 3,
		FieldTypeBool: 4, FieldTypeTimestamp: 5, FieldTypeDuration: 6,
		FieldTypeArray: 7, FieldTypeObject: 8,
	}
	for ft, want := range stable {
		if uint8(ft) != want {
			t.Errorf("FieldType %s = %d, want %d", ft, uint8(ft), want)
		}
	}
	names := map[FieldType]string{
		FieldTypeDuration: "duration", FieldTypeArray: "array", FieldTypeObject: "object",
	}
	for ft, want := range names {
		if ft.String() != want {
			t.Errorf("FieldType(%d).String() = %q, want %q", uint8(ft), ft.String(), want)
		}
	}
}
