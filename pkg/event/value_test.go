package event

import (
	"testing"
	"time"
)

func TestNullValue(t *testing.T) {
	v := NullValue()
	if v.Type() != FieldTypeNull {
		t.Fatalf("expected FieldTypeNull, got %s", v.Type())
	}
	if !v.IsNull() {
		t.Fatal("expected IsNull to be true")
	}
	if v.String() != "<null>" {
		t.Fatalf("expected <null>, got %s", v.String())
	}
}

func TestStringValue(t *testing.T) {
	v := StringValue("hello")
	if v.Type() != FieldTypeString {
		t.Fatalf("expected FieldTypeString, got %s", v.Type())
	}
	if v.IsNull() {
		t.Fatal("string should not be null")
	}
	if v.AsString() != "hello" {
		t.Fatalf("expected hello, got %s", v.AsString())
	}
	if v.String() != "hello" {
		t.Fatalf("expected hello, got %s", v.String())
	}
}

func TestStringValueEmpty(t *testing.T) {
	v := StringValue("")
	if v.AsString() != "" {
		t.Fatalf("expected empty string, got %q", v.AsString())
	}
}

func TestIntValue(t *testing.T) {
	tests := []struct {
		name string
		val  int64
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -100},
		{"max", 1<<63 - 1},
		{"min", -1 << 63},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := IntValue(tt.val)
			if v.Type() != FieldTypeInt {
				t.Fatalf("expected FieldTypeInt, got %s", v.Type())
			}
			if v.AsInt() != tt.val {
				t.Fatalf("expected %d, got %d", tt.val, v.AsInt())
			}
		})
	}
}

func TestFloatValue(t *testing.T) {
	tests := []struct {
		name string
		val  float64
	}{
		{"zero", 0.0},
		{"positive", 3.14},
		{"negative", -2.71},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := FloatValue(tt.val)
			if v.Type() != FieldTypeFloat {
				t.Fatalf("expected FieldTypeFloat, got %s", v.Type())
			}
			if v.AsFloat() != tt.val {
				t.Fatalf("expected %f, got %f", tt.val, v.AsFloat())
			}
		})
	}
}

func TestBoolValue(t *testing.T) {
	vTrue := BoolValue(true)
	if vTrue.Type() != FieldTypeBool {
		t.Fatalf("expected FieldTypeBool, got %s", vTrue.Type())
	}
	if !vTrue.AsBool() {
		t.Fatal("expected true")
	}
	if vTrue.String() != "true" {
		t.Fatalf("expected 'true', got %s", vTrue.String())
	}

	vFalse := BoolValue(false)
	if vFalse.AsBool() {
		t.Fatal("expected false")
	}
	if vFalse.String() != "false" {
		t.Fatalf("expected 'false', got %s", vFalse.String())
	}
}

func TestTimestampValue(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	v := TimestampValue(ts)
	if v.Type() != FieldTypeTimestamp {
		t.Fatalf("expected FieldTypeTimestamp, got %s", v.Type())
	}
	got := v.AsTimestamp()
	if !got.Equal(ts) {
		t.Fatalf("expected %v, got %v", ts, got)
	}
}

func TestTimestampValueNano(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 0, 0, 123456789, time.UTC)
	v := TimestampValue(ts)
	got := v.AsTimestamp()
	if !got.Equal(ts) {
		t.Fatalf("nano precision lost: expected %v, got %v", ts, got)
	}
}

func TestValuePanicsOnWrongType(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{"AsString on int", func() { IntValue(1).AsString() }},
		{"AsInt on string", func() { StringValue("x").AsInt() }},
		{"AsFloat on bool", func() { BoolValue(true).AsFloat() }},
		{"AsBool on null", func() { NullValue().AsBool() }},
		{"AsTimestamp on string", func() { StringValue("x").AsTimestamp() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			tt.fn()
		})
	}
}

func TestFieldTypeString(t *testing.T) {
	tests := []struct {
		ft   FieldType
		want string
	}{
		{FieldTypeNull, "null"},
		{FieldTypeString, "string"},
		{FieldTypeInt, "int"},
		{FieldTypeFloat, "float"},
		{FieldTypeBool, "bool"},
		{FieldTypeTimestamp, "timestamp"},
		{FieldType(99), "unknown(99)"},
	}
	for _, tt := range tests {
		if got := tt.ft.String(); got != tt.want {
			t.Errorf("FieldType(%d).String() = %s, want %s", tt.ft, got, tt.want)
		}
	}
}
