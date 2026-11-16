package variant

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
)

func TestRoundTripThroughJSON(t *testing.T) {
	var b []byte
	b, obj := BeginObject(b)
	b = AppendKey(b, []byte("n"))
	b = AppendNull(b)
	b = AppendKey(b, []byte("t"))
	b = AppendBool(b, true)
	b = AppendKey(b, []byte("f"))
	b = AppendBool(b, false)
	b = AppendKey(b, []byte("i"))
	b = AppendInt(b, math.MinInt64)
	b = AppendKey(b, []byte("x"))
	b = AppendFloat(b, -2.5e-9)
	b = AppendKey(b, []byte("s \"q\"\n"))
	b = AppendString(b, []byte("tab\there \x01 \xff ünï"))
	b = AppendKey(b, []byte("a"))
	b, arr := BeginArray(b)
	b = AppendInt(b, 7)
	b, inner := BeginArray(b)
	b = End(b, inner)
	b, emptyObj := BeginObject(b)
	b = End(b, emptyObj)
	b = AppendString(b, nil)
	b = End(b, arr)
	b = End(b, obj)

	text, err := AppendJSON(nil, b)
	if err != nil {
		t.Fatal(err)
	}
	var got any
	if err := json.Unmarshal(text, &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, text)
	}
	want := map[string]any{
		"n": nil, "t": true, "f": false,
		"i":         float64(math.MinInt64),
		"x":         -2.5e-9,
		"s \"q\"\n": "tab\there \x01 � ünï",
		"a":         []any{float64(7), []any{}, map[string]any{}, ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got  %#v\nwant %#v", got, want)
	}
}

func TestAppendJSONRejectsTruncatedInput(t *testing.T) {
	var b []byte
	b, arr := BeginArray(b)
	b = AppendString(b, []byte("hello"))
	b = AppendFloat(b, 1)
	b = End(b, arr)

	for n := 0; n < len(b); n++ {
		if _, err := AppendJSON(nil, b[:n]); err == nil {
			t.Fatalf("prefix of %d bytes decoded without error", n)
		}
	}
	if _, err := AppendJSON(nil, []byte{0x7F}); err == nil {
		t.Fatal("unknown tag decoded without error")
	}
}

func TestFloatFormatting(t *testing.T) {
	cases := map[float64]string{0: "0", 1: "1", -1.5: "-1.5", 1e21: "1e+21", 1e-7: "1e-07", math.NaN(): "null", math.Inf(1): "null"}
	for in, want := range cases {
		if got := string(AppendJSONFloat(nil, in)); got != want {
			t.Errorf("AppendJSONFloat(%v) = %s, want %s", in, got, want)
		}
	}
}

func TestForEachStringVisitsValuesOnly(t *testing.T) {
	var b []byte
	b, obj := BeginObject(b)
	b = AppendKey(b, []byte("key1"))
	b, arr := BeginArray(b)
	b = AppendString(b, []byte("one"))
	b = AppendInt(b, 5)
	b = AppendFloat(b, 1.5)
	b = AppendNull(b)
	b, inner := BeginObject(b)
	b = AppendKey(b, []byte("key2"))
	b = AppendString(b, []byte("two"))
	b = End(b, inner)
	b = End(b, arr)
	b = AppendKey(b, []byte("key3"))
	b = AppendString(b, []byte("three"))
	b = End(b, obj)

	var got []string
	ForEachString(b, func(s []byte) { got = append(got, string(s)) })
	if !reflect.DeepEqual(got, []string{"one", "two", "three"}) {
		t.Fatalf("got %v", got)
	}
	for n := 0; n < len(b); n++ {
		ForEachString(b[:n], func([]byte) {})
	}
}
