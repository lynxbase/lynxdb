package ndjson

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTextRead(t *testing.T) {
	conv, err := NewText("")
	if err != nil {
		t.Fatal(err)
	}
	conv.Source = "app.log"
	in := "2025-01-15T08:00:00.105Z host=web-03 status=502 user=grace\n\n{\"a\":1}\nplain words\n"
	var got []string
	if err := conv.Read(strings.NewReader(in), func(b []byte) error {
		got = append(got, strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("lines: %q", got)
	}
	if got[0] != `{"a":1}` {
		t.Fatalf("json line changed: %s", got[0])
	}
	var kv map[string]any
	if err := json.Unmarshal([]byte(got[1]), &kv); err != nil {
		t.Fatal(err)
	}
	if kv["_time"] != "2025-01-15T08:00:00.105Z" || kv["host"] != "web-03" || kv["source"] != "app.log" || kv["user"] != "grace" {
		t.Fatalf("text line: %s", got[1])
	}
	if !strings.Contains(got[2], `"_raw":"plain words"`) {
		t.Fatalf("plain line: %s", got[2])
	}
}

func TestTextFormat(t *testing.T) {
	conv, err := NewText("combined")
	if err != nil {
		t.Fatal(err)
	}
	line := `192.168.1.42 - john [10/Feb/2026:08:12:03 +0000] "GET /x HTTP/1.1" 200 1243 "-" "curl/8"` + "\n"
	var out string
	if err := conv.Read(strings.NewReader(line), func(b []byte) error { out += string(b); return nil }); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"status":200`) {
		t.Fatalf("combined fields: %s", out)
	}
}
