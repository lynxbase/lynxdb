package ndjson

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
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

func TestTextSyslog(t *testing.T) {
	conv, err := NewText("syslog")
	if err != nil {
		t.Fatal(err)
	}
	year := time.Now().UTC().Year()
	in := `<34>1 2025-01-15T08:00:00.003Z web-01 sshd 4242 ID47 [origin ip="10.0.0.7"] Accepted key user=alice` + "\n" +
		`<13>Feb  5 17:32:18 db-02 cron[42]: job done status=0` + "\n" +
		"<13>not a syslog message level=warn\n"
	var out string
	if err := conv.Read(strings.NewReader(in), func(b []byte) error { out += string(b); return nil }); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines: %q", lines)
	}
	for i, want := range []map[string]any{
		{"_time": "2025-01-15T08:00:00.003Z", "host": "web-01", "app_name": "sshd", "procid": "4242", "msgid": "ID47",
			"sd_origin_ip": "10.0.0.7", "facility": 4.0, "severity": 2.0, "level": "crit", "message": "Accepted key user=alice", "user": "alice"},
		{"_time": time.Date(year, 2, 5, 17, 32, 18, 0, time.UTC).Format(time.RFC3339Nano), "host": "db-02", "app_name": "cron",
			"procid": "42", "facility_label": "user", "severity_label": "notice", "message": "job done status=0", "status": 0.0},
		{"level": "warn"},
	} {
		var got map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &got); err != nil {
			t.Fatal(err)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("line %d: %s = %v, want %v (%s)", i, k, got[k], v, lines[i])
			}
		}
	}
}
