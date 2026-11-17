package exec_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// logShapes counts what a generated stream holds.
type logShapes struct {
	lines, objects, access, syslog int
	paths                          map[string]struct{}
	services, levels               map[string]struct{}
	stacks, nullUsers, tags        int
	wholeLatency, fracLatency      int
	textCode, numberCode           int
	outOfOrder, late               int
}

func logPaths(into map[string]struct{}, prefix string, obj map[string]any) {
	for name, v := range obj {
		if sub, ok := v.(map[string]any); ok && len(sub) > 0 {
			logPaths(into, prefix+name+".", sub)

			continue
		}
		into[prefix+name] = struct{}{}
	}
}

func shapesOf(t *testing.T, stream []byte) logShapes {
	t.Helper()
	s := logShapes{paths: map[string]struct{}{}, services: map[string]struct{}{}, levels: map[string]struct{}{}}
	var newest int64
	for _, line := range bytes.Split(bytes.TrimSuffix(stream, []byte("\n")), []byte("\n")) {
		s.lines++
		if line[0] != '{' {
			if bytes.Contains(line, []byte(` HTTP/1.1" `)) && bytes.Contains(line, []byte(" +0000] ")) {
				s.access++
			} else {
				s.syslog++
			}

			continue
		}
		s.objects++
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		var doc map[string]any
		if err := dec.Decode(&doc); err != nil {
			t.Fatalf("%v in %s", err, line)
		}
		logPaths(s.paths, "", doc)
		s.services[doc["service"].(string)] = struct{}{}
		s.levels[doc["level"].(string)] = struct{}{}
		ts, err := doc["ts"].(json.Number).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if ts < newest {
			s.outOfOrder++
		}
		if ts < newest-time.Minute.Microseconds() {
			s.late++
		}
		newest = max(newest, ts)
		if len(doc["trace_id"].(string)) != 32 || len(doc["span_id"].(string)) != 16 {
			t.Fatalf("identifiers of %s", line)
		}
		if fail, ok := doc["error"].(map[string]any); ok && strings.Count(fail["stack"].(string), "\n") > 2 {
			s.stacks++
		}
		if user, ok := doc["user_id"]; ok && user == nil {
			s.nullUsers++
		}
		if _, ok := doc["tags"].([]any); ok {
			s.tags++
		}
		if http, ok := doc["http"].(map[string]any); ok {
			if strings.Contains(http["latency_ms"].(json.Number).String(), ".") {
				s.fracLatency++
			} else {
				s.wholeLatency++
			}
		}
		switch doc["code"].(type) {
		case string:
			s.textCode++
		case json.Number:
			s.numberCode++
		}
	}

	return s
}

func TestLogsGeneratorShapes(t *testing.T) {
	opts := logGenOptions{Seed: 7, Events: 6000, Span: time.Hour, Text: 0.2}
	var first, again, other bytes.Buffer
	if _, err := newLogGen(opts).write(&first); err != nil {
		t.Fatal(err)
	}
	if _, err := newLogGen(opts).write(&again); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), again.Bytes()) {
		t.Fatal("two streams of one seed differ")
	}
	opts.Seed++
	if _, err := newLogGen(opts).write(&other); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first.Bytes(), other.Bytes()) {
		t.Fatal("two seeds give one stream")
	}
	opts.Seed--
	opts.WrapText = true
	var wrapped bytes.Buffer
	if _, err := newLogGen(opts).write(&wrapped); err != nil {
		t.Fatal(err)
	}
	plain := bytes.Split(first.Bytes(), []byte("\n"))
	for i, line := range bytes.Split(wrapped.Bytes(), []byte("\n")) {
		if bytes.Equal(line, plain[i]) {
			continue
		}
		var doc struct {
			TS  int64  `json:"ts"`
			Raw string `json:"_raw"`
		}
		if err := json.Unmarshal(line, &doc); err != nil || doc.Raw != string(plain[i]) || doc.TS == 0 {
			t.Fatalf("line %d is wrapped as %s, not %s", i, line, plain[i])
		}
	}

	s := shapesOf(t, first.Bytes())
	t.Logf("%d lines, %d bytes: %d events of %d services and %d levels with %d paths, %d access and %d syslog lines; "+
		"%d stack traces, %d null users, %d tag lists, latency %d whole and %d fractional, code %d text and %d number, %d out of order, %d late",
		s.lines, first.Len(), s.objects, len(s.services), len(s.levels), len(s.paths), s.access, s.syslog,
		s.stacks, s.nullUsers, s.tags, s.wholeLatency, s.fracLatency, s.textCode, s.numberCode, s.outOfOrder, s.late)
	for name, ok := range map[string]bool{
		"every line":             s.lines == opts.Events,
		"text of both kinds":     s.access > opts.Events/10 && s.syslog > opts.Events/30,
		"many services":          len(s.services) > 40,
		"levels in two cases":    len(s.levels) >= 8,
		"hundreds of paths":      len(s.paths) > 500,
		"stack traces":           s.stacks > 20,
		"null users":             s.nullUsers > 100,
		"tag lists":              s.tags > 500,
		"latency of both kinds":  s.wholeLatency > 100 && s.fracLatency > 100,
		"code of both kinds":     s.textCode > 50 && s.numberCode > 50,
		"jitter and late events": s.outOfOrder > opts.Events/50 && s.late > 5,
	} {
		if !ok {
			t.Errorf("the stream lacks %s", name)
		}
	}
}
