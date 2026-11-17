package exec_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

const logsTimeField = "ts"

// logQuery is a query over the generated logs as PPL text; plan is what the
// text cannot say, or the nearest the executor has.
type logQuery struct {
	name string
	text string
	plan exec.Node
	// ordered is set when the query's sort makes the row order unique.
	ordered bool
}

// logConstants are the values the queries look for in a stream.
type logConstants struct {
	// service is the busiest service, trace the identifier of one event and
	// rare the path of a member that few events of the service carry.
	service, trace, rare string
	// The time windows, of an hour and of ten minutes around the middle of
	// the span.
	hourLo, hourHi, tenLo, tenHi time.Time
}

func logConstantsOf(g *logGen, stream []byte) logConstants {
	c := logConstants{service: g.services[0].name}
	keys := g.services[0].keys
	c.rare = g.keys[keys[len(keys)*3/4]].path()
	const member = `"trace_id":"`
	if at := bytes.Index(stream[len(stream)/2:], []byte(member)); at >= 0 {
		from := len(stream)/2 + at + len(member)
		c.trace = string(stream[from : from+32])
	}
	mid := logsEpoch.Add(g.opts.Span / 2).Truncate(time.Minute)
	c.hourLo, c.hourHi = mid.Add(-30*time.Minute), mid.Add(30*time.Minute)
	c.tenLo, c.tenHi = mid.Add(-5*time.Minute), mid.Add(5*time.Minute)

	return c
}

func logQueries(c logConstants) []logQuery {
	within := func(lo, hi time.Time) string {
		return fmt.Sprintf(`_time >= '%s' and _time < '%s'`, lo.Format(time.RFC3339), hi.Format(time.RFC3339))
	}
	const failed = `level in ('error', 'ERROR', 'fatal', 'FATAL')`
	count := []exec.Agg{{Name: "n", Func: exec.Count}}
	raw := str(shred.RawPath)
	counted := func(filter *exec.Expr) exec.Node {
		return &exec.Aggregate{Input: &exec.Scan{Filter: filter}, Aggs: count}
	}
	sshd := regex(`Failed password for invalid user \w+ from 10\.`)
	posted := regex(`POST /api/v1/\w+/\d+/\w+ HTTP`)

	return []logQuery{
		{name: "C1 count", text: `source=logs | stats count() as n`, ordered: true},
		{name: "G1 count by level", text: `source=logs | stats count() as n by level | sort - n, level`, ordered: true},
		{name: "G2 errors per service and minute, 1h", ordered: true,
			text: `source=logs | where ` + within(c.hourLo, c.hourHi) + ` and ` + failed +
				` | stats count() as n by service, span(_time, 1m) as minute | sort service, minute`},
		{name: "G3 top 10 routes", text: `source=logs | stats count() as n by http.route | sort - n, http.route | head 10`, ordered: true},
		{name: "G4 latency by service", ordered: true,
			text: `source=logs | stats avg(http.latency_ms) as mean, max(http.latency_ms) as worst, count(http.latency_ms) as n by service | sort service`},
		{name: "G5 latency percentiles by service", ordered: true,
			text: `source=logs | stats p50(http.latency_ms) as p50, p95(http.latency_ms) as p95, p99(http.latency_ms) as p99 by service | sort service`},
		{name: "G6 distinct users per service", text: `source=logs | stats distinct_count(user_id) as users by service | sort - users, service`, ordered: true},
		{name: "G11 estimated users per service", text: `source=logs | stats distinct_count_approx(user_id) as users by service | sort service`, ordered: true},
		{name: "G12 failed requests per service", ordered: true,
			text: `source=logs | stats count(http.status) as n, count(eval(http.status >= 500)) as failed by service | sort - failed, service`},
		{name: "G13 latest message per service", text: `source=logs | stats latest(message) as last, max(_time) as at by service | sort service`, ordered: true},
		{name: "G7 top 10 users", text: `source=logs | stats count() as n by user_id | sort - n, user_id | head 10`, ordered: true},
		{name: "G8 groups of trace ids", text: `source=logs | stats count() as n by trace_id | sort - n, trace_id | head 5`, ordered: true},
		{name: "G9 top 10 paths", text: `source=logs | stats count() as n by http.path | sort - n, http.path | head 10`, ordered: true},
		{name: "G10 hosts and pods per service", ordered: true,
			text: `source=logs | stats distinct_count(host) as hosts, distinct_count(pod) as pods, count() as n by service, env | sort service, env`},
		{name: "P1 events of a trace", text: `source=logs | where trace_id = '` + c.trace + `' | fields _time, service, level, message | sort _time`, ordered: true},
		{name: "P2 identifier in any field", text: `source=logs "` + c.trace + `" | stats count() as n`, ordered: true},
		{name: "S1 phrase in message", text: `source=logs | where match_phrase(message, 'connection reset by peer') | stats count() as n`, ordered: true},
		{name: "S2 word in any field", text: `source=logs deadlock | stats count() as n`, ordered: true},
		{name: "S3 regex on message", text: `source=logs | where regexp_match(message, 'timeout after \d+ms waiting') | stats count() as n`, ordered: true},
		{name: "S4 regex on text lines", text: `source=logs | where regexp_match(_raw, 'Failed password for invalid user \w+ from 10\.') | stats count() as n`,
			plan: counted(exec.Match(raw, sshd)), ordered: true},
		{name: "S5 regex of classes on text lines", text: `source=logs | where regexp_match(_raw, 'POST /api/v1/\w+/\d+/\w+ HTTP') | stats count() as n`,
			plan: counted(exec.Match(raw, posted)), ordered: true},
		{name: "S6 word on text lines", text: `source=logs sshd | stats count() as n`,
			plan: counted(exec.Match(raw, tokenfilter.Word("sshd").IgnoreCase())), ordered: true},
		{name: "S7 word in stack traces", ordered: true,
			text: `source=logs | where match(error.stack, 'DeadlockLoserDataAccessException') | stats count() as n by service | sort - n, service`},
		{name: "S8 substring in message", text: `source=logs | where like(message, '%exhaust%') | stats count() as n`, ordered: true},
		{name: "T1 latest 100 errors of a service", ordered: true,
			text: `source=logs | where service = '` + c.service + `' and ` + failed + ` | fields _time, host, message | sort - _time, message | head 100`},
		{name: "T2 server errors by service, 1h", ordered: true,
			text: `source=logs | where ` + within(c.hourLo, c.hourHi) + ` and http.status >= 500 | stats count() as n, avg(http.latency_ms) as mean by service | sort - n, service`},
		{name: "T3 count in 10 minutes", text: `source=logs | where ` + within(c.tenLo, c.tenHi) + ` | stats count() as n`, ordered: true},
		{name: "T4 slowest 10 requests, 1h", ordered: true,
			text: `source=logs | where ` + within(c.hourLo, c.hourHi) + ` and http.latency_ms > 2000 | fields http.latency_ms, service, http.path | sort - http.latency_ms, http.path | head 10`},
		{name: "M1 mixed field equals text", text: `source=logs | where code = 'E1000' | stats count() as n`, ordered: true},
		{name: "M2 mixed field equals number", text: `source=logs | where code = 1000 | stats count() as n`, ordered: true},
		{name: "M3 mixed field in a range", text: `source=logs | where code >= 1400 | stats count() as n, min(code) as least, max(code) as most`, ordered: true},
		{name: "M4 kinds of the mixed field", text: `source=logs | eval kind = typeof(code) | stats count() as n by kind | sort kind`, ordered: true},
		{name: "E1 errors by type", text: `source=logs | where isnotnull(error.type) | stats count() as n by error.type | sort - n, error.type`, ordered: true},
		{name: "E2 no user", text: `source=logs | where isnull(user_id) | stats count() as n`, ordered: true},
		{name: "E3 rare member is there", text: `source=logs | where isnotnull(` + c.rare + `) | stats count() as n`, ordered: true},
		{name: "E4 events without a request", text: `source=logs | where isnull(http.status) | stats count() as n by env | sort env`, ordered: true},
	}
}

// logRefusal says why the language refused a query, in a few words.
func logRefusal(err error) string {
	return strings.TrimPrefix(strings.TrimPrefix(err.Error(), "irplan: "), "not supported: ")
}

// TestLogsBenchReference requires, for a small stream loaded through a
// store in arrival order and by a sort key, the answers of the reference
// evaluator to every query of the logs benchmark. LYNXDB_LOGSBENCH_REFERENCE
// turns it on and is the number of lines.
func TestLogsBenchReference(t *testing.T) {
	if os.Getenv("LYNXDB_LOGSBENCH_REFERENCE") == "" {
		t.Skip("set LYNXDB_LOGSBENCH_REFERENCE to a number of lines")
	}
	opts := logGenOptions{Seed: 3, Events: logsEnvInt(t, "LYNXDB_LOGSBENCH_REFERENCE", 0), Span: 3 * time.Hour, Text: 0.2, WrapText: true}
	gen := newLogGen(opts)
	var stream bytes.Buffer
	if _, err := gen.write(&stream); err != nil {
		t.Fatal(err)
	}
	var rows []refRow
	for line := range bytes.Lines(stream.Bytes()) {
		rows = append(rows, flatten(t, bytes.TrimSuffix(line, []byte{'\n'}), logsTimeField))
	}
	queries := logQueries(logConstantsOf(gen, stream.Bytes()))
	for _, key := range [][]string{nil, {"service", "level"}} {
		store, err := colstore.OpenStore(t.TempDir(), colstore.StoreOptions{TimeField: logsTimeField, SortKey: key,
			BlockRows: 2048, FlushBytes: 4 << 20, FlushInterval: -1})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Append(stream.Bytes()); err != nil {
			t.Fatal(err)
		}
		if err := errors.Join(store.Sync(), store.Settle()); err != nil {
			t.Fatal(err)
		}
		snap := store.Table().Snapshot()
		for _, q := range queries {
			plans := map[string]exec.Node{}
			if plan, err := pplPlan(q.text, logsEpoch.Add(opts.Span)); err == nil {
				plans[""] = plan
			}
			if q.plan != nil {
				plans[", by hand"] = q.plan
			}
			for name, plan := range plans {
				for _, workers := range []int{4, 1} {
					t.Run(fmt.Sprintf("%s/%s%s/%d", layoutName(key), q.name, name, workers), func(t *testing.T) {
						check(t, snap, rows, plan, q.ordered, workers)
					})
				}
			}
		}
		snap.Release()
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
