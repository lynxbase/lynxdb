package query

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/physical"
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

var t0 = time.Date(2024, 2, 1, 9, 0, 0, 0, time.UTC)

func ev(min int, raw string, fields map[string]event.Value) *event.Event {
	e := event.NewEvent(t0.Add(time.Duration(min)*time.Minute), raw)
	e.Index = "logs"
	for k, v := range fields {
		e.SetField(k, v)
	}
	return e
}

func s(v string) event.Value  { return event.StringValue(v) }
func i(v int64) event.Value   { return event.IntValue(v) }
func f(v float64) event.Value { return event.FloatValue(v) }

func testStore() physical.MapStore {
	return physical.MapStore{
		"logs": {
			ev(0, "GET /api/users 200 user=alice@example.com", map[string]event.Value{"host": s("web-1"), "status": i(200), "bytes": i(100), "level": s("INFO"), "service": s("api")}),
			ev(1, "POST /api/orders 500 connection refused", map[string]event.Value{"host": s("web-2"), "status": i(500), "bytes": i(50), "level": s("ERROR"), "service": s("api")}),
			ev(2, "GET /health 200", map[string]event.Value{"host": s("web-1"), "status": i(200), "bytes": i(10), "level": s("INFO"), "service": s("web")}),
			ev(7, "GET /api/users 404 user=bob@example.org", map[string]event.Value{"host": s("web-3"), "status": i(404), "bytes": i(30), "level": s("WARN"), "service": s("api")}),
			ev(12, "POST /api/pay 503 connection timeout", map[string]event.Value{"host": s("web-2"), "status": i(503), "bytes": i(70), "level": s("ERROR"), "service": s("pay"), "tags": event.ArrayValue([]event.Value{s("x"), s("y")})}),
		},
		"users": {
			ev(0, "", map[string]event.Value{"host": s("web-1"), "owner": s("ops")}),
			ev(0, "", map[string]event.Value{"host": s("web-2"), "owner": s("dev")}),
		},
	}
}

func run(t *testing.T, q string) string {
	t.Helper()
	rows, err := Execute(context.Background(), q, testStore(), Options{Now: t0.Add(time.Hour)})
	if err != nil {
		return "ERROR " + err.Error()
	}
	return rowsText(rows)
}

func rowsText(rows []map[string]event.Value) string {
	var lines []string
	for _, r := range rows {
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			v := r[k]
			var val string
			switch v.Type() {
			case event.FieldTypeTimestamp:
				val = v.AsTimestamp().UTC().Format("15:04")
			case event.FieldTypeNull:
				val = "null"
			case event.FieldTypeString:
				val = strconv.Quote(v.AsString())
			default:
				b, _ := json.Marshal(v.Interface())
				val = string(b)
				if v.Type() == event.FieldTypeArray || v.Type() == event.FieldTypeObject {
					val = v.String()
				}
			}
			parts = append(parts, k+"="+val)
		}
		lines = append(lines, strings.Join(parts, " "))
	}
	return strings.Join(lines, "\n")
}

// TestCorrelatedSubqueriesFailToCompile pins subqueries that read a field of
// the query around them: they fail to compile instead of matching nothing.
func TestCorrelatedSubqueriesFailToCompile(t *testing.T) {
	for q, field := range map[string]string{
		"source=logs | where exists [ source=users | where host = logs.host ] | fields host":                                   "logs.host",
		"source=logs | eval top = 100 | where host in [ source=users | fields host, owner | where owner = top | fields host ]": "field top",
	} {
		_, err := Compile(q)
		pe, ok := ppl.AsError(err)
		if !ok || pe.Kind != ppl.ErrUnsupportedFeature || !strings.Contains(pe.Msg, "correlated subqueries are not supported") || !strings.Contains(pe.Msg, field) {
			t.Errorf("%s: got %v, want an unsupported_feature error naming %s", q, err, field)
		}
	}
}

func TestExecute(t *testing.T) {
	cases := []struct {
		q, want string
	}{
		{"source=logs | where status >= 500 | fields host, status",
			"host=\"web-2\" status=500\nhost=\"web-2\" status=503"},
		{"search source=logs connection refused | fields host",
			`host="web-2"`},
		{"search source=logs conn* NOT timeout | fields status",
			"status=500"},
		{"source=logs | where match(level, 'error') | fields status",
			"status=500\nstatus=503"},
		{"source=logs | where match(host, '2 3') | fields status",
			"status=500\nstatus=404\nstatus=503"},
		{"source=logs | where match(host, 'web 3', operator='AND') | fields status",
			"status=404"},
		{"source=logs | where match_phrase(_raw, 'connection Timeout') | fields status",
			"status=503"},
		{"source=logs | where match_phrase_prefix(_raw, 'conn') | fields status",
			"status=500\nstatus=503"},
		{"source=logs | where match_phrase_prefix(_raw, 'connection ref') | fields status",
			"status=500"},
		{`search source=logs "connection timeout" | fields status`,
			"status=503"},
		{"search source=logs status=404 OR level=ERROR | fields status",
			"status=500\nstatus=404\nstatus=503"},
		{"search source=logs host=web-* host!=web-2 | fields host",
			"host=\"web-1\"\nhost=\"web-1\"\nhost=\"web-3\""},
		{"source=logs earliest='2024-02-01 09:05:00' | fields status",
			"status=404\nstatus=503"},
		{"source=logs | stats count() as n, sum(bytes) as b by level | sort level",
			"b=120 level=\"ERROR\" n=2\nb=110 level=\"INFO\" n=2\nb=30 level=\"WARN\" n=1"},
		{"source=logs | stats count() by span(@timestamp, 5m) | sort `span(@timestamp,5m)`",
			"count()=3 span(@timestamp,5m)=09:00\ncount()=1 span(@timestamp,5m)=09:05\ncount()=1 span(@timestamp,5m)=09:10"},
		{"source=logs | stats count(eval(status >= 500)) as errors, dc(host) as hosts",
			"errors=2 hosts=3"},
		{"source=logs | stats earliest(status, @timestamp) as e, latest(status, _time) as l, earliest(host) as h",
			"e=200 h=\"web-1\" l=503"},
		{"source=logs | eventstats max(bytes) as mx by service | where bytes = mx | fields host",
			"host=\"web-1\"\nhost=\"web-1\"\nhost=\"web-2\""},
		{"source=logs | streamstats count() as n | fields n",
			"n=1\nn=2\nn=3\nn=4\nn=5"},
		{"source=logs | dedup host | fields host",
			"host=\"web-1\"\nhost=\"web-2\"\nhost=\"web-3\""},
		{"source=logs | sort - bytes | head 2 | fields bytes",
			"bytes=100\nbytes=70"},
		{"source=logs | head 2 from 3 | fields status",
			"status=404\nstatus=503"},
		{"source=logs | top 2 host",
			"count=2 host=\"web-1\"\ncount=2 host=\"web-2\""},
		{"source=logs | rare level",
			"count=1 level=\"WARN\"\ncount=2 level=\"ERROR\"\ncount=2 level=\"INFO\""},
		{"source=logs | parse _raw '(?<method>[A-Z]+) (?<path>\\S+)' | where path = '/health' | fields method",
			`method="GET"`},
		{"source=logs | grok _raw '%{WORD:verb} %{URIPATH:uri} %{NUMBER:code}' | where code = '404' | fields verb, uri",
			`uri="/api/users" verb="GET"`},
		{"source=logs | rex field=_raw 'user=(?<email>\\S+)' | where isnotnull(email) | fields email",
			"email=\"alice@example.com\"\nemail=\"bob@example.org\""},
		{"source=logs | regex host = '^web-[13]$' | fields host",
			"host=\"web-1\"\nhost=\"web-1\"\nhost=\"web-3\""},
		{"source=logs | eval doc = json_object('a', status) | spath input=doc path=a output=st | where st > 400 | fields st",
			"st=500\nst=404\nst=503"},
		{"source=logs | where status = 200 | join left=l right=r on l.host = r.host users | fields host, owner",
			"host=\"web-1\" owner=\"ops\"\nhost=\"web-1\" owner=\"ops\""},
		{"source=logs | lookup users host replace owner | where isnotnull(owner) | fields status, owner",
			"owner=\"ops\" status=200\nowner=\"dev\" status=500\nowner=\"ops\" status=200\nowner=\"dev\" status=503"},
		{"source=logs | where host in [ source=users | where owner = 'dev' | fields host ] | fields status",
			"status=500\nstatus=503"},
		{"source=logs | where bytes > [ source=logs | stats avg(bytes) ] | fields bytes",
			"bytes=100\nbytes=70"},
		{"source=logs | where status = 404 | append [ search source=users | where owner = 'ops' ] | fields host",
			"host=\"web-3\"\nhost=\"web-1\""},
		{"source=logs | stats count() as n by level | sort level | appendpipe [ stats sum(n) as n ] | fields n",
			"n=2\nn=2\nn=1\nn=5"},
		{"source=logs | where status = 503 | eval z = nul | fillnull with 0 in z | fields z",
			"z=0"},
		{"source=logs | where status = 503 | expand tags as tag | fields tag",
			"tag=\"x\"\ntag=\"y\""},
		{"source=logs | where status = 503 | mvexpand tags | fields tags",
			"tags=\"x\"\ntags=\"y\""},
		{"source=logs | trendline sma(2, bytes) as avg2 | fields avg2",
			"avg2=null\navg2=75\navg2=30\navg2=20\navg2=50"},
		{"source=logs | timechart span=5m count() by service",
			"_time=09:00 count()=2 service=\"api\"\n_time=09:00 count()=1 service=\"web\"\n_time=09:05 count()=1 service=\"api\"\n_time=09:10 count()=1 service=\"pay\""},
		{"source=logs | timechart span=10m limit=1 count() by service",
			"_time=09:00 count()=1 service=\"OTHER\"\n_time=09:00 count()=3 service=\"api\"\n_time=09:10 count()=1 service=\"OTHER\""},
		{"source=logs | chart count() by level, service",
			"count()=1 level=\"ERROR\" service=\"api\"\ncount()=1 level=\"ERROR\" service=\"pay\"\ncount()=1 level=\"INFO\" service=\"api\"\ncount()=1 level=\"INFO\" service=\"web\"\ncount()=1 level=\"WARN\" service=\"api\""},
		{"source=logs | stats count() as n by level | sort level | transpose",
			"column=\"level\" row 1=\"ERROR\" row 2=\"INFO\" row 3=\"WARN\"\ncolumn=\"n\" row 1=2 row 2=2 row 3=1"},
		{"source=logs | where status = 200 | fields host, bytes | addcoltotals labelfield=host label='Sum'",
			"bytes=100 host=\"web-1\"\nbytes=10 host=\"web-1\"\nbytes=110 host=\"Sum\""},
		{"source=logs | fields service, host | mvcombine host | where service = 'api' | fields host",
			`host=["web-1","web-2","web-3"]`},
		{"makeresults count=2 | eval x = 1 | fields x",
			"x=1\nx=1"},
		{"makeresults format=json data='[{\"a\":1},{\"a\":2}]' | stats sum(a) as total",
			"total=3"},
		{"source=logs | replace 'web-*' WITH 'host-*' IN host | dedup host | fields host",
			"host=\"host-1\"\nhost=\"host-2\"\nhost=\"host-3\""},
		{"source=logs | eval b = tostring(bytes) | convert num(b) as n | where n > 60 | fields n",
			"n=100\nn=70"},
		{"source=logs | bin bytes span=50 as bucket | dedup bucket | fields bucket",
			"bucket=\"100-150\"\nbucket=\"50-100\"\nbucket=\"0-50\""},
		{"source=logs | where status = 200 | reverse | fields bytes",
			"bytes=10\nbytes=100"},
		{"source=logs | patterns _raw mode=aggregation | where pattern_count > 1 | fields patterns_field, pattern_count",
			"pattern_count=2 patterns_field=\"<*> /<*>/<*> <*> <*>=<*>@<*>.<*>\"\npattern_count=2 patterns_field=\"<*> /<*>/<*> <*> <*> <*>\""},
		{"union [search source=logs | where status = 404] [search source=users | where owner = 'dev'] | fields host",
			"host=\"web-3\"\nhost=\"web-2\""},
		{"source=logs | stats count() as c by service, level | xyseries service level in ('ERROR', 'INFO') c",
			"c: ERROR=1 c: INFO=1 service=\"api\"\nc: ERROR=1 c: INFO=null service=\"pay\"\nc: ERROR=null c: INFO=1 service=\"web\""},
		{"describe users | fields COLUMN_NAME, TYPE_NAME",
			"COLUMN_NAME=\"_time\" TYPE_NAME=\"timestamp\"\nCOLUMN_NAME=\"host\" TYPE_NAME=\"string\"\nCOLUMN_NAME=\"index\" TYPE_NAME=\"string\"\nCOLUMN_NAME=\"owner\" TYPE_NAME=\"string\""},
	}
	for _, c := range cases {
		got := run(t, c.q)
		if got != c.want {
			t.Errorf("%s\n got:\n%s\nwant:\n%s", c.q, got, c.want)
		}
	}
}
