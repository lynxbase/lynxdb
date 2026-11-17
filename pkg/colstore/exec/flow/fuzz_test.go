package flow

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
)

// FuzzCompile requires that any text either is refused or gives a plan that
// runs to an answer or an error.
func FuzzCompile(f *testing.F) {
	for _, text := range []string{
		`from logs disk | count`,
		`from logs level=info NOT host=b | count`,
		`from logs "disk full" h* | count`,
		`from logs[-1h] | where status >= 500 and not has(msg, "node") | count`,
		`from logs[-8h..-2h] | where matches(msg, r"[Dd]isk f(ull|ailure)") | count`,
		`from logs | where matches(_raw, r"(?i)^(warn|not found)$") or contains(msg, "x") | count`,
		`from logs | extend slow = dur > 1.0, n = len(msg) % 3 | keep status, slow, n | sort -status | head 2`,
		`from logs | keep level, status, host | drop host | rename status as code | sort -code, level | limit 1`,
		`from logs | stats count() as n, avg(dur) as mean, max(status) as top, dc(host) as hosts, stdev(dur) as sd by level | sort level`,
		`from logs | stats latest(msg) as last, earliest(status) as first, arg_max(msg, dur) as slow by host | where first >= 200 | sort host`,
		`from logs | stats min(msg) as lo, max(msg) as hi, first(host) as f by level | extend up = upper(hi) | sort level`,
		`from logs | where status < 500 | dedup level, host | extend code = status + 1 | keep level, code | sort level`,
		`from logs | where status in [200, 404] and dur between 0.1 and 2 | count by level | sort level`,
		`from logs | extend k = if(status >= 500, "bad", coalesce(level, "none")), t = typeof(dur) | stats count() by k, t`,
		`from logs | extend h = date_part(_time, "hour"), b = bin(_time, 5m) | stats count() by h, b | sort h, b`,
		`from logs | where exists(msg) and not is_null(dur) and is_missing(nope) | stats sum(dur * 2 - 1) as s`,
		`from logs | where _time >= timestamp("2023-11-14T00:00:00Z") | keep _time, msg | sort -_time | head 3`,
		`from logs | extend s = substr(msg, 2, 3) + "!" , r = replace(lower(msg), "disk", "d") | keep s, r | sort s | head 2`,
		`from logs | stats count() as n by host | where n > 2 | extend m = n / 0 | sort host`,
		`from logs | where has_glob(msg, "req*") and starts_with(host, "a") and ends_with(level, "o") | count`,
		`from logs | top 5 host`,
		`from logs | stats p95(dur) by host`,
		`from logs | where (`,
	} {
		f.Add(text)
	}
	snap := testSnapshot(f)
	f.Fuzz(func(t *testing.T, text string) {
		plan, err := Compile(text, Options{Now: testNow})
		if err != nil {
			return
		}
		if _, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2}); err != nil {
			return
		}
	})
}
