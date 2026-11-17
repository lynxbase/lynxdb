package irplan

import (
	"context"
	"fmt"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/ppl"
)

// compileText compiles a query and reports a panic of the language, which
// has fuzz tests of its own, as an error.
func compileText(text string) (res *ppl.Result, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("ppl: %v", r)
		}
	}()

	return ppl.Compile(text)
}

// FuzzPlan requires that any text the language takes is planned or refused,
// and that a plan runs or is refused by the executor, without a panic.
func FuzzPlan(f *testing.F) {
	for _, text := range []string{
		`source=logs disk "disk full" fail* NOT host=b | stats count()`,
		`source=logs | where status >= 500 and not like(msg, '%node%') | stats count() as n, avg(dur) as mean by level | sort - n | head 2`,
		`source=logs | eval k = case(status >= 500, 'bad' else 'ok'), s = substring(msg, 1, 4) | fields k, s, status | sort - status | head 2`,
		`source=logs | stats latest(msg) as last, percentile(dur, 50) as mid by host | where mid > 1 | eval up = upper(last)`,
		`source=logs | dedup host | fields host, status, _time | sort host`,
		`source=logs | rename host as node | eval slow = dur > 1.0 | fields - msg | sort - status | head 3`,
		`source=logs | dedup host | eval code = status + 1 | sort - code | fields - msg, code`,
		`source=logs | stats count() as n by host | sort - n | head 1 | fields host`,
		`source=logs | stats count() as n by level | eventstats sum(n) as all | eval share = round(n * 100.0 / all, 1) | sort level`,
		`source=logs earliest=-4h latest=-1h | where regexp_match(msg, '[Dd]isk f(ull|ailure)') | top 2 host`,
		`source=logs | stats count() as n by span(_time, 2h) as at, span(status, 100) as code | sort at | head 1`,
	} {
		f.Add(text)
	}
	snap := testSnapshot(f)
	f.Fuzz(func(t *testing.T, text string) {
		res, err := compileText(text)
		if err != nil {
			return
		}
		plan, err := Plan(res.Plan, Options{Now: testNow})
		if err != nil {
			return
		}
		_, _ = exec.Run(context.Background(), snap, plan, exec.Options{Workers: 2})
	})
}
