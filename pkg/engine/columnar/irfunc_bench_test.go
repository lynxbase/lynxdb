package columnar

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec"
	"github.com/lynxbase/lynxdb/pkg/colstore/exec/irfunc"
	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

const benchRows = 1 << 20

var (
	benchOnce  sync.Once
	benchTable *colstore.Table
)

// benchTableOf builds a table of 1M events: msg is different in every
// event, host is one of 16 names and becomes a dictionary column, payload
// is JSON text.
func benchTableOf(b *testing.B) *colstore.Table {
	benchOnce.Do(func() {
		r := rand.New(rand.NewPCG(3, 5))
		var out bytes.Buffer
		w := part.NewWriter(&out, part.Options{})
		batch := shred.NewBatch(shred.Options{TimeField: "ts"})
		start := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC).UnixMicro()
		for i := 0; i < benchRows; i++ {
			doc := fmt.Sprintf(`{"ts":%d,"host":"host-%02d","msg":"request %d served in %dms by worker %x","payload":"{\"a\":%d,\"b\":\"x%d\"}"}`,
				start+int64(i)*1000, r.IntN(16), i, r.IntN(5000), r.Uint32(), r.IntN(1000), i%7)
			if err := batch.AddJSON([]byte(doc), 0); err != nil {
				panic(err)
			}
			if batch.Rows == 16384 {
				if err := w.WriteBatch(batch); err != nil {
					panic(err)
				}
				batch = shred.NewBatch(shred.Options{TimeField: "ts"})
			}
		}
		if batch.Rows > 0 {
			if err := w.WriteBatch(batch); err != nil {
				panic(err)
			}
		}
		if err := w.Close(); err != nil {
			panic(err)
		}
		p, err := part.Open(out.Bytes())
		if err != nil {
			panic(err)
		}
		benchTable = colstore.NewTable()
		if err := benchTable.Apply([]*colstore.Handle{colstore.NewHandle(p, nil)}, nil); err != nil {
			panic(err)
		}
	})

	return benchTable
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}

	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

// benchArg is the executor's expression for an argument of a call.
func benchArg(e ir.Expr) *exec.Expr {
	switch e := e.(type) {
	case *ir.FieldRef:
		if e.Name == ir.TimeColumn {
			return exec.Time()
		}
		return exec.Col(e.Name, exec.KindAny)
	case *ir.Literal:
		if s, ok := e.V.(string); ok {
			return exec.Str(s)
		}
		return exec.Int(e.V.(int64))
	}
	panic(fmt.Sprintf("benchArg %T", e))
}

// BenchmarkFunctions counts the rows where a function gives a value, with
// the function native and through the bridge to the row engine. Run it with
// -cpu 1; cpu-ns/row is the process's CPU time per row.
func BenchmarkFunctions(b *testing.B) {
	snap := benchTableOf(b).Snapshot()
	defer snap.Release()
	field := func(name string, t ir.Type) *ir.FieldRef { return &ir.FieldRef{Name: name, T: t} }
	str := func(s string) *ir.Literal { return &ir.Literal{V: s, T: ir.String} }
	cases := []struct {
		name string
		call *ir.Call
	}{
		{"reverse_msg", &ir.Call{Func: ir.MustFunc("reverse"), Args: []ir.Expr{field("msg", ir.Any)}, T: ir.String}},
		{"reverse_host_dictionary", &ir.Call{Func: ir.MustFunc("reverse"), Args: []ir.Expr{field("host", ir.Any)}, T: ir.String}},
		{"left_msg", &ir.Call{Func: ir.MustFunc("left"), Args: []ir.Expr{field("msg", ir.Any), &ir.Literal{V: int64(7), T: ir.Int}}, T: ir.String}},
		{"date_format_time", &ir.Call{Func: ir.MustFunc("date_format"), Args: []ir.Expr{field(ir.TimeColumn, ir.Timestamp), str("%Y-%m-%d %H:%i")}, T: ir.String}},
		{"json_extract_payload", &ir.Call{Func: ir.MustFunc("json_extract"), Args: []ir.Expr{field("payload", ir.Any), str("$.a")}, T: ir.Any}},
	}
	run := func(b *testing.B, plan exec.Node) {
		b.ReportAllocs()
		cpu := processCPU()
		for i := 0; i < b.N; i++ {
			res, err := exec.Run(context.Background(), snap, plan, exec.Options{Workers: 1})
			if err != nil {
				b.Fatal(err)
			}
			if n := res.Value(0, 0).Int; n == 0 {
				b.Fatalf("no row has a value")
			}
		}
		used := processCPU() - cpu
		b.ReportMetric(float64(used.Nanoseconds())/float64(b.N)/benchRows, "cpu-ns/row")
	}
	// The executor's own operations over the same columns are the floor.
	for name, e := range map[string]*exec.Expr{
		"length_msg_operation":     exec.Length(exec.Col("msg", exec.KindAny)),
		"substr_msg_operation":     exec.Substr(exec.Col("msg", exec.KindAny), exec.Int(0), exec.Int(7)),
		"length_payload_operation": exec.Length(exec.Col("payload", exec.KindAny)),
		"hour_time_operation":      exec.HourOfDay(exec.Time()),
	} {
		plan := &exec.Aggregate{Input: &exec.Scan{}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count, Arg: e}}}
		b.Run(name, func(b *testing.B) { run(b, plan) })
	}
	bridge := opaque(testNow)
	for _, c := range cases {
		args := make([]*exec.Expr, len(c.call.Args))
		var fields []*ir.FieldRef
		var fieldArgs []*exec.Expr
		for i, a := range c.call.Args {
			args[i] = benchArg(a)
			if f, ok := a.(*ir.FieldRef); ok {
				fields = append(fields, f)
				fieldArgs = append(fieldArgs, args[i])
			}
		}
		ways := map[string]*exec.Expr{}
		if newFunc, ok := bridge(c.call, fields); ok {
			ways["bridge"] = exec.Call(newFunc, fieldArgs...)
		}
		if build, ok := irfunc.Lookup(c.call.Func.Name); ok {
			if newFunc, err := build(c.call, irfunc.Env{Now: testNow}); err == nil {
				ways["native"] = exec.Call(newFunc, args...)
			}
		}
		for _, way := range []string{"native", "bridge"} {
			e, ok := ways[way]
			if !ok {
				continue
			}
			plan := &exec.Aggregate{Input: &exec.Scan{}, Aggs: []exec.Agg{{Name: "n", Func: exec.Count, Arg: e}}}
			b.Run(c.name+"/"+way, func(b *testing.B) { run(b, plan) })
		}
	}
}
