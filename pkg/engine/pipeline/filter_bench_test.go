package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

// makeFilterBenchEvents creates n events, with ~5% containing /user_ in _raw.
func makeFilterBenchEvents(n int) []*event.Event {
	events := make([]*event.Event, n)
	for i := range events {
		var raw string
		if i%20 == 0 { // 5% match rate
			raw = fmt.Sprintf(`2026-02-14T14:23:01Z INFO api-gw connection refused to /user_service/health status=502 trace=%d`, i)
		} else {
			raw = fmt.Sprintf(`2026-02-14T14:23:01Z INFO api-gw request completed status=200 duration=45 trace=%d`, i)
		}
		ev := event.NewEvent(time.Now(), raw)
		ev.Source = "bench"
		ev.Index = "main"
		events[i] = ev
	}

	return events
}

// BenchmarkSearchExprIterator_Wildcard benchmarks SearchExprIterator with wildcard pattern.
func BenchmarkSearchExprIterator_Wildcard(b *testing.B) {
	events := makeFilterBenchEvents(1024)
	expr := &spl2.SearchKeywordExpr{
		Value:       "*/user_*",
		HasWildcard: true,
	}
	eval := spl2.NewSearchEvaluator(expr)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		search := NewSearchExprIteratorWithExpr(scan, eval, expr)
		_ = search.Init(ctx)
		batch, _ := search.Next(ctx)
		if batch == nil || batch.Len == 0 {
			b.Fatal("expected matches")
		}
	}
}

// BenchmarkSearchExprIterator_Literal benchmarks SearchExprIterator with literal (no wildcard).
func BenchmarkSearchExprIterator_Literal(b *testing.B) {
	events := makeFilterBenchEvents(1024)
	expr := &spl2.SearchKeywordExpr{
		Value:       "connection refused",
		HasWildcard: false,
	}
	eval := spl2.NewSearchEvaluator(expr)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		search := NewSearchExprIteratorWithExpr(scan, eval, expr)
		_ = search.Init(ctx)
		batch, _ := search.Next(ctx)
		if batch == nil || batch.Len == 0 {
			b.Fatal("expected matches")
		}
	}
}

// BenchmarkScanIterator benchmarks the raw ScanIterator (1024-batch, scan only).
func BenchmarkScanIterator(b *testing.B) {
	events := makeFilterBenchEvents(1024)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		_ = scan.Init(ctx)
		batch, _ := scan.Next(ctx)
		if batch == nil || batch.Len != 1024 {
			b.Fatal("expected 1024 rows")
		}
	}
}

// BenchmarkSearchExprIterator_FullScan benchmarks with match-all wildcard.
func BenchmarkSearchExprIterator_FullScan(b *testing.B) {
	events := makeFilterBenchEvents(1024)
	expr := &spl2.SearchKeywordExpr{
		Value:       "*",
		HasWildcard: true,
	}
	eval := spl2.NewSearchEvaluator(expr)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scan := NewScanIterator(events, 1024)
		search := NewSearchExprIteratorWithExpr(scan, eval, expr)
		_ = search.Init(ctx)
		batch, _ := search.Next(ctx)
		if batch == nil || batch.Len != 1024 {
			b.Fatal("expected all rows")
		}
	}
}
