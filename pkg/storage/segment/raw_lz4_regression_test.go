package segment

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// TestRawColumnLZ4Roundtrip is a regression test for the in-memory flush path
// (server/flush.go): NewWriter -> Write(events) -> OpenSegment -> read back.
//
// The _raw column is LZ4-encoded. lz4.CompressBlock can return a compressed
// size exactly equal to the uncompressed size for small/short log lines, which
// previously collided with the decoder's "stored raw" marker and produced
// "segment: read _raw: column: corrupt data: truncated string data at index 0"
// on queries as simple as `*`.
func TestRawColumnLZ4Roundtrip(t *testing.T) {
	gens := []struct {
		name string
		gen  func(i int) string
	}{
		// Short repetitive lines: lz4 often compresses these to exactly the
		// uncompressed size — the case that triggered the original bug.
		{"short-repetitive", func(i int) string { return fmt.Sprintf("ERROR %d", i%3) }},
		{"json-logs", func(i int) string {
			return fmt.Sprintf(`{"ts":"2026-05-16T00:00:%02d","level":"INFO","msg":"ok user=%d"}`, i%60, i%10)
		}},
		{"empty-mix", func(i int) string {
			if i%3 == 0 {
				return ""
			}
			return fmt.Sprintf("line %d", i)
		}},
	}

	for _, n := range []int{1, 3, 100, 8192, 8193, 20000} {
		for _, g := range gens {
			t.Run(fmt.Sprintf("%s/%d", g.name, n), func(t *testing.T) {
				events := make([]*event.Event, n)
				base := time.Now()
				for i := 0; i < n; i++ {
					e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), g.gen(i))
					e.Index = "main"
					events[i] = e
				}

				var buf bytes.Buffer
				sw := NewWriter(&buf)
				if _, err := sw.Write(events); err != nil {
					t.Fatalf("write: %v", err)
				}
				sr, err := OpenSegment(buf.Bytes())
				if err != nil {
					t.Fatalf("open: %v", err)
				}

				got := 0
				for rg := 0; rg < sr.RowGroupCount(); rg++ {
					evs, err := sr.ReadRowGroupFiltered(rg, nil, nil, nil)
					if err != nil {
						t.Fatalf("rg %d: %v", rg, err)
					}
					for _, ev := range evs {
						if want := g.gen(got); ev.Raw != want {
							t.Fatalf("rg %d row %d: raw mismatch\n got=%q\nwant=%q", rg, got, ev.Raw, want)
						}
						got++
					}
				}
				if got != n {
					t.Fatalf("read %d events, want %d", got, n)
				}
			})
		}
	}
}
