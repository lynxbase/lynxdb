package part

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

func TestBlocksWithKeyPrefixCoversEveryMatchingRow(t *testing.T) {
	r := rand.New(rand.NewSource(21))
	services := []string{"api", "api\x00gw", "billing", "ingest", "zeta"}
	var srcs []*Part
	for src := 0; src < 3; src++ {
		var docs []string
		var times []int64
		for seq := 0; seq < 3000; seq++ {
			doc := fmt.Sprintf(`{"src":%d,"seq":%d`, src, seq)
			if r.Intn(10) != 0 {
				svc, _ := json.Marshal(services[r.Intn(len(services))])
				doc += `,"svc":` + string(svc)
			}
			if r.Intn(4) != 0 {
				doc += fmt.Sprintf(`,"level":%d`, r.Intn(7)-3)
			}
			docs = append(docs, doc+"}")
			times = append(times, int64(seq))
		}
		srcs = append(srcs, orderedPart(t, docs, times))
	}
	p := mergeParts(t, srcs, 128)
	rows := decodeRows(t, p)
	blockOf := make([]int, 0, len(rows))
	for b := 0; b < p.NumBlocks(); b++ {
		n, _, _ := p.BlockStats(b)
		for i := 0; i < n; i++ {
			blockOf = append(blockOf, b)
		}
	}

	check := func(name string, prefix []byte, match func(mergedRow) bool) {
		t.Helper()
		first, end := p.BlocksWithKeyPrefix(prefix)
		lo, hi, hits := p.NumBlocks(), -1, 0
		for i, row := range rows {
			if match(row) {
				hits++
				lo, hi = min(lo, blockOf[i]), max(hi, blockOf[i])
			}
		}
		if hits == 0 {
			if end-first > 1 {
				t.Errorf("%s: no rows match but blocks %d..%d are kept", name, first, end)
			}

			return
		}
		if first > lo || end <= hi {
			t.Errorf("%s: rows live in blocks %d..%d, range is %d..%d", name, lo, hi, first, end)
		}
		if lo-first > 1 || end-hi > 1 {
			t.Errorf("%s: range %d..%d is loose around blocks %d..%d", name, first, end, lo, hi)
		}
	}
	for _, svc := range append(services, "absent", "a", "zzzz") {
		check("svc="+svc, AppendKeyString(nil, []byte(svc)), func(row mergedRow) bool { return row.Svc != nil && *row.Svc == svc })
		for level := int64(-4); level <= 4; level++ {
			check(fmt.Sprintf("svc=%s level=%d", svc, level), AppendKeyInt(AppendKeyString(nil, []byte(svc)), level),
				func(row mergedRow) bool {
					return row.Svc != nil && *row.Svc == svc && row.Level != nil && *row.Level == level
				})
		}
	}
	check("no svc", []byte{0}, func(row mergedRow) bool { return row.Svc == nil })
	check("everything", nil, func(mergedRow) bool { return true })

	unordered := buildPart(t, []string{`{"svc":"api"}`, `{"svc":"zeta"}`}, 1, Options{})
	if first, end := unordered.BlocksWithKeyPrefix(AppendKeyString(nil, []byte("zeta"))); first != 0 || end != 2 {
		t.Errorf("unordered part pruned to %d..%d", first, end)
	}
}
