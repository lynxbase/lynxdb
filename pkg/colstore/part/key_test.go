package part

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
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

func TestKeyRangeBoundsEveryRowOfAnOrderedPart(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	var docs []string
	var times []int64
	for i := 0; i < 3000; i++ {
		doc := fmt.Sprintf(`{"seq":%d`, i)
		if r.Intn(10) != 0 {
			doc += fmt.Sprintf(`,"svc":"s%02d"`, r.Intn(30))
		}
		if r.Intn(4) != 0 {
			doc += fmt.Sprintf(`,"level":%d`, r.Intn(9))
		}
		docs = append(docs, doc+"}")
		times = append(times, int64(i))
	}
	p := mergeParts(t, []*Part{orderedPart(t, docs, times)}, 256)
	first, last := p.KeyRange()
	if first == nil || last == nil || bytes.Compare(first, last) >= 0 {
		t.Fatalf("key range %x .. %x", first, last)
	}
	if !bytes.Equal(first, p.firstKeys[0]) || bytes.Compare(last, p.firstKeys[len(p.firstKeys)-1]) < 0 {
		t.Fatalf("key range %x .. %x does not span the blocks' first keys", first, last)
	}
	// Everything at or below the last key is the whole part; nothing lies
	// above it.
	above := append(append([]byte(nil), last...), 0)
	if rows := decodeRows(t, mergeRange(t, []*Part{p}, 256, nil, above)); len(rows) != len(docs) {
		t.Fatalf("%d of %d rows are at or below the last key", len(rows), len(docs))
	}
	if rows := decodeRows(t, mergeRange(t, []*Part{p}, 256, above, nil)); len(rows) != 0 {
		t.Fatalf("%d rows lie above the last key", len(rows))
	}
	if rows := decodeRows(t, mergeRange(t, []*Part{p}, 256, last, nil)); len(rows) == 0 {
		t.Fatal("no row holds the last key")
	}

	unordered := buildPart(t, docs, 500, Options{})
	if first, last := unordered.KeyRange(); first != nil || last != nil {
		t.Fatal("an unordered part reported a key range")
	}
}

func TestKeyStatsMatchDecodedKeys(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	key := []string{"a", "b", "c"}
	value := func(path string, values int) string {
		switch rng.Intn(10) {
		case 0:
			return ""
		case 1:
			return fmt.Sprintf(`"%s":%d,`, path, rng.Intn(3))
		case 2:
			return fmt.Sprintf(`"%s":%s,`, path, []string{"true", "false", "null", "1.5"}[rng.Intn(4)])
		}

		return fmt.Sprintf(`"%s":"v%d\u0000%d",`, path, rng.Intn(values), rng.Intn(2))
	}
	all := shred.NewBatch(shred.Options{})
	for i := 0; i < 4000; i++ {
		doc := "{" + value("a", 3) + value("b", 5) + value("c", 30) + fmt.Sprintf(`"n":%d}`, i)
		if err := all.AddJSON([]byte(doc), int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	all.SortBy(key)
	var buf bytes.Buffer
	w := NewWriter(&buf, Options{Ordered: true, SortKey: key})
	block := shred.NewBatch(shred.Options{})
	for from := 0; from < all.Rows; {
		to := min(from+1+rng.Intn(60), all.Rows)
		block.Reset()
		all.CopyRows(block, from, to)
		if err := w.WriteBatch(block); err != nil {
			t.Fatal(err)
		}
		from = to
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	var b Block
	var keys BlockKeys
	var prev, cur []byte
	continued := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		if err := keys.Load(&b); err != nil {
			t.Fatal(err)
		}
		runs := make([]int, len(key)+1)
		for row := 0; row < b.Rows; row++ {
			cur = keys.AppendKey(cur[:0], row)
			shared := SharedKeyColumns(prev, cur)
			if row == 0 {
				if i > 0 && shared != p.KeyShared(i) {
					t.Fatalf("block %d shares %d key columns with the row before it, the index says %d", i, shared, p.KeyShared(i))
				}
				if shared == len(key) {
					continued++
				}
				shared = 0
			}
			for d := shared + 1; d <= len(key); d++ {
				runs[d]++
			}
			if len(KeyPrefix(cur, 3)) != len(cur) || SharedKeyColumns(cur, KeyPrefix(cur, 2)) != 2 || len(KeyPrefix(cur, 0)) != 0 {
				t.Fatalf("key prefixes of %x are wrong", cur)
			}
			prev = append(prev[:0], cur...)
		}
		for d := 0; d <= len(key); d++ {
			if want := max(runs[d], 1); p.KeyRuns(i, d) != want {
				t.Fatalf("block %d has %d distinct prefixes of %d columns, the index says %d", i, want, d, p.KeyRuns(i, d))
			}
		}
	}
	if p.NumBlocks() < 100 || continued < 10 {
		t.Fatalf("%d blocks, %d of them start with the key of the row before", p.NumBlocks(), continued)
	}
}

func TestKeyStringUnescapes(t *testing.T) {
	key := AppendKeyString(AppendKeyInt(AppendKeyString(nil, []byte("a\x00b")), 7), []byte{0, 0, 0xFF})
	if got, ok := KeyString(nil, key, 0); !ok || string(got) != "a\x00b" {
		t.Fatalf("column 0: %q %v", got, ok)
	}
	if _, ok := KeyString(nil, key, 1); ok || KeyKind(key, 1) != vec.KindInt {
		t.Fatal("column 1 is not an integer")
	}
	if got, ok := KeyString(nil, key, 2); !ok || string(got) != "\x00\x00\xff" {
		t.Fatalf("column 2: %q %v", got, ok)
	}
	if KeyKind(key, 3) != 0 || SharedKeyColumns(key, KeyPrefix(key, 2)) != 2 || SharedKeyColumns(key, key) != 3 {
		t.Fatal("key columns are miscounted")
	}
}
