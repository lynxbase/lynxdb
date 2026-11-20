package part

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// timedFamily spells the key of a row made at ns, with noise from r.
type timedFamily struct {
	name     string
	alphabet string
	spell    func(r *rand.Rand, ns int64, i int) string
}

const (
	sortable32 = "234567abcdefghijklmnopqrstuvwxyz"
	crockford  = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	sorted64   = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz"
)

var timedFamilies = []timedFamily{
	{"tid", sortable32, func(r *rand.Rand, ns int64, i int) string {
		return timedKey(sortable32, 5, 13, uint64(ns/1e3)<<10|uint64(r.Intn(1024)))
	}},
	{"ulid", crockford, func(r *rand.Rand, ns int64, i int) string {
		return timedKey(crockford, 5, 10, uint64(ns/1e6)) + timedKey(crockford, 5, 16, r.Uint64())
	}},
	{"hex", "0123456789abcdef", func(r *rand.Rand, ns int64, i int) string {
		return fmt.Sprintf("%012x%016x", uint64(ns/1e6)&(1<<48-1), r.Uint64())
	}},
	{"snowflake", "0123456789", func(r *rand.Rand, ns int64, i int) string {
		return strconv.FormatUint(uint64(ns/1e6-1288834974657)<<22|uint64(i&4095), 10)
	}},
	{"padded nanos", "0123456789", func(r *rand.Rand, ns int64, i int) string {
		return fmt.Sprintf("%020d", uint64(ns))
	}},
	{"nanos and noise", "0123456789", func(r *rand.Rand, ns int64, i int) string {
		return fmt.Sprintf("%019d%06d", uint64(ns)%1e19, r.Intn(1e6))
	}},
	{"binary", "01", func(r *rand.Rand, ns int64, i int) string {
		return timedKey("01", 1, 70, uint64(ns/1e3)<<8|uint64(r.Intn(256)))
	}},
	{"quaternary", "acgt", func(r *rand.Rand, ns int64, i int) string {
		return timedKey("acgt", 2, 30, uint64(ns/1e6))
	}},
	{"octal", "01234567", func(r *rand.Rand, ns int64, i int) string {
		return timedKey("01234567", 3, 22, uint64(ns/1e3)<<6|uint64(r.Intn(64)))
	}},
	{"base64", sorted64, func(r *rand.Rand, ns int64, i int) string {
		return timedKey(sorted64, 6, 11, uint64(ns/1e3)<<4|uint64(r.Intn(16)))
	}},
	{"framed", "0123456789abcdef", func(r *rand.Rand, ns int64, i int) string {
		return "req-" + timedKey("0123456789abcdef", 4, 11, uint64(ns/1e6)) + "." + timedKey("0123456789abcdef", 4, 3, uint64(i)) + "-z"
	}},
	{"hex seconds", "0123456789abcdef", func(r *rand.Rand, ns int64, i int) string {
		return fmt.Sprintf("%08x", uint64(ns/1e9)&(1<<32-1))
	}},
	{"hex micros", "0123456789abcdef", func(r *rand.Rand, ns int64, i int) string {
		return fmt.Sprintf("%016x", uint64(ns/1e3))
	}},
	{"short base32", sortable32, func(r *rand.Rand, ns int64, i int) string {
		return timedKey(sortable32, 5, 7, uint64(ns/1e9))
	}},
	{"short quaternary", "acgt", func(r *rand.Rand, ns int64, i int) string {
		return timedKey("acgt", 2, 12, uint64(ns/1e9)>>8)
	}},
	{"uuid7", "0123456789abcdef", func(r *rand.Rand, ns int64, i int) string {
		h := fmt.Sprintf("%012x7%03x%016x", uint64(ns/1e6)&(1<<48-1), r.Intn(4096), r.Uint64()|1<<63)
		return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
	}},
}

// stampFamily spells RFC 3339 stamps of frac digits, in Z or an offset in minutes.
func stampFamily(frac int, offset int, utc bool) timedFamily {
	layout := "2006-01-02T15:04:05"
	if frac > 0 {
		layout += "." + strings.Repeat("0", frac)
	}
	zone := time.UTC
	if utc {
		layout += "Z"
	} else {
		layout += "-07:00"
		zone = time.FixedZone("", offset*60)
	}

	return timedFamily{fmt.Sprintf("stamp frac %d offset %d utc %v", frac, offset, utc), "0123456789", func(r *rand.Rand, ns int64, i int) string {
		return time.Unix(0, ns-r.Int63n(5e9)).In(zone).Format(layout)
	}}
}

// oddValue returns a value near the family's shape that may not fit it.
func oddValue(r *rand.Rand, f timedFamily, ns int64, i int) string {
	v := f.spell(r, ns, i)
	switch r.Intn(12) {
	case 0:
		return v + string(f.alphabet[r.Intn(len(f.alphabet))])
	case 1:
		return v[:len(v)-1]
	case 2:
		b := []byte(v)
		b[r.Intn(len(b))] = "~!#%&*+;<=>?@^`|"[r.Intn(16)]
		return string(b)
	case 3:
		return strings.Repeat(f.alphabet[:1], len(v))
	case 4:
		return strings.Repeat(f.alphabet[len(f.alphabet)-1:], len(v))
	case 5:
		b := []byte(v)
		b[0] = f.alphabet[len(f.alphabet)-1]
		return string(b)
	case 6:
		return f.spell(r, ns-r.Int63n(1<<62), i)
	case 7:
		return f.spell(r, ns+r.Int63n(1<<40)*1e3, i)
	case 8:
		return ""
	case 9:
		return f.spell(r, -ns, i)
	case 10:
		return oddStamp(r)
	}

	return f.spell(r, r.Int63(), i)
}

// oddStamp returns a stamp at an edge of the calendar or of the layouts.
func oddStamp(r *rand.Rand) string {
	odd := []string{
		"2024-02-29T00:00:00Z", "2024-02-29T23:59:59.999999999Z", "2023-02-29T00:00:00Z", "2000-02-29T12:00:00.5Z",
		"1900-02-29T00:00:00Z", "0000-01-01T00:00:00Z", "0001-01-01T00:00:00.000Z", "9999-12-31T23:59:59Z",
		"9999-12-31T23:59:59.999Z", "1677-09-21T00:12:43.145224192Z", "2262-04-11T23:47:16.854775807Z",
		"1969-12-31T23:59:59.999999999Z", "1970-01-01T00:00:00Z", "2024-11-21T24:00:00Z", "2024-11-21T16:25:60Z",
		"2024-11-21T16:25:48.470+14:00", "2024-11-21T16:25:48.470-12:00", "2024-11-21T16:25:48.470+00:00",
		"2024-11-21T16:25:48.470-00:00", "2024-11-21t16:25:48.470z", "2024-11-21 16:25:48.470Z",
		"2024-11-21T16:25:48.1234567890Z", "2024-11-21T16:25:48.Z", "2024-11-21T16:25:48.470+0530",
		"2024-13-01T00:00:00Z", "2024-00-10T00:00:00Z", "2024-04-31T00:00:00Z", "+024-11-21T16:25:48Z",
	}

	return odd[r.Intn(len(odd))]
}

// timedColumn is a string member of the generated rows and how it is made.
type timedColumn struct {
	path    string
	family  timedFamily
	density float64
	odd     float64
	// lag is how far before the row's time its value was made, in ns.
	lag int64
}

// randomTimes returns row times before or after 1970, rising, falling or jumping.
func randomTimes(r *rand.Rand, rows int) []int64 {
	starts := []int64{1732206349000000000, 0, -1e18, 9e18, -9e18, 1 << 40, 1600000000123456789}
	t := starts[r.Intn(len(starts))]
	steps := []int64{0, 1, 1e3, 1e6, 3e8, 1e10, 1e12}
	step := steps[r.Intn(len(steps))]
	times := make([]int64, rows)
	for i := range times {
		switch r.Intn(50) {
		case 0:
			times[i] = 1732206349000000000 + r.Int63n(1e12)
			continue
		case 1:
			t -= r.Int63n(step + 1)
		default:
			t += r.Int63n(step + 1)
		}
		times[i] = t
	}

	return times
}

// timedTrial makes rows whose columns are made near one time, so that they may follow each other.
func timedTrial(r *rand.Rand) (docs []string, times []int64, values []map[string]string, cols []timedColumn) {
	rows := []int{64, 65, 100, 300, 1000, 2500, 5000}[r.Intn(7)]
	times = randomTimes(r, rows)
	all := append([]timedFamily(nil), timedFamilies...)
	for frac := 0; frac <= 9; frac++ {
		all = append(all, stampFamily(frac, 0, true), stampFamily(frac, []int{0, 330, -720, 840}[r.Intn(4)], false))
	}
	// In a chain each column is made a little before the one ahead, so that it follows it.
	chain := r.Intn(3) == 0
	family := all[r.Intn(len(all))]
	for k := 0; k < 2+r.Intn(4); k++ {
		c := timedColumn{
			path:    fmt.Sprintf("c%d", k),
			family:  all[r.Intn(len(all))],
			density: []float64{1, 1, 0.99, 0.9, 0.75, 0.5}[r.Intn(6)],
			odd:     []float64{0, 0, 0.003, 0.02, 0.06, 0.2, 0.3}[r.Intn(7)],
			lag:     []int64{0, 1e6, 1e9, 1e12}[r.Intn(4)],
		}
		if chain {
			c.family, c.lag = family, []int64{1e6, 1e8}[r.Intn(2)]
			c.density = []float64{1, 1, 0.99, 0.9}[r.Intn(4)]
		}
		cols = append(cols, c)
	}
	values = make([]map[string]string, rows)
	for i := range rows {
		values[i] = map[string]string{}
		var sb strings.Builder
		fmt.Fprintf(&sb, `{"n":%d`, i)
		// The time a row's values were made at, which later columns share.
		made := times[i] - r.Int63n(1e9+1)
		at := made
		for _, c := range cols {
			if chain {
				at -= r.Int63n(c.lag + 1)
			} else {
				at = made - r.Int63n(c.lag+1)
			}
			if r.Float64() >= c.density {
				continue
			}
			v := c.family.spell(r, at, i)
			if r.Float64() < c.odd {
				v = oddValue(r, c.family, at, i)
			}
			values[i][c.path] = v
			fmt.Fprintf(&sb, `,%q:%q`, c.path, v)
		}
		docs = append(docs, sb.String()+"}")
	}

	return docs, times, values, cols
}

// timedStats counts the timed chunks that a check met, by what they hold.
type timedStats map[string]int

func (s timedStats) add(b *Block, ref *ColumnRef) {
	if ref.enc != encTimed {
		s["untimed"]++
		return
	}
	s["timed"]++
	chunk, _ := b.timedChunk(ref)
	var v timedValues
	_, ok := readTimedHead(chunk, ref.Count, &v, true)
	if !ok {
		s["unreadable"]++
		return
	}
	s[[]string{"digits", "stamps"}[v.form]]++
	s[[]string{"against nothing", "against the time", "against a column"}[v.refKind]]++
	if len(v.excIndex) > 0 {
		s["with exceptions"]++
	}
	if len(v.farIndex) > 0 {
		s["with far values"]++
	}
	if v.shift > 0 {
		s["with low bits"]++
	}
	if v.form == formDigits && v.from+v.headDigits < v.to {
		s["with tails"]++
	}
	if v.form == formDigits && v.from+v.headDigits == v.to && v.radixBits != 0 && v.headDigits <= 8 {
		s["of one word"]++
	}
	if v.form == formDigits && v.from+v.headDigits == v.to && v.radixBits != 0 && v.headDigits > 8 && v.headDigits <= 16 {
		s["of two words"]++
	}
	if ref.flags&flagSparse != 0 {
		s["sparse"]++
	}
	if v.refKind == refColumn {
		id, _ := b.part.PathID(string(v.reference))
		followed, _ := b.Column(id, vec.KindString)
		if followed.enc != encTimed {
			s["against an untimed column"]++
			return
		}
		chunk, _ := b.timedChunk(followed)
		var f timedValues
		if _, ok := readTimedHead(chunk, followed.Count, &f, false); ok && f.refKind == refColumn {
			s["in a chain"]++
		}
	}
}

// checkTimedColumns reads every column alone and with the others, by row and by value.
func checkTimedColumns(t *testing.T, p *Part, values []map[string]string, cols []timedColumn, stats timedStats) {
	t.Helper()
	first := 0
	var b, all Block
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &all); err != nil {
			t.Fatal(err)
		}
		for _, k := range rand.Perm(len(cols)) {
			c := cols[k]
			id, ok := p.PathID(c.path)
			if !ok {
				continue
			}
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			for _, blk := range []*Block{&b, &all} {
				ref, ok := blk.Column(id, vec.KindString)
				if !ok {
					for row := 0; row < blk.Rows; row++ {
						if v, held := values[first+row][c.path]; held {
							t.Fatalf("block %d has no %s, row %d holds %q", i, c.path, first+row, v)
						}
					}
					continue
				}
				if blk == &b {
					stats.add(blk, ref)
				}
				var byRow, byValue vec.Column
				if err := blk.Decode(ref, &byRow); err != nil {
					t.Fatalf("block %d %s (%s): %v", i, c.path, c.family.name, err)
				}
				if err := blk.DecodeValues(ref, &byValue); err != nil {
					t.Fatalf("block %d %s (%s) by value: %v", i, c.path, c.family.name, err)
				}
				if size := blk.dataSize(ref); size != 0 && int(size) != len(byValue.Data) {
					t.Fatalf("block %d %s: %d bytes, the chunk says %d", i, c.path, len(byValue.Data), size)
				}
				value := 0
				for row := 0; row < blk.Rows; row++ {
					want, held := values[first+row][c.path]
					if byRow.IsValid(row) != held {
						t.Fatalf("block %d %s row %d: present %v, want %v", i, c.path, row, byRow.IsValid(row), held)
					}
					if !held {
						continue
					}
					if got := byRow.Bytes(row); string(got) != want {
						t.Fatalf("block %d %s (%s, enc %d) row %d: %q, want %q", i, c.path, c.family.name, ref.enc, row, got, want)
					}
					if got := byValue.Bytes(value); string(got) != want {
						t.Fatalf("block %d %s (%s) value %d: %q, want %q", i, c.path, c.family.name, value, got, want)
					}
					value++
				}
			}
		}
		first += all.Rows
	}
}

func TestTimedColumnsRoundTripAtTheEdgesOfTheirShapes(t *testing.T) {
	r := rand.New(rand.NewSource(20261120))
	stats := timedStats{}
	trials := 400
	if testing.Short() {
		trials = 60
	}
	for trial := 0; trial < trials; trial++ {
		docs, times, values, cols := timedTrial(r)
		blockRows := []int{64, 100, 256, 1000, 4096}[r.Intn(5)]
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{})
		batch := shred.NewBatch(shred.Options{})
		for from := 0; from < len(docs); from += blockRows {
			batch.Reset()
			for i := from; i < min(from+blockRows, len(docs)); i++ {
				if err := batch.AddJSON([]byte(docs[i]), times[i]); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.WriteBatch(batch); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		p, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer func() {
				if t.Failed() {
					for _, c := range cols {
						t.Logf("trial %d %s: %s density %.2f odd %.3f lag %d", trial, c.path, c.family.name, c.density, c.odd, c.lag)
					}
				}
			}()
			checkTimedColumns(t, p, values, cols, stats)
		}()
	}
	if testing.Short() {
		return
	}
	for _, kind := range []string{"timed", "stamps", "digits", "against nothing", "against the time", "against a column",
		"with exceptions", "with far values", "with low bits", "with tails", "sparse", "in a chain", "against an untimed column", "of one word", "of two words"} {
		if stats[kind] == 0 {
			t.Errorf("no chunk %s was written", kind)
		}
	}
}

// checkTimedByRow is checkTimedColumns for rows that name their place in member n.
func checkTimedByRow(t *testing.T, p *Part, values []map[string]string, cols []timedColumn) int {
	t.Helper()
	nid, _ := p.PathID("n")
	var b Block
	var n, col vec.Column
	seen := 0
	for i := 0; i < p.NumBlocks(); i++ {
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		ref, ok := b.Column(nid, vec.KindInt)
		if !ok {
			t.Fatalf("block %d has no n", i)
		}
		if err := b.Decode(ref, &n); err != nil {
			t.Fatal(err)
		}
		seen += b.Rows
		for _, c := range cols {
			id, ok := p.PathID(c.path)
			if !ok {
				continue
			}
			// A block opened afresh, so that the column is read alone.
			if err := p.OpenBlock(i, &b); err != nil {
				t.Fatal(err)
			}
			ref, ok := b.Column(id, vec.KindString)
			if !ok {
				col = vec.Column{Rows: b.Rows, Valid: make(vec.Bitmap, vec.BitmapWords(b.Rows))}
			} else if err := b.Decode(ref, &col); err != nil {
				t.Fatalf("block %d %s: %v", i, c.path, err)
			}
			for row := 0; row < b.Rows; row++ {
				want, held := values[n.Int(row)][c.path]
				if col.IsValid(row) != held || held && string(col.Bytes(row)) != want {
					t.Fatalf("block %d %s row %d (n %d): %q present %v, want %q present %v",
						i, c.path, row, n.Int(row), col.Bytes(row), col.IsValid(row), want, held)
				}
			}
		}
	}

	return seen
}

// A merge carries some blocks as they are stored, under other path ids.
func TestTimedColumnsSurviveMerges(t *testing.T) {
	r := rand.New(rand.NewSource(20261121))
	key := []string{"g"}
	carried := 0
	for trial := 0; trial < 40; trial++ {
		docs, times, values, cols := timedTrial(r)
		blockRows := []int{64, 100, 256, 1000}[r.Intn(4)]
		groups := 1 + r.Intn(4)
		var srcs []*Part
		per := (len(docs) + 2) / 3
		for from := 0; from < len(docs); from += per {
			var buf bytes.Buffer
			w := NewWriter(&buf, Options{Ordered: true, SortKey: key})
			all := shred.NewBatch(shred.Options{})
			// The parts hold groups of their own or groups they share.
			base := 0
			if r.Intn(2) == 0 {
				base = from
			}
			for i := from; i < min(from+per, len(docs)); i++ {
				doc := fmt.Sprintf(`{"g":"g%04d",`, base+r.Intn(groups)) + docs[i][1:]
				if err := all.AddJSON([]byte(doc), times[i]); err != nil {
					t.Fatal(err)
				}
			}
			all.SortBy(key)
			block := shred.NewBatch(shred.Options{})
			for row := 0; row < all.Rows; row += blockRows {
				block.Reset()
				all.CopyRows(block, row, min(row+blockRows, all.Rows))
				if err := w.WriteBatch(block); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			p, err := Open(buf.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			srcs = append(srcs, p)
		}
		var buf bytes.Buffer
		w := NewWriter(&buf, Options{Ordered: true, SortKey: key})
		if err := Merge(w, srcs, []int{100, 1000, 4096}[r.Intn(3)]); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		merged, err := Open(buf.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		if seen := checkTimedByRow(t, merged, values, cols); seen != len(docs) {
			t.Fatalf("%d rows merged, want %d", seen, len(docs))
		}
		stored := map[string]bool{}
		for _, src := range srcs {
			for i := 0; i < src.NumBlocks(); i++ {
				stored[blockData(src, i)] = true
			}
		}
		for i := 0; i < merged.NumBlocks(); i++ {
			if stored[blockData(merged, i)] {
				carried++
			}
		}
	}
	if carried == 0 {
		t.Fatal("the merges carried no block as it was stored")
	}
}

// blockData returns the column chunks of block i of p as they are stored.
func blockData(p *Part, i int) string {
	e := p.index[i*indexEntrySize:]
	start := binary.LittleEndian.Uint64(e[20:])

	return string(p.blocks[start : start+uint64(binary.LittleEndian.Uint32(e[28:]))])
}
