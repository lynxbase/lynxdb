package part

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// ruleColumn is what a rule knows of a string or variant column of a block,
// and what a search of every string column pays to read it.
type ruleColumn struct {
	col     column
	tokens  int
	encoded int
	// raw is the size of the values before compression, and decoded what a
	// search decodes of them: nothing of a constant, nor of a variant
	// column, which it does not read.
	raw     int
	decoded int
}

// ruleBlock holds the columns of a block and, per token, which of them hold
// it, as a bit set of words words.
type ruleBlock struct {
	cols   []ruleColumn
	words  int
	holder map[uint64][]uint64
}

func ruleBlocks(t testing.TB, p *Part) []ruleBlock {
	t.Helper()
	blocks := make([]ruleBlock, p.NumBlocks())
	var b Block
	for i := range blocks {
		rb := &blocks[i]
		if err := p.OpenBlock(i, &b); err != nil {
			t.Fatal(err)
		}
		truth := columnTokens(t, p, i)
		refs := b.Columns()
		for j := range refs {
			ref := &refs[j]
			tokens, ok := truth[column{ref.PathID, ref.Kind}]
			if !ok {
				continue
			}
			rc := ruleColumn{col: column{ref.PathID, ref.Kind}, tokens: len(tokens), encoded: int(ref.length), raw: int(b.dataSize(ref))}
			if ref.Kind == vec.KindString && !ref.Const() {
				rc.decoded = rc.raw
			}
			rb.cols = append(rb.cols, rc)
		}
		rb.words = (len(rb.cols) + 63) / 64
		rb.holder = map[uint64][]uint64{}
		for n, rc := range rb.cols {
			for h := range truth[rc.col] {
				set := rb.holder[h]
				if set == nil {
					set = make([]uint64, rb.words)
					rb.holder[h] = set
				}
				set[n>>6] |= 1 << (n & 63)
			}
		}
	}

	return blocks
}

// filterBytes is the size of a filter of tokens tokens in a group of
// filterStripes stripes, with its share of page checksums.
func filterBytes(tokens int) int {
	blocks := max((tokens*12+255)/256, 1)
	blocks = (blocks + filterStripes - 1) / filterStripes * filterStripes

	return blocks * tokenfilter.BlockBytes * (pageBlocks + 1) / pageBlocks
}

// ruleCost is what giving the columns that own picks a filter of their own
// costs and saves: the bytes of all filters, the filters of their own, and
// per lookup of queries the bytes a search of every string column decodes.
func ruleCost(blocks []ruleBlock, own func(rc *ruleColumn) bool, queries [][]uint64) (size, filters int, decoded []int) {
	decoded = make([]int, len(queries))
	for i := range blocks {
		rb := &blocks[i]
		shared := make([]uint64, rb.words)
		rest := 0
		for n := range rb.cols {
			rc := &rb.cols[n]
			if own(rc) {
				size += filterBytes(rc.tokens)
				filters++

				continue
			}
			shared[n>>6] |= 1 << (n & 63)
			rest += rc.decoded
		}
		tokens := 0
		for _, set := range rb.holder {
			for w, s := range set {
				if s&shared[w] != 0 {
					tokens++

					break
				}
			}
		}
		size += filterBytes(tokens)
		for q, hashes := range queries {
			for _, h := range hashes {
				set := rb.holder[h]
				if set == nil {
					continue
				}
				inShared := false
				for w, s := range set {
					inShared = inShared || s&shared[w] != 0
					for s &^= shared[w]; s != 0; s &= s - 1 {
						decoded[q] += rb.cols[w<<6+bits.TrailingZeros64(s)].decoded
					}
				}
				if inShared {
					decoded[q] += rest
				}
			}
		}
	}

	return size, filters, decoded
}

// TestRealDataColumnFilterRules reports, for the columns of a real file, what
// a search of every string column reads under several rules for giving a
// column a filter of its own, and holds the writer's rule to its bound.
func TestRealDataColumnFilterRules(t *testing.T) {
	data := realData(t)
	const blockRows = 16384
	var buf bytes.Buffer
	buf.Grow(len(data) / 3)
	w := NewWriter(&buf, Options{})
	w.columnBytes = math.MaxInt
	writeNDJSON(t, w, data, blockRows, "time_us")
	p, err := Open(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	blocks := ruleBlocks(t, p)

	type pathStats struct {
		col                               column
		blocks, constant                  int
		tokens, encoded, decoded, minimum int
	}
	stats := map[column]*pathStats{}
	seen := map[uint64]int{}
	for i := range blocks {
		for _, rc := range blocks[i].cols {
			s := stats[rc.col]
			if s == nil {
				s = &pathStats{col: rc.col, minimum: math.MaxInt}
				stats[rc.col] = s
			}
			s.blocks++
			s.tokens, s.encoded, s.decoded, s.minimum = s.tokens+rc.tokens, s.encoded+rc.encoded, s.decoded+rc.decoded, min(s.minimum, rc.tokens)
			if rc.decoded == 0 {
				s.constant++
			}
		}
		for h := range blocks[i].holder {
			seen[h]++
		}
	}
	table := make([]*pathStats, 0, len(stats))
	for _, s := range stats {
		table = append(table, s)
	}
	sort.Slice(table, func(i, j int) bool { return table[i].encoded > table[j].encoded })
	t.Logf("%d blocks of %d rows, %d bytes with one filter a block; per column and block: tokens (least), encoded bytes, bytes a search decodes", len(blocks), blockRows, p.Size())
	for n, s := range table {
		if n == 40 {
			t.Logf("and %d columns more", len(table)-n)

			break
		}
		t.Logf("%-52s %-7s in %2d blocks (%2d not decoded): %6d tokens (%6d), %7d encoded, %8d decoded",
			p.Paths()[s.col.path], s.col.kind, s.blocks, s.constant, s.tokens/s.blocks, s.minimum, s.encoded/s.blocks, s.decoded/s.blocks)
	}

	// The lookups: two words of the benchmark's searches, then tokens drawn
	// from every distinct one, from those that most blocks hold, and from
	// the places where one occurs.
	r := rand.New(rand.NewSource(7))
	var distinct, frequent []uint64
	for h, n := range seen {
		distinct = append(distinct, h)
		if 2*n >= len(blocks) {
			frequent = append(frequent, h)
		}
	}
	sort.Slice(distinct, func(i, j int) bool { return distinct[i] < distinct[j] })
	sort.Slice(frequent, func(i, j int) bool { return frequent[i] < frequent[j] })
	draw := func(from []uint64, n int) []uint64 {
		out := make([]uint64, n)
		for i := range out {
			out[i] = from[r.Intn(len(from))]
		}

		return out
	}
	const lookups = 400
	placed := make([]uint64, 0, lookups)
	for len(placed) < lookups {
		rb := &blocks[r.Intn(len(blocks))]
		skip := r.Intn(len(rb.holder))
		for h := range rb.holder {
			if skip--; skip < 0 {
				placed = append(placed, h)

				break
			}
		}
	}
	names := []string{"love", "rare id", "distinct", "frequent", "placed"}
	queries := [][]uint64{
		tokenfilter.Word("love").Tokens(nil), tokenfilter.Word("sc7btuzqdaepksx43mr74pma").Tokens(nil),
		draw(distinct, lookups), draw(frequent, lookups), placed,
	}
	t.Logf("%d distinct tokens, %d of them in at least half of the blocks", len(distinct), len(frequent))

	never := func(*ruleColumn) bool { return false }
	oneSize, _, oneDecoded := ruleCost(blocks, never, queries)
	_, _, least := ruleCost(blocks, func(*ruleColumn) bool { return true }, queries)
	report := func(name string, own func(rc *ruleColumn) bool) float64 {
		size, filters, decoded := ruleCost(blocks, own, queries)
		growth := float64(size-oneSize) / float64(p.Size())
		line := fmt.Sprintf("%-34s %+6.2f%% %5.1f filters a block; decoded, of what one filter a block lets through:", name, 100*growth, float64(filters)/float64(len(blocks)))
		for q := range queries {
			line += fmt.Sprintf(" %s %5.1f%%", names[q], 100*float64(decoded[q])/float64(max(oneDecoded[q], 1)))
		}
		t.Log(line)

		return growth
	}
	line := "one filter a block decodes, in MB a lookup:"
	for q := range queries {
		line += fmt.Sprintf(" %s %.2f (a filter for every column %.2f)", names[q],
			float64(oneDecoded[q])/float64(len(queries[q]))/1e6, float64(least[q])/float64(len(queries[q]))/1e6)
	}
	t.Log(line)
	report("a filter for every column", func(*ruleColumn) bool { return true })
	for _, tokens := range []int{64, 256, 1024, 2048, 4096, 8192, 16384, 32768} {
		report(fmt.Sprintf("%d tokens", tokens), func(rc *ruleColumn) bool { return rc.tokens >= tokens })
	}
	for _, size := range []int{4 << 10, 8 << 10, 12 << 10, 16 << 10, 20 << 10, 24 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10} {
		report(fmt.Sprintf("%d KiB encoded", size>>10), func(rc *ruleColumn) bool { return rc.encoded >= size })
	}
	for _, size := range []int{8 << 10, 16 << 10, 32 << 10, 48 << 10, 64 << 10, 128 << 10, 256 << 10} {
		report(fmt.Sprintf("%d KiB raw", size>>10), func(rc *ruleColumn) bool { return rc.raw >= size })
	}
	for _, size := range []int{16 << 10, 32 << 10} {
		for _, tokens := range []int{256, 1024} {
			report(fmt.Sprintf("%d KiB encoded and %d tokens", size>>10, tokens), func(rc *ruleColumn) bool { return rc.encoded >= size && rc.tokens >= tokens })
			report(fmt.Sprintf("%d KiB encoded or %d tokens", size>>10, 4*tokens), func(rc *ruleColumn) bool { return rc.encoded >= size || rc.tokens >= 4*tokens })
			report(fmt.Sprintf("%d KiB raw and %d tokens", 2*size>>10, tokens), func(rc *ruleColumn) bool { return rc.raw >= 2*size && rc.tokens >= tokens })
		}
	}
	report("16 KiB encoded, strings alone", func(rc *ruleColumn) bool { return rc.encoded >= 16<<10 && rc.col.kind == vec.KindString })
	for _, ratio := range []int{2, 4, 8, 16} {
		report(fmt.Sprintf("encoded %d times its filter", ratio), func(rc *ruleColumn) bool { return rc.encoded >= ratio*filterBytes(rc.tokens) })
		report(fmt.Sprintf("raw %d times its filter", 4*ratio), func(rc *ruleColumn) bool { return rc.raw >= 4*ratio*filterBytes(rc.tokens) })
	}

	growth := report("the writer", func(rc *ruleColumn) bool { return rc.encoded >= filterColumnBytes })
	if growth > 0.02 {
		t.Fatalf("filters of their own grow the part by %.2f%%", 100*growth)
	}
}
