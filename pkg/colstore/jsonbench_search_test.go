package colstore

import (
	"context"
	"sync/atomic"

	"github.com/lynxbase/lynxdb/pkg/colstore/part"
	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
	"github.com/lynxbase/lynxdb/pkg/vec"
)

// searchScan is what the latest search read: the blocks it decoded and the
// blocks its tokens ruled out.
var searchScan struct{ opened, skippedByToken atomic.Int64 }

type searchWorker struct {
	col   vec.Column
	hits  []uint32
	marks []bool
	mask  []uint8
	count int64
}

// countRows scans the blocks that may hold every token and adds up what
// count returns for the string column of path in each.
func countRows(snap *Snapshot, workers int, path string, tokens []uint64, count func(w *searchWorker, c *vec.Column, valid int) int) (int64, error) {
	ws := make([]searchWorker, workers)
	stats, err := snap.Scan(context.Background(), ScanOptions{Workers: workers, Tokens: tokens}, func(n int, p *part.Part, b *part.Block) error {
		w := &ws[n]
		ref, err := column(p, b, path, vec.KindString, &w.col)
		if err != nil || ref == nil {
			return err
		}
		w.count += int64(count(w, &w.col, ref.Count))

		return nil
	})
	if err != nil {
		return 0, err
	}
	searchScan.opened.Store(stats.Blocks)
	searchScan.skippedByToken.Store(stats.SkippedByToken)
	var total int64
	for i := range ws {
		total += ws[i].count
	}

	return total, nil
}

// queryCountPattern counts the rows whose string at path holds the pattern.
func queryCountPattern(snap *Snapshot, workers int, path string, pat tokenfilter.Pattern) (int64, error) {
	return countRows(snap, workers, path, pat.Tokens(nil), func(w *searchWorker, c *vec.Column, valid int) int {
		switch c.Layout {
		case vec.Const:
			if pat.Match(c.DictValue(0)) {
				return valid
			}

			return 0
		case vec.Dict:
			w.hits = pat.AppendMatches(w.hits[:0], c.Offsets[:c.DictLen()+1], c.Data)
			if len(w.hits) == 0 {
				return 0
			}
			if cap(w.marks) < c.DictLen() {
				w.marks = make([]bool, c.DictLen())
			}
			w.marks = w.marks[:c.DictLen()]
			clear(w.marks)
			for _, code := range w.hits {
				w.marks[code] = true
			}
			n := 0
			for i := 0; i < c.Rows; i++ {
				if w.marks[c.Code(i)] && c.IsValid(i) {
					n++
				}
			}

			return n
		}
		w.hits = pat.AppendMatches(w.hits[:0], c.Offsets[:c.Rows+1], c.Data)
		if c.Valid == nil {
			return len(w.hits)
		}
		n := 0
		for _, row := range w.hits {
			if c.Valid.Get(int(row)) {
				n++
			}
		}

		return n
	})
}

// queryCountEquals counts the rows whose string at path is want.
func queryCountEquals(snap *Snapshot, workers int, path, want string) (int64, error) {
	value := []byte(want)

	return countRows(snap, workers, path, tokenfilter.AppendHashes(nil, value), func(w *searchWorker, c *vec.Column, _ int) int {
		if c.Layout == vec.Dict {
			if _, ok := c.DictFind(value); !ok {
				return 0
			}
		}
		if cap(w.mask) < c.Rows {
			w.mask = make([]uint8, c.Rows)
		}
		w.mask = w.mask[:c.Rows]
		vec.MaskEqBytes(w.mask, c, value)

		return vec.MaskCount(w.mask)
	})
}

// A benchSearch is one search of the benchmark; the hit counts of the
// reference systems differ slightly from ours where their token rules do.
type benchSearch struct {
	name string
	run  func(snap *Snapshot, workers int) (int64, error)
}

const (
	textPath = "commit.record.text"
	rareDID  = "did:plc:yj3sjq3blzpynh27cumnp5ks"
	rareCID  = "bafyreicsj6qpl6dreic6ekdjxvdjltncngl52x3jddf6dxhg2gwo35iqhi"
)

var benchSearches = []benchSearch{
	{"S1 rare token", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, "commit.record.subject.uri", tokenfilter.Word("sc7btuzqdaepksx43mr74pma"))
	}},
	{"S2 common token", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Word("love"))
	}},
	{"S3 phrase", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Word("Good morning"))
	}},
	{"S4 prefix", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Prefix("crypto"))
	}},
	{"S5 substring", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Substring("ware"))
	}},
	{"S6 field equals", func(s *Snapshot, n int) (int64, error) { return queryCountEquals(s, n, "did", rareDID) }},
	{"S7 unique id", func(s *Snapshot, n int) (int64, error) { return queryCountEquals(s, n, "commit.cid", rareCID) }},
	// The same text searches without regard to case, which is what the
	// query language's has and contains mean.
	{"S2 any case", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Word("love").IgnoreCase())
	}},
	{"S3 any case", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Word("good morning").IgnoreCase())
	}},
	{"S4 any case", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Prefix("crypto").IgnoreCase())
	}},
	{"S5 any case", func(s *Snapshot, n int) (int64, error) {
		return queryCountPattern(s, n, textPath, tokenfilter.Substring("ware").IgnoreCase())
	}},
}
