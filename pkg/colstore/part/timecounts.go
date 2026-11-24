package part

import (
	"encoding/binary"
	"math"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// Every block records how many of its rows fall in each bucket of time, so
// that a count of rows by a function of their time bucket needs no row
// times. Buckets are UTC and start at multiples of their width since the
// epoch. A block holds a record for every width whose record fits in
// maxTimeCountBytes, so that a count by hours reads 24 buckets of a day
// and not 1440, and none when no width fits.
//
// The section lists, block after block, the length of the block's records
// and the records, the widest first, each behind its length: the width's
// code, the number of buckets that hold rows, the first of them, and for
// each its distance from the one before and its rows.

// timeWidths holds the bucket widths in nanoseconds by their codes, the
// narrowest first.
var timeWidths = [...]int64{0, 60e9, 3600e9, 86400e9}

const (
	// maxTimeCountBytes bounds a record. It holds the 1440 minutes of a day
	// that a block in key order has rows in, at about two bytes a minute.
	maxTimeCountBytes = 4096
	// maxTimeBuckets bounds the buckets that a block's span may cover at a
	// width that it is counted in.
	maxTimeBuckets = 1 << 16
)

// TimeCount is the number of rows of a block in one bucket of time.
type TimeCount struct {
	Bucket int64 // the bucket's start divided by its width
	Rows   uint32
}

// bucketScratch is what appendTimeCounts reuses: the rows by bucket of
// the narrowest width, and the buckets that hold rows at every width.
type bucketScratch struct {
	rows    []uint32
	buckets []TimeCount
}

// appendTimeCounts appends the records of a block whose rows have times,
// from minT to maxT, to dst. The rows are counted once, in the narrowest
// buckets whose span fits, and the wider buckets add those up.
func appendTimeCounts(dst []byte, times []int64, minT, maxT int64, s *bucketScratch) []byte {
	if len(times) == 0 {
		return dst
	}
	code := 1
	for ; code < len(timeWidths); code++ {
		width := timeWidths[code]
		lo, hi := floorDiv(minT, width), floorDiv(maxT, width)
		// Every bucket must start and end within the range of times.
		if hi-lo < maxTimeBuckets && lo >= math.MinInt64/width && hi < math.MaxInt64/width {
			break
		}
	}
	if code == len(timeWidths) {
		return dst
	}
	width := timeWidths[code]
	lo := floorDiv(minT, width)
	span := int(floorDiv(maxT, width)-lo) + 1
	if cap(s.rows) < span {
		s.rows = make([]uint32, span)
	}
	rows := s.rows[:span]
	clear(rows)
	countBuckets(rows, times, lo*width, code)

	// at[c] is where the buckets of width code c start in s.buckets.
	var at [len(timeWidths) + 1]int
	b := s.buckets[:0]
	for k, n := range rows {
		if n != 0 {
			b = append(b, TimeCount{Bucket: lo + int64(k), Rows: n})
		}
	}
	for c := code + 1; c < len(timeWidths); c++ {
		at[c] = len(b)
		ratio := timeWidths[c] / timeWidths[c-1]
		for _, n := range b[at[c-1]:at[c]] {
			bucket := floorDiv(n.Bucket, ratio)
			if last := len(b) - 1; last >= at[c] && b[last].Bucket == bucket {
				b[last].Rows += n.Rows

				continue
			}
			b = append(b, TimeCount{Bucket: bucket, Rows: n.Rows})
		}
	}
	at[len(timeWidths)] = len(b)
	s.buckets = b

	var room [binary.MaxVarintLen64]byte
	for c := len(timeWidths) - 1; c >= code; c-- {
		buckets := b[at[c]:at[c+1]]
		if w := timeWidths[c]; buckets[0].Bucket < math.MinInt64/w || buckets[len(buckets)-1].Bucket >= math.MaxInt64/w {
			continue
		}
		// The record is built behind room for its length.
		start := len(dst) + len(room)
		record := append(append(dst, room[:]...), byte(c))
		record = binary.AppendUvarint(record, uint64(len(buckets)))
		record = binary.AppendUvarint(record, zigzag(buckets[0].Bucket))
		prev := buckets[0].Bucket
		for _, n := range buckets {
			record = binary.AppendUvarint(record, uint64(n.Bucket-prev))
			record = binary.AppendUvarint(record, uint64(n.Rows))
			if prev = n.Bucket; len(record)-start > maxTimeCountBytes {
				break
			}
		}
		if size := len(record) - start; size <= maxTimeCountBytes {
			head := binary.AppendUvarint(room[:0], uint64(size))
			dst = append(append(dst, head...), record[start:]...)
		}
	}

	return dst
}

// countBuckets adds every time to rows at its bucket of width code, the
// first of which starts at base, no later than any time.
func countBuckets(rows []uint32, times []int64, base int64, code int) {
	// Widths known to the compiler divide by multiplying.
	switch code {
	case 1:
		for _, t := range times {
			rows[uint64(t-base)/60e9]++
		}
	case 2:
		for _, t := range times {
			rows[uint64(t-base)/3600e9]++
		}
	default:
		for _, t := range times {
			rows[uint64(t-base)/86400e9]++
		}
	}
}

// addTimeCounts appends to the part's section the records of a block whose
// rows have times, from minT to maxT.
func (w *Writer) addTimeCounts(times []int64, minT, maxT int64) {
	w.timeRecord = appendTimeCounts(w.timeRecord[:0], times, minT, maxT, &w.timeBuckets)
	w.timeCounts = binary.AppendUvarint(w.timeCounts, uint64(len(w.timeRecord)))
	w.timeCounts = append(w.timeCounts, w.timeRecord...)
}

// copyTimeCounts appends to the part's section the records of block i of
// src, whose rows the block being copied holds.
func (w *Writer) copyTimeCounts(src *Part, i int) {
	records := src.timeCounts[src.timeCountAt[2*i]:src.timeCountAt[2*i+1]]
	w.timeCounts = binary.AppendUvarint(w.timeCounts, uint64(len(records)))
	w.timeCounts = append(w.timeCounts, records...)
}

// readTimeCounts reads the section of the records of the part's blocks and
// checks that every record counts the rows of its block, from the bucket of
// its first time to that of its last.
func (p *Part) readTimeCounts(r *reader) bool {
	section := r.bytes(r.uvarint())
	blocks := p.NumBlocks()
	p.timeCounts, p.timeCountAt = section, make([]uint32, 0, 2*blocks)
	at := 0
	var counts []TimeCount
	for i := 0; i < blocks; i++ {
		n, used := binary.Uvarint(section[at:])
		if used <= 0 || n > uint64(len(section)-at-used) {
			return false
		}
		at += used
		p.timeCountAt = append(p.timeCountAt, uint32(at), uint32(at)+uint32(n))
		at += int(n)
		rows, minT, maxT := p.BlockStats(i)
		records := section[at-int(n) : at]
		for len(records) > 0 {
			record, rest, ok := nextTimeRecord(records)
			if !ok {
				return false
			}
			records = rest
			var width int64
			if counts, width, ok = parseTimeRecord(record, counts[:0]); !ok || width == 0 ||
				counts[0].Bucket != floorDiv(minT, width) || counts[len(counts)-1].Bucket != floorDiv(maxT, width) {
				return false
			}
			total := uint64(0)
			for _, c := range counts {
				total += uint64(c.Rows)
			}
			if total != uint64(rows) {
				return false
			}
		}
	}

	return !r.bad && at == len(section)
}

// TimeCounts appends to dst the rows of block i by bucket of time, in the
// order of the buckets, in the widest buckets that divide granule
// nanoseconds, and returns them with the width of the buckets; a block
// without such buckets returns a width of zero.
func (p *Part) TimeCounts(i int, granule int64, dst []TimeCount) ([]TimeCount, int64) {
	if i < 0 || 2*i+1 >= len(p.timeCountAt) || granule <= 0 {
		return dst, 0
	}
	records := p.timeCounts[p.timeCountAt[2*i]:p.timeCountAt[2*i+1]]
	for len(records) > 0 {
		record, rest, ok := nextTimeRecord(records)
		if !ok {
			return dst, 0
		}
		if code := record[0]; int(code) < len(timeWidths) && code > 0 && granule%timeWidths[code] == 0 {
			if counts, width, ok := parseTimeRecord(record, dst); ok {
				return counts, width
			}

			return dst, 0
		}
		records = rest
	}

	return dst, 0
}

// NarrowTimeCounts appends to dst the rows of block i by bucket of time, in
// the order of the buckets, in the narrowest buckets the block counts its
// rows in, and returns them with the width of the buckets; a block without
// a record returns a width of zero.
func (p *Part) NarrowTimeCounts(i int, dst []TimeCount) ([]TimeCount, int64) {
	record := p.narrowTimeRecord(i)
	if record == nil {
		return dst, 0
	}
	if counts, width, ok := parseTimeRecord(record, dst); ok {
		return counts, width
	}

	return dst, 0
}

// narrowTimeRecord returns the record of block i in its narrowest buckets,
// the last of the block's records, or nil when it has none.
func (p *Part) narrowTimeRecord(i int) []byte {
	if i < 0 || 2*i+1 >= len(p.timeCountAt) {
		return nil
	}
	records := p.timeCounts[p.timeCountAt[2*i]:p.timeCountAt[2*i+1]]
	var last []byte
	for len(records) > 0 {
		record, rest, ok := nextTimeRecord(records)
		if !ok {
			return nil
		}
		last, records = record, rest
	}

	return last
}

// NoneWithin reports whether the records of block i show that none of its
// rows has a time from lo to hi, both included: no bucket that the range
// meets holds a row. It is false for a block without a record.
func (p *Part) NoneWithin(i int, lo, hi int64) bool {
	record := p.narrowTimeRecord(i)
	if record == nil || int(record[0]) >= len(timeWidths) || record[0] == 0 {
		return false
	}
	width := timeWidths[record[0]]
	r := reader{buf: record[1:]}
	held := r.uvarint()
	bucket := unzigzag(r.uvarint())
	// Buckets come in order: the first that ends at lo or later decides.
	first, last := floorDiv(lo, width), floorDiv(hi, width)
	for ; held > 0; held-- {
		bucket += int64(r.uvarint())
		r.uvarint()
		if r.bad {
			return false
		}
		if bucket >= first {
			return bucket > last
		}
	}

	return true
}

// nextTimeRecord splits the record at the start of records from those that
// follow it.
func nextTimeRecord(records []byte) (record, rest []byte, ok bool) {
	n, used := binary.Uvarint(records)
	if used <= 0 || n == 0 || n > uint64(len(records)-used) {
		return nil, nil, false
	}

	return records[used : used+int(n)], records[used+int(n):], true
}

// parseTimeRecord appends the buckets of a record to dst and returns them
// with their width.
func parseTimeRecord(record []byte, dst []TimeCount) ([]TimeCount, int64, bool) {
	code := record[0]
	if int(code) >= len(timeWidths) || code == 0 {
		return dst, 0, false
	}
	r := reader{buf: record[1:]}
	held := r.uvarint()
	bucket := unzigzag(r.uvarint())
	if held == 0 || held > uint64(len(r.buf)) {
		return dst, 0, false
	}
	start := len(dst)
	for ; held > 0 && !r.bad; held-- {
		step := r.uvarint()
		bucket += int64(step)
		n := r.uvarint()
		if n == 0 || n > shred.MaxRows || (step == 0) != (len(dst) == start) {
			return dst[:start], 0, false
		}
		dst = append(dst, TimeCount{Bucket: bucket, Rows: uint32(n)})
	}
	if r.bad || len(r.buf) != 0 {
		return dst[:start], 0, false
	}

	return dst, timeWidths[code], true
}
