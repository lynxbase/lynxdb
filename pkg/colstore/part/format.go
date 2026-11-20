package part

import (
	"hash/crc32"

	"github.com/lynxbase/lynxdb/pkg/colstore/tokenfilter"
)

// File layout:
//
//	header   magic, version
//	blocks   each: column chunks; behind the last block of a filter group,
//	         the group's token filters in stripes
//	dirs     the column directory of every block
//	footer   path names, sort key, block index, filters, filter groups,
//	         first keys, totals, the rows of the values of counted paths
//	trailer  footer offset, footer length, footer checksum, version, magic
//
// A token's probe reads one cell of a filter, and which n-th of the filter
// holds it depends on the token alone when n divides the filter's cells. A
// group cuts each of its filters into as many pieces as it has stripes and
// stores stripe after stripe, stripe k being piece k of every filter in
// block order, so that one token's probes of the whole group read one
// stripe. The filter blocks, the units of the size of a cell, fill pages of
// filterPage bytes, each behind the checksum of its page, and a group of
// filterAligned bytes or more starts on a page of the file. A block has one
// filter or several, each for a column of its own or for all the columns
// that have none, with as many cells as the stripes of the group it was
// written in.
const (
	fileMagic     = 0x5450584c // "LXPT"
	formatVersion = 8

	headerSize  = 8
	trailerSize = 28
	// rows, min time, max time, block offset, block length, filter group,
	// the first of the block's filters among the part's, the directory's
	// offset among the directories, its length and its checksum; column
	// chunks and filter pages carry their own checksums.
	indexEntrySize = 52
	// path id or noPath for the columns without a filter of their own,
	// kind, the power of two of its cells, two zero bytes, filter blocks
	// per stripe, those of the group's earlier filters.
	filterEntrySize = 16
	// offset and length of the group's stripes, and their number.
	groupEntrySize = 20
	// A filter page begins with its checksum, in the room of a filter block,
	// and holds pageBlocks filter blocks; a probe verifies the pages it
	// reads and nothing else.
	filterPage = 4096
	pageBlocks = filterPage/tokenfilter.BlockBytes - 1
	// A group of filterStripes stripes takes blocks until their filters
	// reach filterGroupBytes, the most a writer holds, one of fewer stripes
	// until they reach the same share of it, and any at most
	// filterGroupBlocks. A page fault on a mapped file reads 64 KiB to
	// either side by default, which a stripe is to fit in: filterGroupBytes
	// over filterStripes is 48 KiB.
	filterGroupBlocks = 128
	filterGroupBytes  = 6 << 20
	filterStripes     = 128
	// A block asks for the largest power of two of stripes that is at most
	// one in filterRounding of the filter blocks of its filters, and at
	// most filterStripes: rounding the cells of a filter to whole blocks
	// then adds less than that share of the block's filters.
	filterRounding = 16
	// Padding a smaller group to a page of the file would cost more than
	// the page that a probe of it may read beyond its own.
	filterAligned = 16 * filterPage
	// filterColumnBytes is the encoded size from which a column of a block
	// gets a filter of its own, so that a search can tell which columns to
	// read; the filter of the other columns holds their tokens. Measured on
	// 1m events in blocks of 16384 rows, a search of every string column
	// for a word that most blocks hold decodes 9% of what it does with one
	// filter a block, and the part is 1.8% larger; from 12 KiB on it is
	// 2.1% larger, and from 32 KiB on the search decodes 11%.
	filterColumnBytes = 16 << 10
	// path id, kind, encoding, flags, a zero byte, value count, chunk
	// offset, chunk length, chunk checksum, and the offset of the entry's
	// statistics and constant value among those that follow the entries.
	dirEntrySize = 28
)

// Column encodings.
const (
	encNone  = 0 // presence only
	encConst = 1 // one value, held in the directory
	encFlat  = 2 // one value per present row
	encDict  = 3 // sorted dictionary and one code per present row
	encTime  = 4 // the time of every row in a coarser unit, whose power of ten is the chunk
)

const (
	flagSparse = 1
	// flagCounts marks a dictionary column that holds, after its entries,
	// the number of rows of each.
	flagCounts = 2
)

// maxCounted is the most entries of a dictionary whose rows are counted.
const maxCounted = 256

// Presence modes of a sparse column.
const (
	presenceBitmap = 0 // one bit per row
	presenceList   = 1 // 16-bit row numbers of the present rows
	presenceHoles  = 2 // 16-bit row numbers of the absent rows
)

// Layouts of the codes of a dictionary column.
const (
	codesPacked = 0 // every code in the bits that the largest code needs
	codesRuns   = 1 // the code and the length of every run of equal codes
)

// TimePath names the column holding each row's timestamp in nanoseconds.
const TimePath = "_time"

const maxDictSize = 1 << 16

var castagnoli = crc32.MakeTable(crc32.Castagnoli)
