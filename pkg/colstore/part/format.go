package part

import "hash/crc32"

// File layout:
//
//	header   magic, version
//	blocks   each: column chunks, token filter
//	dirs     the column directory of every block
//	footer   path names, sort key, block index, first keys, totals
//	trailer  footer offset, footer length, footer checksum, version, magic
const (
	fileMagic     = 0x5450584c // "LXPT"
	formatVersion = 5

	headerSize  = 8
	trailerSize = 28
	// rows, min time, max time, block offset, block length, filter offset,
	// filter length, the directory's offset among the directories, its
	// length and its checksum; column chunks and filter pages carry their
	// own checksums.
	indexEntrySize = 52
	// A filter is followed by one checksum per filterPage of its bytes, so
	// a probe verifies the page it reads and not the whole filter.
	filterPage = 4096
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
)

const flagSparse = 1

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
