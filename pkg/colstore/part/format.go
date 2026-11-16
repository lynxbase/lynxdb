package part

import "hash/crc32"

// File layout:
//
//	header   magic, version
//	blocks   each: column chunks, token filter, column directory
//	footer   path names, sort key, block index, first keys, totals
//	trailer  footer offset, footer length, footer checksum, version, magic
const (
	fileMagic     = 0x5450584c // "LXPT"
	formatVersion = 1

	headerSize  = 8
	trailerSize = 28
	// rows, min time, max time, block offset, block length, filter offset,
	// filter length, directory offset, directory length, and the checksum
	// of filter and directory; column chunks carry their own checksums.
	indexEntrySize = 52
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
)

// TimePath names the column holding each row's timestamp in nanoseconds.
const TimePath = "_time"

const maxDictSize = 1 << 16

var castagnoli = crc32.MakeTable(crc32.Castagnoli)
