// Package part implements the immutable columnar part: a sequence of
// self-contained blocks followed by a footer that indexes them.
package part

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// ErrCorrupt reports part bytes that do not decode.
var ErrCorrupt = errors.New("part: corrupt data")

// minCompressSize is the payload size below which setting up a decoder
// costs more time than the few bytes compression saves are worth.
const minCompressSize = 512

var (
	codecOnce sync.Once
	encoders  [2]*zstd.Encoder
	decoder   *zstd.Decoder
)

// Level selects the compression effort of a part.
type Level uint8

const (
	// LevelFast favors write speed; it suits freshly flushed parts.
	LevelFast Level = iota
	// LevelCompact favors size; it suits merged parts that live long.
	LevelCompact
)

func codecs() {
	codecOnce.Do(func() {
		// EncodeAll and DecodeAll are stateless and safe for concurrent use.
		encoders[LevelFast], _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderCRC(false))
		encoders[LevelCompact], _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression), zstd.WithEncoderCRC(false))
		decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	})
}

// Codec selects how the bytes of string and variant values are compressed.
type Codec uint8

const (
	// CodecZstd gives the smallest parts.
	CodecZstd Codec = iota
	// CodecLZ4 decodes several times faster and compresses less.
	CodecLZ4
	// CodecNone stores the bytes as they are.
	CodecNone
)

// How the payload of a section is stored, kept in the low bits of its head.
const (
	blobStored = 0
	blobZstd   = 1
	blobLZ4    = 2
)

// blobCodec compresses sections with one codec and reuses its buffers.
type blobCodec struct {
	codec Codec
	level Level
	lz    lz4.Compressor
}

// appendBlob appends payload as a length-prefixed section, compressed through
// scratch when that makes it smaller.
func (bc *blobCodec) appendBlob(dst, payload []byte, scratch *[]byte) []byte {
	if bc.codec != CodecNone && len(payload) >= minCompressSize {
		kind := uint64(blobZstd)
		if bc.codec == CodecLZ4 {
			kind = blobLZ4
			if n := lz4.CompressBlockBound(len(payload)); cap(*scratch) < n {
				*scratch = make([]byte, n)
			}
			// An incompressible payload reports zero bytes written.
			n, err := bc.lz.CompressBlock(payload, (*scratch)[:cap(*scratch)])
			if err != nil {
				n = 0
			}
			*scratch = (*scratch)[:n]
		} else {
			codecs()
			*scratch = encoders[bc.level].EncodeAll(payload, (*scratch)[:0])
		}
		if c := *scratch; len(c) != 0 && len(c)+binary.MaxVarintLen32 < len(payload) {
			dst = binary.AppendUvarint(dst, uint64(len(c))<<2|kind)
			dst = binary.AppendUvarint(dst, uint64(len(payload)))

			return append(dst, c...)
		}
	}
	dst = binary.AppendUvarint(dst, uint64(len(payload))<<2|blobStored)

	return append(dst, payload...)
}

// readBlob returns the payload of the section at the start of src and the
// bytes after it; stored reports a payload that aliases src instead of living
// in buf.
func readBlob(src []byte, buf *[]byte) (payload, rest []byte, stored bool, err error) {
	head, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, nil, false, ErrCorrupt
	}
	src = src[n:]
	size, kind := head>>2, head&3
	if kind == blobStored {
		if size > uint64(len(src)) {
			return nil, nil, false, ErrCorrupt
		}

		return src[:size], src[size:], true, nil
	}
	rawLen, n := binary.Uvarint(src)
	if n <= 0 || size > uint64(len(src)-n) || rawLen > maxSectionSize {
		return nil, nil, false, ErrCorrupt
	}
	src = src[n:]
	if cap(*buf) < int(rawLen) {
		*buf = make([]byte, 0, rawLen)
	}
	var out []byte
	switch kind {
	case blobZstd:
		codecs()
		var derr error
		if out, derr = decoder.DecodeAll(src[:size], (*buf)[:0]); derr != nil {
			return nil, nil, false, ErrCorrupt
		}
	case blobLZ4:
		got, derr := lz4.UncompressBlock(src[:size], (*buf)[:rawLen])
		if derr != nil {
			return nil, nil, false, ErrCorrupt
		}
		out = (*buf)[:got]
	default:
		return nil, nil, false, ErrCorrupt
	}
	if uint64(len(out)) != rawLen {
		return nil, nil, false, ErrCorrupt
	}
	*buf = out

	return out, src[size:], false, nil
}

const maxSectionSize = 1 << 30
