// Package part implements the immutable columnar part: a sequence of
// self-contained blocks followed by a footer that indexes them.
package part

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ErrCorrupt reports part bytes that do not decode.
var ErrCorrupt = errors.New("part: corrupt data")

// minCompressSize is the payload size below which compression cannot pay
// for its frame header.
const minCompressSize = 64

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

// appendBlob appends payload as a length-prefixed section, compressed when
// that makes it smaller. scratch is reused for the compressed form.
func appendBlob(dst, payload []byte, level Level, compress bool, scratch *[]byte) []byte {
	if compress && len(payload) >= minCompressSize {
		codecs()
		*scratch = encoders[level].EncodeAll(payload, (*scratch)[:0])
		if c := *scratch; len(c)+binary.MaxVarintLen32 < len(payload) {
			dst = binary.AppendUvarint(dst, uint64(len(c))<<1|1)
			dst = binary.AppendUvarint(dst, uint64(len(payload)))

			return append(dst, c...)
		}
	}
	dst = binary.AppendUvarint(dst, uint64(len(payload))<<1)

	return append(dst, payload...)
}

// readBlob returns the payload of the section at the start of src and the
// bytes after it. A compressed payload is decoded into buf; a stored one
// aliases src, which stored reports.
func readBlob(src []byte, buf *[]byte) (payload, rest []byte, stored bool, err error) {
	head, n := binary.Uvarint(src)
	if n <= 0 {
		return nil, nil, false, ErrCorrupt
	}
	src = src[n:]
	size := head >> 1
	if head&1 == 0 {
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
	codecs()
	if cap(*buf) < int(rawLen) {
		*buf = make([]byte, 0, rawLen)
	}
	out, derr := decoder.DecodeAll(src[:size], (*buf)[:0])
	if derr != nil || uint64(len(out)) != rawLen {
		return nil, nil, false, ErrCorrupt
	}
	*buf = out

	return out, src[size:], false, nil
}

const maxSectionSize = 1 << 30
