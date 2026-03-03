package segment

import "errors"

var (
	ErrCorruptSegment   = errors.New("segment: corrupt data")
	ErrChecksumMismatch = errors.New("segment: checksum mismatch")
	ErrNoEvents         = errors.New("segment: no events to write")
	ErrColumnNotFound   = errors.New("segment: column not found")
	ErrInvalidMagic     = errors.New("segment: invalid magic bytes")
)
