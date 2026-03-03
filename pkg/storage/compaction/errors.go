package compaction

import "errors"

var (
	ErrNoInputSegments = errors.New("compaction: no input segments")
	ErrEmptyMerge      = errors.New("compaction: no events after merge")
)
