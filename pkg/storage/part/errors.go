package part

import "errors"

// ErrTooManyParts is returned when the part count exceeds the reject threshold,
// indicating that compaction is falling behind ingestion. Callers should apply
// backpressure (e.g., HTTP 503 with Retry-After header) to slow ingest.
var ErrTooManyParts = errors.New("part: too many parts, ingest delayed — compaction falling behind")
