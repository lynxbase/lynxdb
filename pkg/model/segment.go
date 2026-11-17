package model

import "time"

// SegmentMeta holds metadata about an immutable segment file.
type SegmentMeta struct {
	ID         string    `json:"id" msgpack:"id"`
	Index      string    `json:"index" msgpack:"idx"`
	Partition  string    `json:"partition,omitempty" msgpack:"part,omitempty"`
	MinTime    time.Time `json:"min_time" msgpack:"tmin"`
	MaxTime    time.Time `json:"max_time" msgpack:"tmax"`
	EventCount int64     `json:"event_count" msgpack:"cnt"`
	SizeBytes  int64     `json:"size_bytes" msgpack:"sz"`
	Level      int       `json:"level" msgpack:"lvl"`
	Path       string    `json:"path" msgpack:"path"`
	CreatedAt  time.Time `json:"created_at" msgpack:"cat"`
	Columns    []string  `json:"columns,omitempty" msgpack:"cols,omitempty"`
	Tier       string    `json:"tier,omitempty" msgpack:"tier,omitempty"`
	ObjectKey  string    `json:"object_key,omitempty" msgpack:"okey,omitempty"`
}
