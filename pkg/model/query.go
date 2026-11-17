// Package model provides shared data types used across layers.
package model

import (
	"time"
)

// TimeBounds represents a concrete time range for storage queries.
type TimeBounds struct {
	Earliest time.Time
	Latest   time.Time
}

// SourceScope constants describe how a query addresses sources.
const (
	SourceScopeAll    = "all"
	SourceScopeSingle = "single"
	SourceScopeList   = "list"
	SourceScopeGlob   = "glob"
)
