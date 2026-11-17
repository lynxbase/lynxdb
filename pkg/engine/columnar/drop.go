package columnar

import "time"

// DropIndexBefore removes from one index the partitions that end at or
// before t and returns the number of events removed; an index that does not
// exist or is not partitioned by time keeps everything.
func (db *DB) DropIndexBefore(index string, t time.Time) (uint64, error) {
	s, err := db.store(index, false)
	if err != nil || s == nil {
		return 0, err
	}

	return s.DropBefore(t.UnixNano())
}
