package database

import (
	"errors"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/ndjson"
)

// AppendEvents adds events to the indexes they name, each written as one
// JSON object; an event without an index goes to DefaultSource.
func (db *DB) AppendEvents(events []*event.Event) error {
	byIndex := map[string][]byte{}
	var order []string
	for _, ev := range events {
		index := ev.Index
		if index == "" {
			index = DefaultSource
		}
		buf, ok := byIndex[index]
		if !ok {
			order = append(order, index)
		}
		byIndex[index] = append(ndjson.AppendEvent(buf, ev), '\n')
	}
	var errs []error
	for _, index := range order {
		if _, err := db.AppendLines(index, byIndex[index]); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
