package receiver

import "github.com/lynxbase/lynxdb/pkg/event"

// EventSink is the destination for processed events.
type EventSink interface {
	Write(events []*event.Event) error
}
