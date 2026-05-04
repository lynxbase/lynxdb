// Package splunkhec implements Splunk HTTP Event Collector-compatible ingest.
package splunkhec

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

type Event struct {
	Time       *float64               `json:"time,omitempty"`
	Event      interface{}            `json:"event"`
	Source     string                 `json:"source,omitempty"`
	SourceType string                 `json:"sourcetype,omitempty"`
	Host       string                 `json:"host,omitempty"`
	Index      string                 `json:"index,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

func (h Event) ToEvent() *event.Event {
	var ts time.Time
	if h.Time != nil {
		sec := int64(*h.Time)
		ts = time.Unix(sec, int64((*h.Time-float64(sec))*1e9))
	}

	raw := rawString(h.Event)
	e := event.NewEvent(ts, raw)
	e.Source = h.Source
	if e.Source == "" {
		e.Source = h.Index
	} else if h.Index != "" {
		e.SetField("source_tag", event.StringValue(h.Index))
	}
	e.SourceType = h.SourceType
	e.Host = h.Host
	e.Index = "main"
	for k, v := range h.Fields {
		e.SetField(k, event.ValueFromInterface(v))
	}
	return e
}

func RawEvent(raw, source, sourceType, host, index string) *event.Event {
	e := event.NewEvent(time.Time{}, raw)
	e.Source = source
	if e.Source == "" {
		e.Source = index
	}
	e.SourceType = sourceType
	e.Host = host
	e.Index = "main"
	return e
}

func rawString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return string(b)
	}
}
