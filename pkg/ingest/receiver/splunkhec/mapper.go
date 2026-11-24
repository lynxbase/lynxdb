// Package splunkhec implements Splunk HTTP Event Collector-compatible ingest.
package splunkhec

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// EpochSeconds is the time of a HEC event in seconds since the Unix epoch.
// Splunk takes it as a JSON number or as a string that holds one, which is
// what Docker's Splunk logging driver sends.
type EpochSeconds float64

func (t *EpochSeconds) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		// A JSON number begins with a digit or a minus sign; the check
		// keeps "null" from reading as no time at all.
		if s == "" || (s[0] != '-' && (s[0] < '0' || s[0] > '9')) {
			return fmt.Errorf("splunkhec: time %q is not a number of seconds", s)
		}
		b = []byte(s)
	}
	var secs float64
	if err := json.Unmarshal(b, &secs); err != nil {
		return err
	}
	*t = EpochSeconds(secs)

	return nil
}

type Event struct {
	Time       *EpochSeconds          `json:"time,omitempty"`
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
		secs := float64(*h.Time)
		whole := int64(secs)
		ts = time.Unix(whole, int64((secs-float64(whole))*1e9))
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
