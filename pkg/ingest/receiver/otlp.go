package receiver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// OTLP JSON types (camelCase per OTLP spec).

// OTLPLogsRequest is the top-level envelope for OTLP JSON log export requests.
type OTLPLogsRequest struct {
	ResourceLogs []OTLPResourceLogs `json:"resourceLogs"`
}

// OTLPResourceLogs groups scope logs under a single resource.
type OTLPResourceLogs struct {
	Resource  OTLPResource    `json:"resource"`
	ScopeLogs []OTLPScopeLogs `json:"scopeLogs"`
}

// OTLPResource carries resource-level attributes.
type OTLPResource struct {
	Attributes []OTLPKeyValue `json:"attributes"`
}

// OTLPScopeLogs groups log records under an instrumentation scope.
type OTLPScopeLogs struct {
	Scope      OTLPScope       `json:"scope"`
	LogRecords []OTLPLogRecord `json:"logRecords"`
}

// OTLPScope identifies the instrumentation scope.
type OTLPScope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// OTLPLogRecord is a single OTLP log record.
type OTLPLogRecord struct {
	TimeUnixNano   string         `json:"timeUnixNano"`
	SeverityNumber int            `json:"severityNumber"`
	SeverityText   string         `json:"severityText"`
	Body           OTLPAnyValue   `json:"body"`
	Attributes     []OTLPKeyValue `json:"attributes"`
	TraceID        string         `json:"traceId"`
	SpanID         string         `json:"spanId"`
	Flags          uint32         `json:"flags"`
}

// OTLPKeyValue is a key-value pair with a typed value.
type OTLPKeyValue struct {
	Key   string       `json:"key"`
	Value OTLPAnyValue `json:"value"`
}

// OTLPAnyValue represents the OTLP AnyValue union type.
type OTLPAnyValue struct {
	StringValue *string          `json:"stringValue,omitempty"`
	IntValue    *string          `json:"intValue,omitempty"`
	DoubleValue *float64         `json:"doubleValue,omitempty"`
	BoolValue   *bool            `json:"boolValue,omitempty"`
	BytesValue  *string          `json:"bytesValue,omitempty"`
	ArrayValue  *OTLPArrayValue  `json:"arrayValue,omitempty"`
	KvlistValue *OTLPKvlistValue `json:"kvlistValue,omitempty"`
}

// OTLPArrayValue holds an array of OTLPAnyValue.
type OTLPArrayValue struct {
	Values []OTLPAnyValue `json:"values"`
}

// OTLPKvlistValue holds a list of key-value pairs.
type OTLPKvlistValue struct {
	Values []OTLPKeyValue `json:"values"`
}

// toEventValue converts an OTLPAnyValue to an event.Value.
func toEventValue(v OTLPAnyValue) event.Value {
	switch {
	case v.StringValue != nil:
		return event.StringValue(*v.StringValue)
	case v.IntValue != nil:
		n, err := strconv.ParseInt(*v.IntValue, 10, 64)
		if err != nil {
			return event.StringValue(*v.IntValue)
		}

		return event.IntValue(n)
	case v.DoubleValue != nil:
		return event.FloatValue(*v.DoubleValue)
	case v.BoolValue != nil:
		return event.BoolValue(*v.BoolValue)
	case v.BytesValue != nil:
		return event.StringValue(*v.BytesValue)
	case v.ArrayValue != nil:
		b, err := json.Marshal(v.ArrayValue.Values)
		if err != nil {
			return event.StringValue(fmt.Sprintf("%v", v.ArrayValue.Values))
		}

		return event.StringValue(string(b))
	case v.KvlistValue != nil:
		m := make(map[string]interface{}, len(v.KvlistValue.Values))
		for _, kv := range v.KvlistValue.Values {
			m[kv.Key] = toStringForRaw(kv.Value)
		}
		b, err := json.Marshal(m)
		if err != nil {
			return event.StringValue(fmt.Sprintf("%v", m))
		}

		return event.StringValue(string(b))
	default:
		return event.NullValue()
	}
}

// toStringForRaw converts an OTLPAnyValue to a string suitable for _raw.
func toStringForRaw(v OTLPAnyValue) string {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return *v.IntValue
	case v.DoubleValue != nil:
		return strconv.FormatFloat(*v.DoubleValue, 'g', -1, 64)
	case v.BoolValue != nil:
		if *v.BoolValue {
			return "true"
		}

		return "false"
	case v.BytesValue != nil:
		return *v.BytesValue
	case v.ArrayValue != nil:
		b, err := json.Marshal(v.ArrayValue.Values)
		if err != nil {
			return fmt.Sprintf("%v", v.ArrayValue.Values)
		}

		return string(b)
	case v.KvlistValue != nil:
		m := make(map[string]interface{}, len(v.KvlistValue.Values))
		for _, kv := range v.KvlistValue.Values {
			m[kv.Key] = toStringForRaw(kv.Value)
		}
		b, err := json.Marshal(m)
		if err != nil {
			return fmt.Sprintf("%v", m)
		}

		return string(b)
	default:
		return ""
	}
}

// otlpSeverityText maps an OTLP severity number and text to a canonical level string.
// If text is non-empty, it takes precedence over the number. The result is lowercase.
func otlpSeverityText(num int, text string) string {
	if text != "" {
		return strings.ToLower(text)
	}
	switch {
	case num >= 1 && num <= 4:
		return "trace"
	case num >= 5 && num <= 8:
		return "debug"
	case num >= 9 && num <= 12:
		return "info"
	case num >= 13 && num <= 16:
		return "warn"
	case num >= 17 && num <= 20:
		return "error"
	case num >= 21 && num <= 24:
		return "fatal"
	default:
		return ""
	}
}

// ToEvents converts the OTLP log export request into LynxDB events.
func (r *OTLPLogsRequest) ToEvents() []*event.Event {
	if len(r.ResourceLogs) == 0 {
		return nil
	}

	var events []*event.Event

	for _, rl := range r.ResourceLogs {
		var source, host, index string
		resourceAttrs := make(map[string]event.Value)

		for _, attr := range rl.Resource.Attributes {
			switch attr.Key {
			case "service.name":
				source = toStringForRaw(attr.Value)
			case "host.name":
				host = toStringForRaw(attr.Value)
			case "deployment.environment":
				index = toStringForRaw(attr.Value)
			default:
				resourceAttrs["resource."+attr.Key] = toEventValue(attr.Value)
			}
		}

		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				var ts time.Time
				if lr.TimeUnixNano != "" {
					if nanos, err := strconv.ParseInt(lr.TimeUnixNano, 10, 64); err == nil {
						ts = time.Unix(0, nanos)
					}
				}

				raw := toStringForRaw(lr.Body)

				e := event.NewEvent(ts, raw)
				e.SourceType = "otlp"
				e.Source = source
				e.Host = host
				e.Index = index

				// Apply resource attributes first (lowest priority).
				for k, v := range resourceAttrs {
					e.SetField(k, v)
				}

				// Scope attributes (medium priority).
				if sl.Scope.Name != "" {
					e.SetField("otel.scope.name", event.StringValue(sl.Scope.Name))
				}
				if sl.Scope.Version != "" {
					e.SetField("otel.scope.version", event.StringValue(sl.Scope.Version))
				}

				level := otlpSeverityText(lr.SeverityNumber, lr.SeverityText)
				if level != "" {
					e.SetField("level", event.StringValue(level))
				}
				if lr.SeverityNumber != 0 {
					e.SetField("severity_number", event.IntValue(int64(lr.SeverityNumber)))
				}

				if lr.TraceID != "" {
					e.SetField("trace_id", event.StringValue(lr.TraceID))
				}
				if lr.SpanID != "" {
					e.SetField("span_id", event.StringValue(lr.SpanID))
				}
				if lr.Flags != 0 {
					e.SetField("flags", event.IntValue(int64(lr.Flags)))
				}

				// Log record attributes (highest priority — overwrites resource attrs).
				for _, attr := range lr.Attributes {
					e.SetField(attr.Key, toEventValue(attr.Value))
				}

				events = append(events, e)
			}
		}
	}

	return events
}
