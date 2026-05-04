// Package otlpcommon contains transport-independent OTLP mapping helpers.
package otlpcommon

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func LogsToEvents(resourceLogs []*logspb.ResourceLogs) []*event.Event {
	if len(resourceLogs) == 0 {
		return nil
	}

	var out []*event.Event
	now := time.Now()
	for _, rl := range resourceLogs {
		resourceAttrs := attrsToMap(rl.GetResource())
		source := firstNonEmpty(resourceAttrs["service.name"], resourceAttrs["host.name"], "otlp")
		host := resourceAttrs["host.name"]

		for _, sl := range rl.ScopeLogs {
			scope := sl.GetScope()
			for _, rec := range sl.LogRecords {
				raw := anyValueString(rec.GetBody())
				if raw == "" {
					continue
				}
				ts := time.Unix(0, int64(rec.GetTimeUnixNano()))
				if rec.GetTimeUnixNano() == 0 {
					ts = time.Unix(0, int64(rec.GetObservedTimeUnixNano()))
				}
				if ts.IsZero() {
					ts = now
				}

				ev := event.NewEvent(ts, raw)
				ev.Source = source
				ev.SourceType = "otlp"
				ev.Host = host
				ev.Index = "main"

				for k, v := range resourceAttrs {
					ev.SetField("resource."+k, event.StringValue(v))
				}
				if scope.GetName() != "" {
					ev.SetField("scope.name", event.StringValue(scope.GetName()))
				}
				if scope.GetVersion() != "" {
					ev.SetField("scope.version", event.StringValue(scope.GetVersion()))
				}
				if level := severityText(rec.GetSeverityNumber(), rec.GetSeverityText()); level != "" {
					ev.SetField("level", event.StringValue(level))
				}
				if tid := rec.GetTraceId(); len(tid) > 0 {
					ev.SetField("trace_id", event.StringValue(hex.EncodeToString(tid)))
				}
				if sid := rec.GetSpanId(); len(sid) > 0 {
					ev.SetField("span_id", event.StringValue(hex.EncodeToString(sid)))
				}
				for _, attr := range rec.Attributes {
					ev.SetField(attr.GetKey(), event.StringValue(anyValueString(attr.GetValue())))
				}
				out = append(out, ev)
			}
		}
	}
	return out
}

func CountLogRecords(resourceLogs []*logspb.ResourceLogs) int {
	total := 0
	for _, rl := range resourceLogs {
		for _, sl := range rl.GetScopeLogs() {
			total += len(sl.GetLogRecords())
		}
	}
	return total
}

func attrsToMap(res *resourcepb.Resource) map[string]string {
	out := make(map[string]string)
	if res == nil {
		return out
	}
	for _, attr := range res.Attributes {
		out[attr.GetKey()] = anyValueString(attr.GetValue())
	}
	return out
}

func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		b, _ := json.Marshal(x.ArrayValue)
		return string(b)
	case *commonpb.AnyValue_KvlistValue:
		b, _ := json.Marshal(x.KvlistValue)
		return string(b)
	default:
		return ""
	}
}

func severityText(num logspb.SeverityNumber, text string) string {
	if text != "" {
		return strings.ToLower(text)
	}
	switch {
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_TRACE && num <= logspb.SeverityNumber_SEVERITY_NUMBER_TRACE4:
		return "trace"
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG && num <= logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG4:
		return "debug"
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_INFO && num <= logspb.SeverityNumber_SEVERITY_NUMBER_INFO4:
		return "info"
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_WARN && num <= logspb.SeverityNumber_SEVERITY_NUMBER_WARN4:
		return "warn"
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_ERROR && num <= logspb.SeverityNumber_SEVERITY_NUMBER_ERROR4:
		return "error"
	case num >= logspb.SeverityNumber_SEVERITY_NUMBER_FATAL && num <= logspb.SeverityNumber_SEVERITY_NUMBER_FATAL4:
		return "fatal"
	default:
		return ""
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
