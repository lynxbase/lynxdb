package otlphttp

import (
	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ingest/receiver/otlpcommon"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

func LogsToEvents(resourceLogs []*logspb.ResourceLogs) []*event.Event {
	return otlpcommon.LogsToEvents(resourceLogs)
}

func countLogRecords(resourceLogs []*logspb.ResourceLogs) int {
	return otlpcommon.CountLogRecords(resourceLogs)
}
