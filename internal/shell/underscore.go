package shell

import (
	"context"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

// executeUnderscoreQuery executes a pipeline against the last query results
// using an in-memory engine, rather than re-querying the server.
func executeUnderscoreQuery(pipeline string, lastRows []map[string]interface{}) ([]map[string]interface{}, error) {
	engine := storage.NewEphemeralEngine()

	// Convert lastRows to events for the ephemeral engine.
	events := make([]*event.Event, 0, len(lastRows))
	for _, row := range lastRows {
		raw := ""
		if r, ok := row["_raw"]; ok {
			raw = fmt.Sprint(r)
		}
		ev := event.NewEvent(time.Now(), raw)

		for k, v := range row {
			switch k {
			case "_time":
				// Skip — NewEvent sets time.
			case "_raw":
				// Already set.
			case "source", "_source":
				ev.Source = fmt.Sprint(v)
			case "sourcetype", "_sourcetype":
				ev.SourceType = fmt.Sprint(v)
			case "host":
				ev.Host = fmt.Sprint(v)
			case "index":
				ev.Index = fmt.Sprint(v)
			default:
				switch val := v.(type) {
				case string:
					ev.SetField(k, event.StringValue(val))
				case int:
					ev.SetField(k, event.IntValue(int64(val)))
				case int64:
					ev.SetField(k, event.IntValue(val))
				case float64:
					ev.SetField(k, event.FloatValue(val))
				case bool:
					ev.SetField(k, event.BoolValue(val))
				default:
					ev.SetField(k, event.StringValue(fmt.Sprint(val)))
				}
			}
		}
		events = append(events, ev)
	}

	engine.SetEvents("_default", events)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, _, err := engine.Query(ctx, pipeline, storage.QueryOpts{})
	if err != nil {
		return nil, fmt.Errorf("underscore query: %w", err)
	}

	return result.Rows, nil
}
