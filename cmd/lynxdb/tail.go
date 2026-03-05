package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
)

func init() {
	rootCmd.AddCommand(newTailCmd())
}

func newTailCmd() *cobra.Command {
	var (
		count int
		from  string
	)

	cmd := &cobra.Command{
		Use:     "tail [filter]",
		Aliases: []string{"t"},
		Short:   "Live tail logs from server (SSE stream)",
		Long:    `Stream live log events from the server using Server-Sent Events. Optionally filter with an SPL2 expression.`,
		Example: `  lynxdb tail                                   Stream all events
  lynxdb tail 'level=error'                     Stream errors only
  lynxdb tail '_source=nginx status>=500'        Stream 5xx from nginx
  lynxdb tail --count 50 --from -1h             Last 50 events + live`,
		RunE: func(cmd *cobra.Command, args []string) error {
			filter := ""
			if len(args) > 0 {
				filter = strings.Join(args, " ")
			}

			return runTail(filter, from, count)
		},
	}

	cmd.Flags().IntVarP(&count, "count", "n", 100, "Number of historical events to fetch before live streaming")
	cmd.Flags().StringVar(&from, "from", "-1h", "Historical lookback period")

	return cmd
}

func runTail(filter, from string, count int) error {
	// Pre-validate filter via explain so users get caret display on parse errors.
	if filter != "" {
		ctx := context.Background()
		if _, err := apiClient().Explain(ctx, filter); err != nil {
			if client.IsInvalidQuery(err) {
				return &queryError{inner: err, query: filter}
			}
			// Non-parse errors (e.g. connection) — proceed anyway (best-effort).
		}
	}

	if !globalQuiet {
		if filter != "" {
			printMeta("Tailing events matching: %s", filter)
		} else {
			printMeta("Tailing all events...")
		}
		printHint("Press Ctrl+C to stop.")
	}

	ctx := context.Background()
	start := time.Now()
	eventCount := 0

	err := apiClient().Tail(ctx, filter, from, count, func(evt client.SSEEvent) error {
		if evt.Event == "catchup_done" {
			elapsed := formatElapsed(time.Since(start))
			printMeta("--- historical catchup complete (%d events, %s) — streaming live ---", eventCount, elapsed)

			return nil
		}

		// Default: treat as result event.
		eventCount++

		if isJSONFormat() {
			fmt.Println(string(evt.Data))
		} else {
			printTailEvent(string(evt.Data))
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("stream error: %w", err)
	}

	return nil
}

func printTailEvent(data string) {
	var event map[string]interface{}
	if json.Unmarshal([]byte(data), &event) != nil {
		fmt.Println(data)

		return
	}

	t := ui.Stdout

	ts, _ := event["_time"].(string)
	level, _ := event["level"].(string)
	source, _ := event["source"].(string)
	msg, _ := event["message"].(string)
	if msg == "" {
		msg, _ = event["_raw"].(string)
	}

	var levelStr string

	switch strings.ToUpper(level) {
	case "ERROR":
		levelStr = t.LevelError.Render(level)
	case "WARN", "WARNING":
		levelStr = t.LevelWarn.Render(level)
	case "INFO":
		levelStr = t.LevelInfo.Render(level)
	case "DEBUG":
		levelStr = t.LevelDebug.Render(level)
	default:
		levelStr = level
	}

	fmt.Fprintf(os.Stdout, "%s [%s] %s: %s\n",
		t.Dim.Render(ts),
		levelStr,
		t.Info.Render(source),
		msg)
}
