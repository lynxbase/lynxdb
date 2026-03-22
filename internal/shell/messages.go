package shell

import (
	"time"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// querySubmitMsg is sent when the user submits a query from the editor.
type querySubmitMsg struct{ query string }

// queryResultMsg carries the result of an executed query back to the model.
type queryResultMsg struct {
	query   string
	rows    []map[string]interface{}
	elapsed time.Duration
	meta    *client.Meta
	err     error
	hints   string // pre-formatted compat hint text, empty if none
}

// slashCommandMsg carries the output of a slash command.
type slashCommandMsg struct {
	output string
	clear  bool
	quit   bool
}

// fieldsLoadedMsg carries dynamically fetched field names, info, and sources for autocomplete.
type fieldsLoadedMsg struct {
	fields    []string
	fieldInfo []client.FieldInfo
	sources   []string
}

// jobCreatedMsg is sent when an async query job is successfully submitted.
type jobCreatedMsg struct{ jobID string }

// progressMsg carries async job progress for status bar display.
type progressMsg struct {
	phase           string
	segmentsTotal   int
	segmentsScanned int
	segmentsSkipped int
	rowsReadSoFar   int64
}

// pollTickMsg triggers the next poll iteration for an async job.
type pollTickMsg struct{}

// savedQueryRunMsg carries a saved query to be executed.
type savedQueryRunMsg struct {
	name  string
	query string
	err   error
}

// tailStartMsg initiates a live tail SSE stream.
type tailStartMsg struct{ query string }

// tailEventMsg carries a single event from the live tail stream.
type tailEventMsg struct {
	event   map[string]interface{}
	eventCh <-chan map[string]interface{}
	errCh   <-chan error
}

// tailDoneMsg signals that the live tail has ended.
type tailDoneMsg struct{ err error }
