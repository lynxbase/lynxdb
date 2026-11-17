package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// maxPollDuration bounds the total wall-clock a PollJob call will wait before
// giving up. It sits above the server's default MaxQueryRuntime (5m) so a job
// that runs its full budget still returns, while preventing an unbounded poll
// loop when the caller supplies no deadline. Pass a context with an earlier
// deadline (e.g. the CLI --timeout flag) to override it.
const maxPollDuration = 6 * time.Minute

// GetJob returns the status and results of a query job.
func (c *Client) GetJob(ctx context.Context, jobID string) (*JobResult, error) {
	path := "/query/jobs/" + url.PathEscape(jobID)

	var result JobResult
	meta, err := c.doJSON(ctx, http.MethodGet, path, nil, &result)
	if err != nil {
		return nil, err
	}

	result.Meta = meta

	return &result, nil
}

// CancelJob cancels a running query job.
func (c *Client) CancelJob(ctx context.Context, jobID string) error {
	return c.doNoContent(ctx, http.MethodDelete, "/query/jobs/"+url.PathEscape(jobID))
}

// PollJob polls a job until it completes, calling progressFn on each progress update.
// Uses exponential backoff from 100ms to 1s between polls. When the caller's
// context has no deadline, an internal ceiling (maxPollDuration) is applied so
// the loop cannot run forever against a job that never reaches a terminal state.
func (c *Client) PollJob(ctx context.Context, jobID string, progressFn func(*JobProgress)) (*QueryResult, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxPollDuration)
		defer cancel()
	}

	interval := 100 * time.Millisecond
	maxInterval := 1 * time.Second

	for {
		job, err := c.GetJob(ctx, jobID)
		if err != nil {
			return nil, err
		}

		switch job.Status {
		case "complete", "done":
			return c.jobResultToQueryResult(job)
		case "error", "failed":
			msg := "query failed"
			if job.Error != nil {
				msg = job.Error.Message
			}

			return nil, fmt.Errorf("lynxdb: job %s failed: %s", jobID, msg)
		case "canceled":
			return nil, fmt.Errorf("lynxdb: job %s was canceled", jobID)
		}

		if progressFn != nil && job.Progress != nil {
			progressFn(job.Progress)
		}

		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("lynxdb: job %s did not complete before the poll deadline (raise --timeout if the query needs longer)", jobID)
			}

			return nil, ctx.Err()
		case <-time.After(interval):
		}

		interval *= 2
		if interval > maxInterval {
			interval = maxInterval
		}
	}
}

// jobResultToQueryResult converts a completed JobResult into a QueryResult.
func (c *Client) jobResultToQueryResult(job *JobResult) (*QueryResult, error) {
	if job.Results == nil {
		// Job completed but response came as direct data (e.g. from handleGetJob for done jobs).
		// The data was already unmarshaled into job fields — try to reconstruct.
		return nil, fmt.Errorf("lynxdb: job %s has no results field", job.JobID)
	}

	var typed struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(*job.Results, &typed); err != nil {
		return nil, fmt.Errorf("lynxdb: decode job results type: %w", err)
	}

	result := &QueryResult{Meta: job.Meta}

	switch QueryResultType(typed.Type) {
	case ResultTypeEvents, ResultTypeSchema:
		var events EventsResult
		if err := json.Unmarshal(*job.Results, &events); err != nil {
			return nil, fmt.Errorf("lynxdb: decode job events: %w", err)
		}

		result.Type = QueryResultType(typed.Type)
		result.Events = &events
	case ResultTypeAggregate, ResultTypeTimechart:
		var agg AggregateResult
		if err := json.Unmarshal(*job.Results, &agg); err != nil {
			return nil, fmt.Errorf("lynxdb: decode job aggregate: %w", err)
		}

		result.Type = QueryResultType(typed.Type)
		result.Aggregate = &agg
	default:
		return nil, fmt.Errorf("lynxdb: unknown job result type: %q", typed.Type)
	}

	return result, nil
}
