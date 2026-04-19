package server

import (
	"context"
	"sort"
	"time"
)

type CancelJobResult struct {
	Snapshot JobSnapshot
	Canceled bool
}

// GetJob returns a job by ID.
func (e *Engine) GetJob(id string) (*SearchJob, bool) {
	val, ok := e.jobs.Load(id)
	if !ok {
		return nil, false
	}

	return val.(*SearchJob), true
}

// CancelJob cancels a running job. Returns the current job state if found.
func (e *Engine) CancelJob(id string) (CancelJobResult, bool) {
	val, ok := e.jobs.Load(id)
	if !ok {
		return CancelJobResult{}, false
	}
	job := val.(*SearchJob)
	canceled, snap := job.Cancel()

	return CancelJobResult{Snapshot: snap, Canceled: canceled}, true
}

// ListJobs returns info about all active/recent jobs, optionally filtered by status.
func (e *Engine) ListJobs(status string) []JobInfo {
	var jobs []JobInfo
	e.jobs.Range(func(key, value interface{}) bool {
		job := value.(*SearchJob)
		job.mu.Lock()
		if status != "" && job.Status != status {
			job.mu.Unlock()

			return true
		}
		jobs = append(jobs, JobInfo{
			ID:        job.ID,
			Query:     job.Query,
			Status:    job.Status,
			CreatedAt: job.CreatedAt,
		})
		job.mu.Unlock()

		return true
	})
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].CreatedAt.After(jobs[j].CreatedAt)
	})

	return jobs
}

// startJobGC removes completed jobs older than the configured job TTL.
func (e *Engine) startJobGC(ctx context.Context) {
	cfg := e.queryCfg.Load()
	gcInterval := cfg.JobGCInterval
	if gcInterval == 0 {
		gcInterval = 30 * time.Second
	}
	jobTTL := cfg.JobTTL
	if jobTTL == 0 {
		jobTTL = 5 * time.Minute
	}
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			e.jobs.Range(func(key, value interface{}) bool {
				job := value.(*SearchJob)
				job.mu.Lock()
				isDone := job.Status == JobStatusDone || job.Status == JobStatusError || job.Status == JobStatusCanceled
				doneAt := job.DoneAt
				job.mu.Unlock()
				if isDone && now.Sub(doneAt) > jobTTL {
					e.jobs.Delete(key)
				}

				return true
			})
		}
	}
}
