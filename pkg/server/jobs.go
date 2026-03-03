package server

import (
	"context"
	"time"
)

// GetJob returns a job by ID.
func (e *Engine) GetJob(id string) (*SearchJob, bool) {
	val, ok := e.jobs.Load(id)
	if !ok {
		return nil, false
	}

	return val.(*SearchJob), true
}

// CancelJob cancels a running job. Returns true if the job was found.
func (e *Engine) CancelJob(id string) bool {
	val, ok := e.jobs.Load(id)
	if !ok {
		return false
	}
	job := val.(*SearchJob)
	job.Cancel()

	return true
}

// ListJobs returns info about all active/recent jobs.
func (e *Engine) ListJobs() []JobInfo {
	var jobs []JobInfo
	e.jobs.Range(func(key, value interface{}) bool {
		job := value.(*SearchJob)
		job.mu.Lock()
		jobs = append(jobs, JobInfo{
			ID:        job.ID,
			Query:     job.Query,
			Status:    job.Status,
			CreatedAt: job.CreatedAt,
		})
		job.mu.Unlock()

		return true
	})

	return jobs
}

// startJobGC removes completed jobs older than the configured job TTL.
func (e *Engine) startJobGC(ctx context.Context) {
	gcInterval := e.queryCfg.JobGCInterval
	if gcInterval == 0 {
		gcInterval = 30 * time.Second
	}
	jobTTL := e.queryCfg.JobTTL
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
