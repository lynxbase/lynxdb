package database

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/engine/columnar"
	"github.com/lynxbase/lynxdb/pkg/query"
)

// JobState is the state of a query job.
type JobState string

const (
	JobRunning  JobState = "running"
	JobDone     JobState = "done"
	JobFailed   JobState = "error"
	JobCanceled JobState = "canceled"
)

// Job is a query that runs apart from the request that started it.
type Job struct {
	ID      string
	Query   string
	Started time.Time

	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	state    JobState
	finished time.Time
	result   *columnar.Result
	err      error
}

// JobStatus is what is known of a job at one moment.
type JobStatus struct {
	ID      string
	Query   string
	State   JobState
	Started time.Time
	Elapsed time.Duration
	// Rows is the number of rows of a finished job.
	Rows int
	Err  error
}

// Done is closed when the job has finished.
func (j *Job) Done() <-chan struct{} { return j.done }

// Status returns what is known of the job now.
func (j *Job) Status() JobStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := JobStatus{ID: j.ID, Query: j.Query, State: j.state, Started: j.Started, Err: j.err}
	if j.state == JobRunning {
		st.Elapsed = time.Since(j.Started)
	} else {
		st.Elapsed = j.finished.Sub(j.Started)
	}
	if j.result != nil {
		st.Rows = j.result.Len()
	}

	return st
}

// Result returns the rows of a finished job, or its error.
func (j *Job) Result() (*columnar.Result, error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	return j.result, j.err
}

// Cancel stops the job.
func (j *Job) Cancel() { j.cancel() }

func (j *Job) finish(res *columnar.Result, err error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.finished = time.Now()
	j.result, j.err = res, err
	switch {
	case err == nil:
		j.state = JobDone
	case errors.Is(err, context.Canceled):
		j.state = JobCanceled
	default:
		j.state = JobFailed
	}
}

// Submit starts a query as a job that runs until it finishes, Cancel stops
// it or the database closes.
func (db *DB) Submit(c *query.Compiled, src string, opts QueryOptions) (*Job, error) {
	release, err := db.acquire()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	j := &Job{ID: newJobID(), Query: src, Started: time.Now(), cancel: cancel,
		done: make(chan struct{}), state: JobRunning}
	db.jobs.add(j)
	go func() {
		defer close(j.done)
		defer cancel()
		defer release()
		res, err := db.query(ctx, c, opts)
		j.finish(res, err)
	}()

	return j, nil
}

// Job returns the job with an id.
func (db *DB) Job(id string) (*Job, bool) { return db.jobs.get(id) }

// Jobs lists the jobs, the latest first.
func (db *DB) Jobs() []*Job { return db.jobs.list() }

type jobs struct {
	mu   sync.Mutex
	byID map[string]*Job
}

func (js *jobs) add(j *Job) {
	js.mu.Lock()
	defer js.mu.Unlock()
	js.byID[j.ID] = j
}

func (js *jobs) get(id string) (*Job, bool) {
	js.mu.Lock()
	defer js.mu.Unlock()
	j, ok := js.byID[id]

	return j, ok
}

func (js *jobs) list() []*Job {
	js.mu.Lock()
	out := make([]*Job, 0, len(js.byID))
	for _, j := range js.byID {
		out = append(out, j)
	}
	js.mu.Unlock()
	sort.Slice(out, func(a, b int) bool { return out[a].Started.After(out[b].Started) })

	return out
}

// expire forgets the jobs that finished before t.
func (js *jobs) expire(t time.Time) {
	js.mu.Lock()
	defer js.mu.Unlock()
	for id, j := range js.byID {
		j.mu.Lock()
		old := j.state != JobRunning && j.finished.Before(t)
		j.mu.Unlock()
		if old {
			delete(js.byID, id)
		}
	}
}

func (js *jobs) cancelAll() {
	for _, j := range js.list() {
		j.Cancel()
		<-j.done
	}
}

func newJobID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])

	return "qry_" + hex.EncodeToString(b[:])
}
