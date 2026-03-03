//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"
)

func TestE2E_QueryAsync_ReturnsJobHandle(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jh, err := h.Client().QueryAsync(ctx, `FROM idx_ssh | STATS count AS total`, "", "")
	if err != nil {
		t.Fatalf("QueryAsync: %v", err)
	}
	if jh.JobID == "" {
		t.Fatal("expected non-empty job ID")
	}
	t.Logf("job ID: %s, status: %s", jh.JobID, jh.Status)
}

func TestE2E_QueryAsync_PollJob_ReturnsResult(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jh, err := h.Client().QueryAsync(ctx, `FROM idx_ssh | STATS count AS total`, "", "")
	if err != nil {
		t.Fatalf("QueryAsync: %v", err)
	}

	result, err := h.Client().PollJob(ctx, jh.JobID, nil)
	if err != nil {
		t.Fatalf("PollJob: %v", err)
	}
	requireAggValue(t, result, "total", 2000)
}

func TestE2E_QueryAsync_GetJob_CompletedJob_HasStatus(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jh, err := h.Client().QueryAsync(ctx, `FROM idx_ssh | STATS count AS total`, "", "")
	if err != nil {
		t.Fatalf("QueryAsync: %v", err)
	}

	// Wait for job completion via PollJob, then verify GetJob returns a proper envelope.
	_, err = h.Client().PollJob(ctx, jh.JobID, nil)
	if err != nil {
		t.Fatalf("PollJob: %v", err)
	}

	job, err := h.Client().GetJob(ctx, jh.JobID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if job.Status != "done" {
		t.Errorf("expected status='done', got %q (type=%q)", job.Status, job.Type)
	}
	if job.JobID != jh.JobID {
		t.Errorf("expected job_id=%s, got %q", jh.JobID, job.JobID)
	}
}

func TestE2E_QueryAsync_ListJobs_ContainsSubmittedJob(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	jh, err := h.Client().QueryAsync(ctx, `FROM idx_ssh | STATS count AS total`, "", "")
	if err != nil {
		t.Fatalf("QueryAsync: %v", err)
	}

	list, err := h.Client().ListJobs(ctx)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if list == nil {
		t.Fatal("expected non-nil job list")
	}

	found := false
	for _, j := range list.Jobs {
		if j.JobID == jh.JobID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("submitted job %s not found in ListJobs (total jobs: %d)", jh.JobID, len(list.Jobs))
	}
}

func TestE2E_QueryAsync_CancelJob(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	jh, err := h.Client().QueryAsync(ctx, `FROM idx_ssh | STATS count AS total`, "", "")
	if err != nil {
		t.Fatalf("QueryAsync: %v", err)
	}

	err = h.Client().CancelJob(ctx, jh.JobID)
	if err != nil {
		// Job may have already completed before we could cancel it.
		t.Logf("CancelJob returned error (job may have completed): %v", err)
	} else {
		t.Logf("job %s cancelled successfully", jh.JobID)
	}
}
