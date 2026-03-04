package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newJobsCmd())
}

func newJobsCmd() *cobra.Command {
	var (
		status string
		cancel bool
	)

	cmd := &cobra.Command{
		Use:   "jobs [job_id]",
		Short: "List or manage async query jobs",
		Long:  `List running and completed query jobs. Optionally inspect or cancel a specific job.`,
		Example: `  lynxdb jobs                            List all jobs
  lynxdb jobs --status running           Only running jobs
  lynxdb jobs qry_9c1d4e                 Show specific job
  lynxdb jobs qry_9c1d4e --cancel        Cancel a running job`,
		ValidArgsFunction: completeJobIDs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				jobID := args[0]
				if cancel {
					return runJobCancel(jobID)
				}

				return runJobDetail(jobID)
			}

			return runJobsList(status)
		},
	}

	f := cmd.Flags()
	f.StringVar(&status, "status", "", "Filter by status: running, done, error")
	f.BoolVar(&cancel, "cancel", false, "Cancel a running job")

	return cmd
}

func runJobsList(statusFilter string) error {
	ctx := context.Background()

	result, err := apiClient().ListJobsFiltered(ctx, statusFilter)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, j := range result.Jobs {
			b, _ := json.Marshal(j)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(result.Jobs) == 0 {
		fmt.Println("No jobs found.")

		return nil
	}

	// Table output using lipgloss/table.
	tbl := ui.NewTable(ui.Stdout).
		SetColumns("ID", "STATUS", "PROGRESS", "QUERY")

	for _, j := range result.Jobs {
		progress := ""
		if j.Progress != nil {
			progress = fmt.Sprintf("%.0f%%", j.Progress.Percent)
		}

		if j.Status == "done" {
			progress = "100%"
		}

		tbl.AddRow(j.JobID, j.Status, progress, j.Query)
	}

	fmt.Fprintln(os.Stdout, tbl.String())

	return nil
}

func runJobDetail(jobID string) error {
	ctx := context.Background()

	job, err := apiClient().GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(job, "", "  ")
		fmt.Println(string(b))

		return nil
	}

	// Human-readable detail.
	t := ui.Stdout
	fmt.Println()
	fmt.Println(t.KeyValue("Job", job.JobID))
	fmt.Println(t.KeyValue("Status", job.Status))
	fmt.Println(t.KeyValue("Query", job.Query))

	if job.Progress != nil {
		fmt.Println(t.KeyValue("Progress", fmt.Sprintf("%.0f%%", job.Progress.Percent)))

		if job.Progress.TotalEstimate > 0 {
			fmt.Println(t.KeyValue("Segments", fmt.Sprintf("%d / %d scanned", job.Progress.Scanned, job.Progress.TotalEstimate)))
		}
	}

	if job.Progress != nil && job.Progress.ElapsedMS > 0 {
		fmt.Println(t.KeyValue("Elapsed", fmt.Sprintf("%dms", job.Progress.ElapsedMS)))
	}

	if job.Error != nil && job.Error.Message != "" {
		fmt.Println(t.KeyValue("Error", t.Error.Render(job.Error.Message)))
	}

	fmt.Println()

	return nil
}

func runJobCancel(jobID string) error {
	ctx := context.Background()

	if err := apiClient().CancelJob(ctx, jobID); err != nil {
		return err
	}

	printSuccess("Canceled job %s", jobID)

	return nil
}
