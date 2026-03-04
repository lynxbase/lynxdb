package main

import (
	"context"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// completionClient returns a short-timeout client for tab completion.
// Separate from apiClient() because completions need fast failure.
func completionClient() *client.Client {
	return client.NewClient(
		client.WithBaseURL(globalServer),
		client.WithAuthToken(resolveToken()),
		client.WithTimeout(2*time.Second),
	)
}

// completeSavedQueryNames returns a ValidArgsFunction that fetches saved query
// names from the server for tab completion. Falls back silently on error.
func completeSavedQueryNames(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	c := completionClient()

	queries, err := c.ListSavedQueries(context.Background())
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, 0, len(queries))
	for _, q := range queries {
		if q.Name != "" {
			names = append(names, q.Name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}

// completeMVNames returns a ValidArgsFunction that fetches materialized view
// names from the server for tab completion.
func completeMVNames(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	c := completionClient()

	views, err := c.ListViews(context.Background())
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	names := make([]string, 0, len(views))
	for _, v := range views {
		if v.Name != "" {
			names = append(names, v.Name)
		}
	}

	return names, cobra.ShellCompDirectiveNoFileComp
}

// completeJobIDs returns a ValidArgsFunction that fetches job IDs from the
// server for tab completion.
func completeJobIDs(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	c := completionClient()

	result, err := c.ListJobs(context.Background())
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}

	ids := make([]string, 0, len(result.Jobs))
	for _, j := range result.Jobs {
		if j.JobID != "" {
			ids = append(ids, j.JobID)
		}
	}

	return ids, cobra.ShellCompDirectiveNoFileComp
}

// completeFieldNames returns a ValidArgsFunction that fetches field names
// from the server for tab completion.
func completeFieldNames(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
	return fetchFieldNames(), cobra.ShellCompDirectiveNoFileComp
}

// fetchFieldNames fetches field names from the server for completion and
// caching. Returns nil on any error (best-effort).
func fetchFieldNames() []string {
	c := completionClient()

	fields, err := c.Fields(context.Background())
	if err != nil {
		return nil
	}

	names := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.Name != "" {
			names = append(names, f.Name)
		}
	}

	return names
}
