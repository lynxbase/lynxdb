package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/sigmaqueries"
	"github.com/lynxbase/lynxdb/pkg/spl2"
)

func init() {
	rootCmd.AddCommand(newSavedCmd())
	rootCmd.AddCommand(newSaveCmd())
	rootCmd.AddCommand(newRunCmd())
}

func newSavedCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "saved",
		Short: "Manage saved queries",
		Long:  `List, create, run, and delete saved queries.`,
		Example: `  lynxdb saved                                  List all saved queries
  lynxdb saved create "5xx-rate" 'status>=500 | stats count by uri'
  lynxdb saved import rules.spl2 --manifest manifest.json
  lynxdb saved run 5xx-rate --since 1h
  lynxdb saved delete 5xx-rate`,
		RunE: runSavedList,
	}

	createCmd := &cobra.Command{
		Use:   "create <name> <query>",
		Short: "Save a new query",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedCreate(args[0], args[1])
		},
	}

	var since, from, to string
	var queryParams []string

	runCmd := &cobra.Command{
		Use:               "run <name>",
		Short:             "Execute a saved query",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: completeSavedQueryNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedRun(args[0], since, from, to, queryParams)
		},
	}
	runCmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	runCmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	runCmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")
	runCmd.Flags().StringArrayVarP(&queryParams, "param", "D", nil, "Set query parameter: --param name=value")

	var forceFlag bool

	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a saved query",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedDelete(args[0], forceFlag)
		},
	}
	deleteCmd.Flags().BoolVar(&forceFlag, "force", false, "Skip confirmation prompt")

	var manifestPath string
	var dryRun bool
	var updateExisting bool
	importCmd := &cobra.Command{
		Use:   "import <path>",
		Short: "Import one SPL2 query per line as saved queries",
		Long: `Import one SPL2 query per non-empty line as saved queries.

The optional manifest is rsigma metadata JSON. When present, rule_id becomes
the saved query name and rule metadata is attached to the saved query.`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedImport(args[0], manifestPath, dryRun, updateExisting)
		},
	}
	importCmd.Flags().StringVar(&manifestPath, "manifest", "", "Optional rsigma manifest JSON")
	importCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Print import plan without writing")
	importCmd.Flags().BoolVar(&updateExisting, "update-existing", false, "Overwrite saved queries with matching names")

	cmd.AddCommand(createCmd, runCmd, deleteCmd, importCmd)

	return cmd
}

func newSaveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "save <name> <query>",
		Short: "Save a query (shortcut for 'saved create')",
		Args:  cobra.ExactArgs(2),
		Example: `  lynxdb save "5xx-rate" '_source=nginx status>=500 | stats count by uri | sort -count'
  lynxdb save "error-by-host" 'level=error | stats count by host | sort -count'`,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedCreate(args[0], args[1])
		},
	}
}

func newRunCmd() *cobra.Command {
	var (
		since       string
		from        string
		to          string
		queryParams []string
	)

	cmd := &cobra.Command{
		Use:   "run <name>",
		Short: "Execute a saved query (shortcut for 'saved run')",
		Args:  cobra.ExactArgs(1),
		Example: `  lynxdb run 5xx-rate
  lynxdb run 5xx-rate --since 24h
  lynxdb run 5xx-rate --format csv > report.csv`,
		ValidArgsFunction: completeSavedQueryNames,
		RunE: func(_ *cobra.Command, args []string) error {
			return runSavedRun(args[0], since, from, to, queryParams)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	cmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	cmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")
	cmd.Flags().StringArrayVarP(&queryParams, "param", "D", nil, "Set query parameter: --param name=value")

	return cmd
}

func runSavedList(_ *cobra.Command, _ []string) error {
	ctx := context.Background()

	queries, err := apiClient().ListSavedQueries(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		for _, q := range queries {
			b, _ := json.Marshal(q)
			fmt.Println(string(b))
		}

		return nil
	}

	if len(queries) == 0 {
		fmt.Println("No saved queries.")
		printNextSteps(
			"lynxdb save <name> <query>   Save a query",
		)

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).
		SetColumns("NAME", "QUERY", "CREATED")

	for _, q := range queries {
		created := formatRelativeTime(q.CreatedAt)
		tbl.AddRow(q.Name, q.Q, created)
	}

	fmt.Print(tbl.String())
	fmt.Printf("\n%s\n", t.Dim.Render(fmt.Sprintf("%d saved queries", len(queries))))

	return nil
}

func runSavedCreate(name, query string) error {
	ctx := context.Background()

	if _, err := apiClient().CreateSavedQuery(ctx, client.SavedQueryInput{
		Name: name,
		Q:    query,
	}); err != nil {
		return err
	}

	printSuccess("Saved query %q", name)
	printNextSteps(
		fmt.Sprintf("lynxdb run %s               Execute the saved query", name),
		"lynxdb saved                    List all saved queries",
	)

	return nil
}

type savedImportEnvelope struct {
	Name       string `json:"name"`
	ID         string `json:"id,omitempty"`
	LineNumber int    `json:"line_number"`
	SourceFile string `json:"source_file"`
	Result     string `json:"result,omitempty"`
	Error      string `json:"error,omitempty"`
}

func runSavedImport(path, manifestPath string, dryRun, updateExisting bool) error {
	queries, err := readImportQueries(path)
	if err != nil {
		return err
	}
	if len(queries) == 0 {
		return fmt.Errorf("no queries found in %s", path)
	}

	manifestEntries, err := readImportManifest(manifestPath)
	if err != nil {
		return err
	}

	existing, err := apiClient().ListSavedQueries(context.Background())
	if err != nil {
		return err
	}
	existingByName := make(map[string]client.SavedQuery, len(existing))
	for _, q := range existing {
		existingByName[strings.ToLower(q.Name)] = q
	}

	enc := json.NewEncoder(os.Stdout)
	hadError := false
	for _, q := range queries {
		entry, hasManifest := manifestEntries[q.LineNumber]
		name := savedImportName(path, q.LineNumber, entry)
		env := savedImportEnvelope{
			Name:       name,
			LineNumber: q.LineNumber,
			SourceFile: q.Source,
		}

		if dryRun {
			env.Result = "dry-run"
			if err := enc.Encode(env); err != nil {
				return fmt.Errorf("encode import envelope: %w", err)
			}
			continue
		}

		input := client.SavedQueryInput{
			Name:   name,
			Q:      q.Line,
			Source: "rsigma",
		}
		if hasManifest {
			input.Metadata = manifestMetadata(entry)
		}

		if found, ok := existingByName[strings.ToLower(name)]; ok {
			if !updateExisting {
				env.Error = fmt.Sprintf("saved query %q already exists", name)
				hadError = true
				if err := enc.Encode(env); err != nil {
					return fmt.Errorf("encode import envelope: %w", err)
				}
				continue
			}
			updated, err := apiClient().UpdateSavedQuery(context.Background(), found.ID, input)
			if err != nil {
				env.Error = err.Error()
				hadError = true
			} else {
				env.ID = updated.ID
				env.Result = "updated"
				existingByName[strings.ToLower(name)] = *updated
			}
		} else {
			created, err := apiClient().CreateSavedQuery(context.Background(), input)
			if err != nil {
				env.Error = err.Error()
				hadError = true
			} else {
				env.ID = created.ID
				env.Result = "created"
				existingByName[strings.ToLower(name)] = *created
			}
		}

		if err := enc.Encode(env); err != nil {
			return fmt.Errorf("encode import envelope: %w", err)
		}
	}
	if hadError {
		return fmt.Errorf("one or more saved queries failed to import")
	}

	return nil
}

func readImportQueries(path string) ([]sigmaqueries.Query, error) {
	if path == "-" {
		return sigmaqueries.ReadReader(os.Stdin, "stdin")
	}

	return sigmaqueries.ReadFile(path)
}

func readImportManifest(path string) (map[int]sigmaqueries.ManifestEntry, error) {
	entries := make(map[int]sigmaqueries.ManifestEntry)
	if path == "" {
		return entries, nil
	}

	manifest, err := sigmaqueries.ReadManifest(path)
	if err != nil {
		return nil, err
	}
	for _, entry := range manifest.Queries {
		if entry.Line > 0 {
			entries[entry.Line] = entry
		}
	}

	return entries, nil
}

func savedImportName(path string, line int, entry sigmaqueries.ManifestEntry) string {
	if entry.RuleID != "" {
		return entry.RuleID
	}
	base := filepath.Base(path)
	if path == "-" {
		base = "stdin"
	}

	return fmt.Sprintf("%s:%d", base, line)
}

func manifestMetadata(entry sigmaqueries.ManifestEntry) map[string]interface{} {
	metadata := make(map[string]interface{})
	if entry.RuleID != "" {
		metadata["rule_id"] = entry.RuleID
	}
	if entry.Title != "" {
		metadata["title"] = entry.Title
	}
	if entry.Level != "" {
		metadata["level"] = entry.Level
	}
	if len(entry.Tags) > 0 {
		metadata["tags"] = entry.Tags
	}

	return metadata
}

func runSavedRun(name, since, from, to string, queryParams []string) error {
	sq, err := findSavedQueryByName(name)
	if err != nil {
		return err
	}

	query := sq.Q
	if len(queryParams) > 0 {
		query = spl2.SubstituteParams(query, spl2.ParseParamFlags(queryParams))
	}
	SaveLastQuery(query, since, from, to)

	return runQueryServer(query, since, from, to, "", false, "", false, false, false)
}

func runSavedDelete(name string, force bool) error {
	sq, err := findSavedQueryByName(name)
	if err != nil {
		return err
	}

	if !force {
		msg := fmt.Sprintf("Delete saved query '%s'?", name)
		if !confirmAction(msg) {
			printHint("Aborted.")

			return nil
		}
	}

	ctx := context.Background()
	if err := apiClient().DeleteSavedQuery(ctx, sq.ID); err != nil {
		return err
	}

	printSuccess("Deleted saved query %q", name)

	return nil
}

// findSavedQueryByName fetches all saved queries and finds one by name.
func findSavedQueryByName(name string) (*client.SavedQuery, error) {
	ctx := context.Background()

	queries, err := apiClient().ListSavedQueries(ctx)
	if err != nil {
		return nil, err
	}

	for i := range queries {
		if strings.EqualFold(queries[i].Name, name) {
			return &queries[i], nil
		}
	}

	// Suggest similar names.
	var names []string
	for _, q := range queries {
		names = append(names, q.Name)
	}

	if len(names) > 0 {
		return nil, fmt.Errorf("saved query %q not found. Available: %s", name, strings.Join(names, ", "))
	}

	return nil, fmt.Errorf("saved query %q not found. Save one with: lynxdb save <name> <query>", name)
}

// formatRelativeTime formats an ISO timestamp as a relative time string (e.g., "2d ago").
func formatRelativeTime(iso string) string {
	t, err := time.Parse(time.RFC3339Nano, iso)
	if err != nil {
		// Try other common formats.
		t, err = time.Parse(time.RFC3339, iso)
		if err != nil {
			return iso
		}
	}

	d := time.Since(t)

	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 30*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return fmt.Sprintf("%dmo ago", int(d.Hours()/(24*30)))
	}
}
