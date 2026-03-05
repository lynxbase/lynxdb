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
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/client"
)

var (
	flagAuthKeyName     string
	flagAuthScope       string
	flagAuthExpires     string
	flagAuthDescription string
	flagAuthYes         bool
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Manage API keys",
}

var authCreateKeyCmd = &cobra.Command{
	Use:     "create",
	Aliases: []string{"create-key"}, // backward compat
	Short:   "Create a new API key",
	Example: `  lynxdb auth create --name web-01 --scope ingest
  lynxdb auth create --name grafana-dash --scope query --expires 90d
  lynxdb auth create --name ci-pipeline
  lynxdb auth create --name web-01 --scope ingest --format json | jq -r .api_key`,
	RunE: runAuthCreateKey,
}

var authListKeysCmd = &cobra.Command{
	Use:     "list",
	Aliases: []string{"list-keys", "ls"}, // backward compat
	Short:   "List all API keys",
	RunE:    runAuthListKeys,
}

var authRevokeKeyCmd = &cobra.Command{
	Use:     "revoke <id>",
	Aliases: []string{"revoke-key"}, // backward compat
	Short:   "Revoke an API key",
	Args:    cobra.ExactArgs(1),
	RunE:    runAuthRevokeKey,
}

var authRotateRootCmd = &cobra.Command{
	Use:   "rotate-root",
	Short: "Rotate the root key",
	RunE:  runAuthRotateRoot,
}

var authStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show current authentication state",
	RunE:  runAuthStatus,
}

func init() {
	authCreateKeyCmd.Flags().StringVar(&flagAuthKeyName, "name", "", "Human-readable name (required)")
	_ = authCreateKeyCmd.MarkFlagRequired("name")
	authCreateKeyCmd.Flags().StringVar(&flagAuthScope, "scope", "full", "Scope: ingest|query|admin|full")
	authCreateKeyCmd.Flags().StringVar(&flagAuthExpires, "expires", "never", "Expiration: 30d|90d|1y|never")
	authCreateKeyCmd.Flags().StringVar(&flagAuthDescription, "description", "", "Optional description")

	authRevokeKeyCmd.Flags().BoolVarP(&flagAuthYes, "yes", "y", false, "Skip confirmation prompt")
	authRotateRootCmd.Flags().BoolVarP(&flagAuthYes, "yes", "y", false, "Skip confirmation prompt")

	authCmd.AddCommand(authCreateKeyCmd)
	authCmd.AddCommand(authListKeysCmd)
	authCmd.AddCommand(authRevokeKeyCmd)
	authCmd.AddCommand(authRotateRootCmd)
	authCmd.AddCommand(authStatusCmd)
	rootCmd.AddCommand(authCmd)
}

func runAuthCreateKey(_ *cobra.Command, _ []string) error {
	if !auth.ValidScope(flagAuthScope) {
		return fmt.Errorf("invalid scope %q: must be one of ingest, query, admin, full", flagAuthScope)
	}

	input := client.CreateKeyInput{
		Name:        flagAuthKeyName,
		Scope:       flagAuthScope,
		Description: flagAuthDescription,
	}
	if flagAuthExpires != "" && flagAuthExpires != "never" {
		input.ExpiresIn = flagAuthExpires
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	created, err := apiClient().AuthCreateKeyWithOpts(ctx, input)
	if err != nil {
		return err
	}

	if globalFormat == formatJSON {
		return json.NewEncoder(os.Stdout).Encode(created)
	}

	t := ui.Stderr

	printSuccess("API key created\n")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, t.KeyValue("Name", created.Name))
	fmt.Fprintln(os.Stderr, t.KeyValue("ID", created.ID))
	fmt.Fprintln(os.Stderr, t.KeyValue("Scope", string(created.Scope)))

	if !created.ExpiresAt.IsZero() {
		remaining := time.Until(created.ExpiresAt)
		days := int(remaining.Hours() / 24)
		fmt.Fprintln(os.Stderr, t.KeyValue("Expires", fmt.Sprintf("%s (%dd)", created.ExpiresAt.Format("2006-01-02"), days)))
	} else {
		fmt.Fprintln(os.Stderr, t.KeyValue("Expires", "never"))
	}
	if created.Description != "" {
		fmt.Fprintln(os.Stderr, t.KeyValue("Description", created.Description))
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Token (shown once — save it now):\n")
	fmt.Fprintf(os.Stderr, "    %s\n", t.Bold.Render(created.Token))

	scope := created.Scope
	switch scope {
	case "ingest":
		printFilebeatSnippet(t, created)
	case "query":
		printQuerySnippet(t, created)
	}

	fmt.Fprintln(os.Stderr)

	return nil
}

// printFilebeatSnippet prints a ready-to-paste filebeat.yml snippet.
func printFilebeatSnippet(t *ui.Theme, created *client.AuthCreatedKey) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s filebeat.yml %s\n",
		t.Dim.Render("───"),
		t.Dim.Render("─────────────────────────────────────────"))

	host := strings.TrimRight(globalServer, "/") + "/api/v1/es"

	fmt.Fprintf(os.Stderr, "  output.elasticsearch:\n")
	fmt.Fprintf(os.Stderr, "    hosts: [\"%s\"]\n", host)
	fmt.Fprintf(os.Stderr, "    api_key: \"%s\"\n", created.APIKeyComposite)
	fmt.Fprintf(os.Stderr, "    allow_older_versions: true\n")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  setup.ilm.enabled: false\n")
	fmt.Fprintf(os.Stderr, "  setup.template.enabled: false\n")

	fmt.Fprintf(os.Stderr, "  %s\n",
		t.Dim.Render("─────────────────────────────────────────────────────────────"))
}

// printQuerySnippet prints usage hints for query-scoped keys.
func printQuerySnippet(t *ui.Theme, created *client.AuthCreatedKey) {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s Usage %s\n",
		t.Dim.Render("───"),
		t.Dim.Render("───────────────────────────────────────────"))
	fmt.Fprintf(os.Stderr, "  CLI:      lynxdb query 'level=error' --token %s\n", truncateStr(created.Token, 20)+"...")
	fmt.Fprintf(os.Stderr, "  Header:   Authorization: Bearer %s\n", truncateStr(created.Token, 20)+"...")
	fmt.Fprintf(os.Stderr, "  %s\n",
		t.Dim.Render("─────────────────────────────────────────────────────────────"))
}

func runAuthListKeys(_ *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	keys, err := apiClient().AuthListKeys(ctx)
	if err != nil {
		return err
	}

	if len(keys) == 0 {
		printMeta("No API keys configured.")

		return nil
	}

	t := ui.Stdout
	tbl := ui.NewTable(t).SetColumns("ID", "NAME", "SCOPE", "EXPIRES", "LAST USED", "CREATED")

	expiresSoon := 0
	for _, k := range keys {
		age := formatRelativeTime(k.CreatedAt.Format(time.RFC3339))
		lastUsed := "—"

		if !k.LastUsedAt.IsZero() {
			lastUsed = formatRelativeTime(k.LastUsedAt.Format(time.RFC3339))
		}

		name := k.Name
		if k.IsRoot {
			name = "[root]"
		}

		scope := k.Scope
		if scope == "" {
			scope = "full"
		}

		expires := "never"
		if !k.ExpiresAt.IsZero() {
			expires = k.ExpiresAt.Format("2006-01-02")
			if time.Until(k.ExpiresAt) < 30*24*time.Hour {
				expiresSoon++
			}
		}

		tbl.AddRow(
			truncateStr(k.ID, 18),
			truncateStr(name, 18),
			scope,
			expires,
			lastUsed,
			age,
		)
	}
	fmt.Print(tbl.String())

	summary := fmt.Sprintf("  %d keys", len(keys))
	if expiresSoon > 0 {
		summary += fmt.Sprintf("  •  %d expires soon (< 30d)", expiresSoon)
	}
	fmt.Fprintf(os.Stderr, "\n%s\n", summary)

	return nil
}

func runAuthRevokeKey(_ *cobra.Command, args []string) error {
	id := args[0]

	if !flagAuthYes && isStdinTTY() {
		if !confirmAction(fmt.Sprintf("Revoke key %s? This will immediately reject all requests using this key.", id)) {
			fmt.Fprintln(os.Stderr, "  Aborted.")

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := apiClient().AuthRevokeKey(ctx, id); err != nil {
		return err
	}

	printSuccess("Key revoked")

	return nil
}

func runAuthRotateRoot(_ *cobra.Command, _ []string) error {
	if !flagAuthYes && isStdinTTY() {
		msg := "This will generate a new root key and revoke the current one.\n" +
			"  All clients using the current root key will lose access."
		if !confirmAction(msg) {
			fmt.Fprintln(os.Stderr, "  Aborted.")

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := apiClient().AuthRotateRoot(ctx)
	if err != nil {
		return err
	}

	t := ui.Stderr
	printSuccess("New root key:\n")
	fmt.Fprintf(os.Stderr, "\n    %s\n\n", t.Bold.Render(result.Token))
	fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render("Save this key now. It will NOT be shown again."))
	fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render(fmt.Sprintf("Old root key (%s) has been revoked.", result.RevokedKeyID)))

	if err := auth.SaveToken(globalServer, result.Token); err != nil {
		printWarning("Could not update credentials file: %v", err)
	}

	return nil
}

func runAuthStatus(_ *cobra.Command, _ []string) error {
	t := ui.Stderr
	fmt.Fprintln(os.Stderr, t.KeyValue("Server", globalServer))

	if strings.HasPrefix(globalServer, "https://") {
		_, fp, loadErr := auth.LoadCredentials(globalServer)
		if loadErr == nil && fp != "" {
			fmt.Fprintln(os.Stderr, t.KeyValue("TLS", "TOFU (fingerprint saved)"))
			fmt.Fprintln(os.Stderr, t.KeyValue("Fingerprint", fp))
		} else if globalTLSSkipVerify {
			fmt.Fprintln(os.Stderr, t.KeyValue("TLS", t.Warning.Render("skip-verify (insecure)")))
		} else {
			fmt.Fprintln(os.Stderr, t.KeyValue("TLS", "CA-verified"))
		}
	} else {
		fmt.Fprintln(os.Stderr, t.KeyValue("TLS", t.Dim.Render("off")))
	}

	token := resolveToken()
	if token == "" {
		fmt.Fprintln(os.Stderr, t.KeyValue("Auth", t.Dim.Render("unknown (no saved credentials)")))

		return nil
	}

	c := client.NewClient(
		client.WithBaseURL(globalServer),
		client.WithAuthToken(token),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.Status(ctx)
	if err != nil {
		if client.IsAuthRequired(err) {
			fmt.Fprintln(os.Stderr, t.KeyValue("Auth", "enabled"))
			fmt.Fprintln(os.Stderr, t.KeyValue("Status", t.Error.Render("invalid credentials")))

			return nil
		}

		return fmt.Errorf("connect to %s: %w", globalServer, err)
	}

	prefix := auth.KeyPrefix(token)
	fmt.Fprintln(os.Stderr, t.KeyValue("Your key", prefix+"..."))
	fmt.Fprintln(os.Stderr, t.KeyValue("Status", t.Success.Render("authenticated")))

	return nil
}
