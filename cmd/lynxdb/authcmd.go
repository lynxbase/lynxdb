package main

import (
	"context"
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
	flagAuthKeyName string
	flagAuthYes     bool
)

var authCmd = &cobra.Command{
	Use:   "auth",
	Short: "Manage API keys",
}

var authCreateKeyCmd = &cobra.Command{
	Use:   "create-key",
	Short: "Create a new API key",
	Example: `  lynxdb auth create-key --name ci-pipeline
  lynxdb auth create-key --name grafana --server https://lynxdb.prod.com`,
	RunE: runAuthCreateKey,
}

var authListKeysCmd = &cobra.Command{
	Use:     "list-keys",
	Short:   "List all API keys",
	Aliases: []string{"ls"},
	RunE:    runAuthListKeys,
}

var authRevokeKeyCmd = &cobra.Command{
	Use:   "revoke-key <id>",
	Short: "Revoke an API key",
	Args:  cobra.ExactArgs(1),
	RunE:  runAuthRevokeKey,
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	created, err := apiClient().AuthCreateKey(ctx, flagAuthKeyName)
	if err != nil {
		return err
	}

	t := ui.Stderr
	printSuccess("Created API key:\n")
	fmt.Fprintln(os.Stderr, t.KeyValue("Key ID", created.ID))
	fmt.Fprintln(os.Stderr, t.KeyValue("Name", created.Name))
	fmt.Fprintln(os.Stderr, t.KeyValue("Token", t.Bold.Render(created.Token)))
	fmt.Fprintf(os.Stderr, "\n  %s\n", t.Dim.Render("Save this key now. It will NOT be shown again."))

	return nil
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
	tbl := ui.NewTable(t).SetColumns("ID", "NAME", "PREFIX", "CREATED", "LAST USED")

	for _, k := range keys {
		age := formatRelativeTime(k.CreatedAt.Format(time.RFC3339))
		lastUsed := "never"

		if !k.LastUsedAt.IsZero() {
			lastUsed = formatRelativeTime(k.LastUsedAt.Format(time.RFC3339))
		}

		name := k.Name
		if k.IsRoot {
			name += " (root)"
		}

		tbl.AddRow(
			truncateStr(k.ID, 13),
			truncateStr(name, 13),
			k.Prefix,
			age,
			lastUsed)
	}
	fmt.Print(tbl.String())

	return nil
}

func runAuthRevokeKey(_ *cobra.Command, args []string) error {
	id := args[0]

	if !flagAuthYes && isStdinTTY() {
		if !confirmAction(fmt.Sprintf("Revoke key %s? This cannot be undone.", id)) {
			fmt.Fprintln(os.Stderr, "  Aborted.")

			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := apiClient().AuthRevokeKey(ctx, id); err != nil {
		return err
	}

	printSuccess("Revoked key %s", id)

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

	// Auto-update credentials file if we had the old key saved.
	if err := auth.SaveToken(globalServer, result.Token); err != nil {
		printWarning("Could not update credentials file: %v", err)
	}

	return nil
}

func runAuthStatus(_ *cobra.Command, _ []string) error {
	t := ui.Stderr
	fmt.Fprintln(os.Stderr, t.KeyValue("Server", globalServer))

	// Show TLS status for HTTPS servers.
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

	// Try to authenticate.
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
