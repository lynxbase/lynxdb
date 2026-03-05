package main

import (
	"fmt"
	"net/url"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(newUICmd())
	rootCmd.AddCommand(newOpenCmd())
	rootCmd.AddCommand(newShareCmd())
}

// lynxdb ui

func newUICmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ui",
		Short: "Open LynxDB Web UI in browser",
		Example: `  lynxdb ui
  lynxdb ui --server https://prod:3100`,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runUI()
		},
	}
}

func runUI() error {
	u := globalServer
	printHint("Opening %s in browser...", u)

	if err := openBrowser(u); err != nil {
		return fmt.Errorf("could not open browser: %w\n  Open manually: %s", err, u)
	}

	return nil
}

// lynxdb open

func newOpenCmd() *cobra.Command {
	var (
		since string
		from  string
		to    string
	)

	cmd := &cobra.Command{
		Use:   "open [query]",
		Short: "Open a query in the Web UI",
		Example: `  lynxdb open 'level=error | stats count by source'
  lynxdb open 'level=error | stats count by source' --since 1h`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runOpen(args[0], since, from, to)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	cmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	cmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")

	return cmd
}

func runOpen(query, since, from, to string) error {
	u := buildSearchURL(query, since, from, to)
	printHint("Opening in browser: %s", u)

	if err := openBrowser(u); err != nil {
		return fmt.Errorf("could not open browser: %w\n  Open manually: %s", err, u)
	}

	return nil
}

// lynxdb share

func newShareCmd() *cobra.Command {
	var (
		since string
		from  string
		to    string
	)

	cmd := &cobra.Command{
		Use:   "share [query]",
		Short: "Generate a shareable URL for a query",
		Example: `  lynxdb share 'level=error | stats count by source'
  lynxdb share 'level=error | stats count by source' --since 1h`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runShare(args[0], since, from, to)
		},
	}

	cmd.Flags().StringVarP(&since, "since", "s", "", "Relative time range (e.g., 15m, 1h)")
	cmd.Flags().StringVar(&from, "from", "", "Absolute start time (ISO 8601)")
	cmd.Flags().StringVar(&to, "to", "", "Absolute end time (ISO 8601)")

	return cmd
}

func runShare(query, since, from, to string) error {
	u := buildSearchURL(query, since, from, to)

	// Always print the URL to stdout (pipe-friendly).
	fmt.Println(u)

	return nil
}

// buildSearchURL constructs a Web UI search URL with query parameters.
func buildSearchURL(query, since, from, to string) string {
	params := url.Values{}
	params.Set("q", query)

	if since != "" {
		params.Set("from", "-"+since)
	}
	if from != "" {
		params.Set("from", from)
	}
	if to != "" {
		params.Set("to", to)
	}

	return globalServer + "/search?" + params.Encode()
}

// openBrowser opens the specified URL in the default browser.
func openBrowser(u string) error {
	switch runtime.GOOS {
	case "darwin":
		return exec.Command("open", u).Start()
	case "windows":
		return exec.Command("cmd", "/c", "start", u).Start()
	default:
		return exec.Command("xdg-open", u).Start()
	}
}
