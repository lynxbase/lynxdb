package main

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
)

func init() {
	rootCmd.AddCommand(newVersionCmd())
}

func newVersionCmd() *cobra.Command {
	var (
		flagShort bool
		flagJSON  bool
	)

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runVersion(flagShort, flagJSON)
		},
	}

	cmd.Flags().BoolVar(&flagShort, "short", false, "Output just the version string")
	cmd.Flags().BoolVar(&flagJSON, "json", false, "Output structured JSON")

	return cmd
}

func runVersion(short, jsonOut bool) error {
	switch {
	case short:
		fmt.Println(buildinfo.Version)
	case jsonOut || !isTTY():
		info := buildinfo.Info()
		b, err := json.MarshalIndent(info, "", "  ")
		if err != nil {
			return fmt.Errorf("version: marshal info: %w", err)
		}
		fmt.Println(string(b))
	default:
		t := ui.Stdout
		fmt.Println()
		fmt.Println(t.KeyValue("Version", buildinfo.Version))
		fmt.Println(t.KeyValue("Commit", buildinfo.Commit))
		fmt.Println(t.KeyValue("Built", buildinfo.Date))
		fmt.Println(t.KeyValue("Go", buildinfo.Runtime()))
		fmt.Println()
	}

	return nil
}
