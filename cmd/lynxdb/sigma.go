package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/sigmaqueries"
)

func init() {
	rootCmd.AddCommand(newSigmaCmd())
}

func newSigmaCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sigma",
		Short: "Inspect Sigma compatibility metadata",
	}
	cmd.AddCommand(newSigmaCompatCheckCmd())

	return cmd
}

func newSigmaCompatCheckCmd() *cobra.Command {
	var rsigmaVersion string
	var jsonOut bool

	cmd := &cobra.Command{
		Use:   "compat-check",
		Short: "Check embedded rsigma compatibility metadata",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runSigmaCompatCheck(rsigmaVersion, jsonOut)
		},
	}
	cmd.Flags().StringVar(&rsigmaVersion, "rsigma-version", "", "rsigma version to check")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print the full embedded compatibility manifest")

	return cmd
}

func runSigmaCompatCheck(rsigmaVersion string, jsonOut bool) error {
	manifest, err := sigmaqueries.EmbeddedCompatManifest()
	if err != nil {
		return err
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(manifest)
	}

	if rsigmaVersion != "" {
		if compatibleRsigVersion(rsigmaVersion, manifest.RsigmaVersion) {
			fmt.Printf("compatible with rsigma %s\n", rsigmaVersion)
			return nil
		}

		return fmt.Errorf("rsigma %s is not covered by LynxDB's embedded compatibility manifest (supported: %s)", rsigmaVersion, manifest.RsigmaVersion)
	}

	fmt.Printf("rsigma compatibility: %s\n", manifest.RsigmaVersion)
	fmt.Printf("lynxdb manifest version: %s\n", manifest.LynxDBVersion)
	fmt.Printf("fixtures: %d\n", len(manifest.Fixtures))

	return nil
}

func compatibleRsigVersion(got, supported string) bool {
	got = strings.TrimPrefix(got, "v")
	supported = strings.TrimPrefix(supported, "v")

	return got == supported
}
