package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/install"
	"github.com/lynxbase/lynxdb/internal/ui"
)

var (
	flagUninstallYes   bool
	flagUninstallPurge bool
)

func init() {
	rootCmd.AddCommand(newUninstallCmd())
}

func newUninstallCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Remove LynxDB binary and service",
		Long: `Remove the LynxDB binary, service files, and optionally config files.

Data in the data directory is NEVER removed — only the binary and service
configuration are cleaned up.

By default, this command:
  1. Stops and disables the LynxDB service
  2. Removes the systemd/launchd service file
  3. Removes the binary

With --purge, it additionally removes:
  - Config directory (/etc/lynxdb or ~/.config/lynxdb)
  - Ulimits file (/etc/security/limits.d/lynxdb.conf on Linux)

Data is always preserved. To remove data, manually run:
  rm -rf /var/lib/lynxdb   (or your configured data directory)`,
		Example: `  # Interactive uninstall
  sudo lynxdb uninstall

  # Non-interactive
  sudo lynxdb uninstall --yes

  # Also remove config files
  sudo lynxdb uninstall --yes --purge`,
		RunE: runUninstall,
	}

	cmd.Flags().BoolVar(&flagUninstallYes, "yes", false, "Skip confirmation prompts")
	cmd.Flags().BoolVar(&flagUninstallPurge, "purge", false, "Also remove config files (data is always preserved)")

	return cmd
}

func runUninstall(_ *cobra.Command, _ []string) error {
	t := ui.Stderr
	if t == nil {
		ui.Init(globalNoColor)
		t = ui.Stderr
	}

	mode, paths := install.DetectInstallMode()

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", ui.Stdout.Bold.Render("LynxDB Uninstall"))
	fmt.Fprintf(os.Stderr, "  %s\n", t.Rule.Render("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Mode:     %s\n", mode)
	fmt.Fprintf(os.Stderr, "  Binary:   %s\n", paths.Binary)

	if paths.ServiceFile != "" {
		fmt.Fprintf(os.Stderr, "  Service:  %s\n", paths.ServiceFile)
	}

	if flagUninstallPurge {
		fmt.Fprintf(os.Stderr, "  Config:   %s %s\n", paths.ConfigDir, t.Warning.Render("(will be removed)"))
	}

	fmt.Fprintf(os.Stderr, "  Data:     %s %s\n", paths.DataDir, t.Success.Render("(preserved)"))
	fmt.Fprintln(os.Stderr)

	if !flagUninstallYes && isStdinTTY() {
		msg := "This will remove LynxDB binary and service. Data is preserved."
		if flagUninstallPurge {
			msg = "This will remove LynxDB binary, service, and config files. Data is preserved."
		}

		if !confirmAction(msg) {
			fmt.Fprintln(os.Stderr, "  Aborted.")
			return nil
		}

		fmt.Fprintln(os.Stderr)
	}

	opts := install.UninstallOptions{
		Mode:  mode,
		Yes:   flagUninstallYes,
		Purge: flagUninstallPurge,
	}

	steps := install.UninstallSteps(opts, paths)

	var hasErrors bool

	for _, step := range steps {
		detail, err := step.Fn()
		if err != nil {
			fmt.Fprintf(os.Stderr, "  %s %-18s %s\n",
				t.IconError(),
				ui.Stdout.Bold.Render(step.Name),
				t.Error.Render(err.Error()))
			hasErrors = true

			continue
		}

		fmt.Fprintf(os.Stderr, "  %s %-18s %s\n",
			t.IconOK(),
			ui.Stdout.Bold.Render(step.Name),
			detail)
	}

	fmt.Fprintln(os.Stderr)

	if hasErrors {
		fmt.Fprintf(os.Stderr, "  %s %s\n\n",
			t.IconError(),
			t.Error.Render("Uninstall completed with errors. Check the output above."))

		return fmt.Errorf("uninstall completed with errors")
	}

	fmt.Fprintf(os.Stderr, "  %s %s\n", t.IconOK(), t.Success.Render("LynxDB has been uninstalled."))
	fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render(fmt.Sprintf("Data preserved at %s. To remove: rm -rf %s", paths.DataDir, paths.DataDir)))
	fmt.Fprintln(os.Stderr)

	return nil
}
