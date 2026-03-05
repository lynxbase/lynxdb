package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/install"
	"github.com/lynxbase/lynxdb/internal/ui"
)

var (
	flagInstallYes              bool
	flagInstallUser             string
	flagInstallGroup            string
	flagInstallDataDir          string
	flagInstallPrefix           string
	flagInstallSkipService      bool
	flagInstallSkipConfig       bool
	flagInstallSkipCapabilities bool
	flagInstallSkipUlimits      bool
	flagInstallSkipSelfTest     bool
)

func init() {
	rootCmd.AddCommand(newInstallCmd())
}

func newInstallCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Set up LynxDB as a system service (self-install)",
		Long: `Install LynxDB as a production-ready system service.

When run as root (sudo), this command:
  1. Copies the binary to /usr/local/bin/lynxdb
  2. Creates a 'lynxdb' system user and group
  3. Creates /etc/lynxdb, /var/lib/lynxdb, /var/log/lynxdb directories
  4. Writes a production config to /etc/lynxdb/config.yaml
  5. Configures ulimits (nofile, nproc, memlock)
  6. Sets CAP_NET_BIND_SERVICE capability
  7. Installs a hardened systemd service
  8. Runs a post-install self-test

When run without root, it installs for the current user:
  - Binary: ~/.local/bin/lynxdb
  - Config: ~/.config/lynxdb/config.yaml
  - Data: ~/.local/share/lynxdb/
  - macOS: installs a launchd agent`,
		Example: `  # System install (production)
  sudo lynxdb install

  # Non-interactive system install
  sudo lynxdb install --yes

  # User-local install
  lynxdb install

  # Custom data directory
  sudo lynxdb install --data-dir /data/lynxdb

  # Minimal install (no service, no config)
  lynxdb install --skip-service --skip-config`,
		RunE: runInstall,
	}

	cmd.Flags().BoolVar(&flagInstallYes, "yes", false, "Skip confirmation prompts (non-interactive)")
	cmd.Flags().StringVar(&flagInstallUser, "user", "lynxdb", "System user name")
	cmd.Flags().StringVar(&flagInstallGroup, "group", "lynxdb", "System group name")
	cmd.Flags().StringVar(&flagInstallDataDir, "data-dir", "", "Data directory override")
	cmd.Flags().StringVar(&flagInstallPrefix, "prefix", "", "Installation prefix override")
	cmd.Flags().BoolVar(&flagInstallSkipService, "skip-service", false, "Skip systemd/launchd setup")
	cmd.Flags().BoolVar(&flagInstallSkipConfig, "skip-config", false, "Skip config file creation")
	cmd.Flags().BoolVar(&flagInstallSkipCapabilities, "skip-capabilities", false, "Skip Linux capabilities")
	cmd.Flags().BoolVar(&flagInstallSkipUlimits, "skip-ulimits", false, "Skip ulimits configuration")
	cmd.Flags().BoolVar(&flagInstallSkipSelfTest, "skip-self-test", false, "Skip post-install verification")

	return cmd
}

func runInstall(_ *cobra.Command, _ []string) error {
	opts := install.DefaultOptions()
	opts.User = flagInstallUser
	opts.Group = flagInstallGroup
	opts.DataDir = flagInstallDataDir
	opts.Prefix = flagInstallPrefix
	opts.Yes = flagInstallYes
	opts.SkipService = flagInstallSkipService
	opts.SkipConfig = flagInstallSkipConfig
	opts.SkipCapabilities = flagInstallSkipCapabilities
	opts.SkipUlimits = flagInstallSkipUlimits
	opts.SkipSelfTest = flagInstallSkipSelfTest

	paths := install.ResolvePaths(opts)

	t := ui.Stderr
	if t == nil {
		// Safety: if theme not initialized yet.
		ui.Init(globalNoColor)
		t = ui.Stderr
	}

	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  %s\n", ui.Stdout.Bold.Render("LynxDB Install"))
	fmt.Fprintf(os.Stderr, "  %s\n", t.Rule.Render("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "  Mode:     %s\n", opts.Mode)
	fmt.Fprintf(os.Stderr, "  Binary:   %s\n", paths.Binary)
	fmt.Fprintf(os.Stderr, "  Config:   %s\n", paths.ConfigFile)
	fmt.Fprintf(os.Stderr, "  Data:     %s\n", paths.DataDir)

	if paths.ServiceFile != "" {
		fmt.Fprintf(os.Stderr, "  Service:  %s\n", paths.ServiceFile)
	}

	fmt.Fprintln(os.Stderr)

	if !opts.Yes && isStdinTTY() {
		if !confirmAction("Proceed with installation?") {
			fmt.Fprintln(os.Stderr, "  Aborted.")
			return nil
		}
		fmt.Fprintln(os.Stderr)
	}

	steps, _ := install.BuildSteps(opts)

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
			t.Error.Render("Installation completed with errors. Check the output above."))

		return fmt.Errorf("install completed with errors")
	}

	printInstallNextSteps(opts, paths)

	return nil
}

func printInstallNextSteps(opts install.Options, paths install.Paths) {
	var steps []string

	switch {
	case opts.Mode == install.ModeSystem && runtime.GOOS == "linux":
		steps = append(steps,
			"systemctl enable --now lynxdb    Start LynxDB and enable on boot",
			"lynxdb demo                      Generate sample data",
			"lynxdb doctor                    Verify full environment",
		)
	case opts.Mode == install.ModeUser && runtime.GOOS == "darwin":
		steps = append(steps,
			fmt.Sprintf("launchctl load %s    Start LynxDB", paths.ServiceFile),
			"lynxdb demo                      Generate sample data",
			"lynxdb doctor                    Verify full environment",
		)
	default:
		steps = append(steps,
			"lynxdb server                    Start the server",
			"lynxdb demo                      Generate sample data",
			"lynxdb doctor                    Verify full environment",
		)
	}

	printNextSteps(steps...)
	fmt.Fprintln(os.Stderr)
}
