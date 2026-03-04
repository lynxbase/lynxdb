package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/config"
)

var flagInitSystem bool

// Shared CLI override flags — used by both "server" and "config" commands.
var (
	flagAddr               string
	flagDataDir            string
	flagS3Bucket           string
	flagS3Region           string
	flagS3Prefix           string
	flagCompactionInterval string
	flagTieringInterval    string
	flagCacheMaxMB         string
	flagLogLevel           string
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Show current configuration",
	RunE:  runConfigShow,
}

var configInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Create default config file",
	RunE:  runConfigInit,
}

var configValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate config file",
	RunE:  runConfigValidate,
}

var configReloadCmd = &cobra.Command{
	Use:   "reload",
	Short: "Reload server config (sends SIGHUP)",
	RunE:  runConfigReload,
}

var configGetCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Show a single config value",
	Example: `  lynxdb config get retention
  lynxdb config get storage.compression
  lynxdb config get listen`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigGet,
	ValidArgsFunction: func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return config.KnownKeyNames(), cobra.ShellCompDirectiveNoFileComp
	},
}

var configSetDryRun bool

var configSetCmd = &cobra.Command{
	Use:   "set <key> <value>",
	Short: "Set a config value in the config file",
	Example: `  lynxdb config set retention 30d
  lynxdb config set storage.compression zstd
  lynxdb config set log_level debug
  lynxdb config set retention 1d --dry-run`,
	Args: cobra.ExactArgs(2),
	RunE: runConfigSet,
	ValidArgsFunction: func(_ *cobra.Command, args []string, _ string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return config.KnownKeyNames(), cobra.ShellCompDirectiveNoFileComp
		}

		return nil, cobra.ShellCompDirectiveNoFileComp
	},
}

var configEditCmd = &cobra.Command{
	Use:   "edit",
	Short: "Open config file in $EDITOR",
	RunE:  runConfigEdit,
}

var configPathCmd = &cobra.Command{
	Use:   "path",
	Short: "Print config file path",
	RunE:  runConfigPath,
}

var configResetDryRun bool

var configResetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset config file to defaults (with confirmation)",
	RunE:  runConfigReset,
}

var (
	profileURL   string
	profileToken string
)

var configAddProfileCmd = &cobra.Command{
	Use:   "add-profile <name>",
	Short: "Add or update a connection profile",
	Example: `  lynxdb config add-profile prod --url https://lynxdb.company.com --token xxx
  lynxdb config add-profile staging --url https://staging.company.com`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigAddProfile,
}

var configListProfilesCmd = &cobra.Command{
	Use:   "list-profiles",
	Short: "List all connection profiles",
	RunE:  runConfigListProfiles,
}

var configRemoveProfileCmd = &cobra.Command{
	Use:   "remove-profile <name>",
	Short: "Remove a connection profile",
	Args:  cobra.ExactArgs(1),
	RunE:  runConfigRemoveProfile,
}

func init() {
	configAddProfileCmd.Flags().StringVar(&profileURL, "url", "", "Server URL for the profile (required)")
	configAddProfileCmd.Flags().StringVar(&profileToken, "token", "", "Authentication token (optional)")
	_ = configAddProfileCmd.MarkFlagRequired("url")

	// Override flags available on config and its subcommands (show, validate).
	configCmd.PersistentFlags().StringVar(&flagAddr, "addr", "", "Listen address override")
	configCmd.PersistentFlags().StringVar(&flagDataDir, "data-dir", "", "Data directory override")
	configCmd.PersistentFlags().StringVar(&flagS3Bucket, "s3-bucket", "", "S3 bucket override")
	configCmd.PersistentFlags().StringVar(&flagS3Region, "s3-region", "", "AWS region override")
	configCmd.PersistentFlags().StringVar(&flagS3Prefix, "s3-prefix", "", "S3 prefix override")
	configCmd.PersistentFlags().StringVar(&flagCompactionInterval, "compaction-interval", "", "Compaction interval override")
	configCmd.PersistentFlags().StringVar(&flagTieringInterval, "tiering-interval", "", "Tiering interval override")
	configCmd.PersistentFlags().StringVar(&flagCacheMaxMB, "cache-max-mb", "", "Cache max size override")
	configCmd.PersistentFlags().StringVar(&flagLogLevel, "log-level", "", "Log level override")

	configInitCmd.Flags().BoolVar(&flagInitSystem, "system", false, "Write to /etc/lynxdb/config.yaml")
	configSetCmd.Flags().BoolVar(&configSetDryRun, "dry-run", false, "Show what would change without applying")
	configResetCmd.Flags().BoolVar(&configResetDryRun, "dry-run", false, "Show what would be reset without applying")
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configValidateCmd)
	configCmd.AddCommand(configReloadCmd)
	configCmd.AddCommand(configGetCmd)
	configCmd.AddCommand(configSetCmd)
	configCmd.AddCommand(configEditCmd)
	configCmd.AddCommand(configPathCmd)
	configCmd.AddCommand(configResetCmd)
	configCmd.AddCommand(configAddProfileCmd)
	configCmd.AddCommand(configListProfilesCmd)
	configCmd.AddCommand(configRemoveProfileCmd)
	rootCmd.AddCommand(configCmd)
}

// sectionForKey returns the section prefix for a dotted config key.
func sectionForKey(key string) string {
	if idx := strings.IndexByte(key, '.'); idx >= 0 {
		return key[:idx]
	}

	return ""
}

// applyCLIOverrides applies explicitly-set CLI flags to cfg and returns both
// the CLIOverride list (for config show) and the flag name list (for startup banner).
// Parse errors for duration/bytesize flags are propagated instead of silently ignored.
func applyCLIOverrides(cmd *cobra.Command, cfg *config.Config) ([]config.CLIOverride, []string, error) {
	var cli []config.CLIOverride
	var flags []string

	checkStr := func(flagName, key, value string, apply func()) {
		if cmd.Flags().Changed(flagName) {
			apply()
			cli = append(cli, config.CLIOverride{Key: key, Value: value, Flag: "--" + flagName})
			flags = append(flags, "--"+flagName)
		}
	}

	checkStr("addr", "listen", flagAddr, func() { cfg.Listen = flagAddr })
	checkStr("data-dir", "data_dir", flagDataDir, func() { cfg.DataDir = flagDataDir })
	checkStr("s3-bucket", "storage.s3_bucket", flagS3Bucket, func() { cfg.Storage.S3Bucket = flagS3Bucket })
	checkStr("s3-region", "storage.s3_region", flagS3Region, func() { cfg.Storage.S3Region = flagS3Region })
	checkStr("s3-prefix", "storage.s3_prefix", flagS3Prefix, func() { cfg.Storage.S3Prefix = flagS3Prefix })
	checkStr("log-level", "log_level", flagLogLevel, func() { cfg.LogLevel = flagLogLevel })

	if cmd.Flags().Changed("compaction-interval") {
		d, err := config.ParseDuration(flagCompactionInterval)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid --compaction-interval: %w", err)
		}
		cfg.Storage.CompactionInterval = d.Duration()
		cli = append(cli, config.CLIOverride{Key: "storage.compaction_interval", Value: d.Duration().String(), Flag: "--compaction-interval"})
		flags = append(flags, "--compaction-interval")
	}
	if cmd.Flags().Changed("tiering-interval") {
		d, err := config.ParseDuration(flagTieringInterval)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid --tiering-interval: %w", err)
		}
		cfg.Storage.TieringInterval = d.Duration()
		cli = append(cli, config.CLIOverride{Key: "storage.tiering_interval", Value: d.Duration().String(), Flag: "--tiering-interval"})
		flags = append(flags, "--tiering-interval")
	}
	if cmd.Flags().Changed("cache-max-mb") {
		b, err := config.ParseByteSize(flagCacheMaxMB)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid --cache-max-mb: %w", err)
		}
		cfg.Storage.CacheMaxBytes = b
		cli = append(cli, config.CLIOverride{Key: "storage.cache_max_bytes", Value: b.String(), Flag: "--cache-max-mb"})
		flags = append(flags, "--cache-max-mb")
	}
	if cmd.Flags().Changed("max-query-pool") {
		b, err := config.ParseByteSize(flagMaxQueryPool)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid --max-query-pool: %w", err)
		}
		cfg.Query.GlobalQueryPoolBytes = b
		cli = append(cli, config.CLIOverride{Key: "query.global_query_pool_bytes", Value: b.String(), Flag: "--max-query-pool"})
		flags = append(flags, "--max-query-pool")
	}

	checkStr("spill-dir", "query.spill_dir", flagSpillDir, func() { cfg.Query.SpillDir = flagSpillDir })

	return cli, flags, nil
}

func runConfigShow(cmd *cobra.Command, args []string) error {
	cfg, cfgPath, _, warnings, err := config.LoadWithOverrides(flagConfigPath)
	if err != nil {
		return err
	}

	t := ui.Stdout

	// Print warnings for unknown keys.
	for _, w := range warnings {
		printWarning("%s", w)
	}

	// Collect and apply CLI overrides.
	cli, _, err := applyCLIOverrides(cmd, cfg)
	if err != nil {
		return err
	}

	entries := config.EntriesWithCLI(flagConfigPath, cli)

	// Print header.
	if cfgPath != "" {
		fmt.Printf("  %s %s\n\n", t.Bold.Render("Config file:"), cfgPath)
	} else {
		fmt.Printf("  %s\n\n", t.Dim.Render("No config file found (using defaults)"))
	}

	// Group entries by section with blank-line separators.
	prevSection := "\x00" // sentinel: no section yet
	for _, e := range entries {
		section := sectionForKey(e.Key)
		if section != prevSection {
			if prevSection != "\x00" {
				fmt.Println()
			}
			switch section {
			case "":
				fmt.Printf("  %s\n", t.Bold.Render("Server"))
			case "storage":
				fmt.Printf("  %s\n", t.Bold.Render("Storage"))
			case "query":
				fmt.Printf("  %s\n", t.Bold.Render("Query"))
			case "ingest":
				fmt.Printf("  %s\n", t.Bold.Render("Ingest"))
			case "http":
				fmt.Printf("  %s\n", t.Bold.Render("HTTP"))
			}
			prevSection = section
		}
		fmt.Printf("  %-40s %-25s %s\n", e.Key+":", e.Value, t.Dim.Render(fmt.Sprintf("(%s)", e.Source)))
	}
	fmt.Println()

	return nil
}

func runConfigInit(cmd *cobra.Command, args []string) error {
	path := flagConfigPath
	if path == "" {
		if flagInitSystem {
			path = "/etc/lynxdb/config.yaml"
		} else {
			path = config.DefaultConfigFilePath()
		}
	}

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("config file already exists: %s", path)
	}

	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create config directory: %w", err)
		}
	}

	if err := os.WriteFile(path, config.DefaultsTemplate, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	fmt.Printf("  %s Created config template at %s\n", ui.Stdout.IconOK(), path)

	return nil
}

func runConfigValidate(cmd *cobra.Command, args []string) error {
	cfg, path, _, warnings, err := config.LoadWithOverrides(flagConfigPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, config.FormatConfigError(err, path))

		return err
	}

	t := ui.Stdout

	// Print warnings for unknown keys.
	for _, w := range warnings {
		printWarning("%s", w)
	}

	// Collect and apply CLI overrides before validation.
	cli, _, err := applyCLIOverrides(cmd, cfg)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, config.FormatConfigError(err, path))

		return err
	}

	if path != "" {
		fmt.Printf("  %s Config file %s is valid.\n", t.IconOK(), path)
	} else {
		fmt.Printf("  %s Using defaults (no config file), configuration is valid.\n", t.IconOK())
	}

	// Show non-default values.
	entries := config.EntriesWithCLI(flagConfigPath, cli)
	var nonDefault []config.Entry
	for _, e := range entries {
		if e.Source != "default" {
			nonDefault = append(nonDefault, e)
		}
	}
	if len(nonDefault) > 0 {
		fmt.Printf("\n  %s\n", t.Bold.Render("Non-default values:"))
		for _, e := range nonDefault {
			fmt.Printf("  %-40s %-15s %s\n", e.Key+":", e.Value, t.Dim.Render(fmt.Sprintf("(%s)", e.Source)))
		}
	}

	return nil
}

func runConfigReload(cmd *cobra.Command, args []string) error {
	pid, err := findServerPID()
	if err != nil {
		return fmt.Errorf("find server process: %w", err)
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process %d: %w", pid, err)
	}
	if err := proc.Signal(syscall.SIGHUP); err != nil {
		return fmt.Errorf("send SIGHUP to %d: %w", pid, err)
	}
	printSuccess("Sent SIGHUP to LynxDB server (PID %d)", pid)

	return nil
}

func runConfigGet(_ *cobra.Command, args []string) error {
	key := args[0]

	value, source, err := config.GetValue(flagConfigPath, key)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		fmt.Printf("{\"key\":%q,\"value\":%q,\"source\":%q}\n", key, value, source)

		return nil
	}

	fmt.Printf("%s %s\n", value, ui.Stdout.Dim.Render(fmt.Sprintf("(%s)", source)))

	return nil
}

func runConfigSet(_ *cobra.Command, args []string) error {
	key, value := args[0], args[1]

	// Resolve config file path.
	path := flagConfigPath
	if path == "" {
		path = config.DefaultConfigFilePath()
	}

	if configSetDryRun {
		t := ui.Stdout
		// Show what would change without applying.
		current, source, _ := config.GetValue(flagConfigPath, key)
		fmt.Printf("  %s\n", t.Bold.Render("Would change:"))
		fmt.Printf("    %s: %s → %s\n", key, current, value)
		if source != "" {
			fmt.Printf("    %s\n", t.Dim.Render(fmt.Sprintf("(current source: %s)", source)))
		}
		fmt.Printf("\n  %s\n", t.Dim.Render("Run without --dry-run to apply."))

		return nil
	}

	if err := config.SetValueInFile(path, key, value); err != nil {
		return err
	}

	printSuccess("Set %s = %s in %s", key, value, path)

	// Hint about reload if server might be running.
	printHint("Run 'lynxdb config reload' to apply changes to a running server.")

	return nil
}

func runConfigEdit(_ *cobra.Command, _ []string) error {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		return fmt.Errorf("$EDITOR is not set; set EDITOR or VISUAL environment variable")
	}

	path := flagConfigPath
	if path == "" {
		path = config.DefaultConfigFilePath()
	}

	// Create the file with defaults if it doesn't exist.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create config directory: %w", err)
		}
		if err := os.WriteFile(path, config.DefaultsTemplate, 0o600); err != nil {
			return fmt.Errorf("create config: %w", err)
		}
		printMeta("Created config template at %s", path)
	}

	// Launch editor — $EDITOR is user-controlled by design.
	cmd := exec.Command(editor, path) //nolint:gosec // $EDITOR is intentionally user-provided
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	return cmd.Run()
}

func runConfigPath(_ *cobra.Command, _ []string) error {
	path := flagConfigPath
	if path == "" {
		path = config.DefaultConfigFilePath()
	}

	fmt.Println(path)

	return nil
}

func runConfigReset(_ *cobra.Command, _ []string) error {
	path := flagConfigPath
	if path == "" {
		path = config.DefaultConfigFilePath()
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("no config file at %s", path)
	}

	if configResetDryRun {
		t := ui.Stdout
		fmt.Printf("  %s\n", t.Bold.Render("Would reset:"))
		fmt.Println(t.KeyValue("File", path))
		fmt.Println(t.KeyValue("Action", "overwrite with default values"))
		fmt.Printf("\n  %s\n", t.Dim.Render("Run without --dry-run to apply."))

		return nil
	}

	if !confirmDestructive(
		fmt.Sprintf("This will overwrite %s with default values.", path),
		"reset",
	) {
		fmt.Fprintln(os.Stderr, "  Aborted.")

		return nil
	}

	if err := os.WriteFile(path, config.DefaultsTemplate, 0o600); err != nil {
		return fmt.Errorf("write config: %w", err)
	}

	printSuccess("Reset %s to defaults", path)

	return nil
}

func runConfigAddProfile(_ *cobra.Command, args []string) error {
	name := args[0]

	if err := config.AddProfile(flagConfigPath, name, profileURL, profileToken); err != nil {
		return err
	}

	printSuccess("Added profile %q (url=%s)", name, profileURL)
	printNextSteps(
		fmt.Sprintf("lynxdb query 'level=error' --profile %s   Use this profile", name),
		"lynxdb config list-profiles                    List all profiles",
	)

	return nil
}

func runConfigListProfiles(_ *cobra.Command, _ []string) error {
	profiles, err := config.ListProfiles(flagConfigPath)
	if err != nil {
		return err
	}

	if len(profiles) == 0 {
		fmt.Println("No profiles configured.")
		printNextSteps(
			"lynxdb config add-profile <name> --url <url>   Add a profile",
		)

		return nil
	}

	if isJSONFormat() {
		for name, p := range profiles {
			fmt.Printf("{\"name\":%q,\"url\":%q}\n", name, p.URL)
		}

		return nil
	}

	// Collect and sort names for stable output.
	var names []string
	for name := range profiles {
		names = append(names, name)
	}
	sort.Strings(names)

	t := ui.Stdout
	tbl := ui.NewTable(t).SetColumns("NAME", "URL", "AUTH")
	for _, name := range names {
		p := profiles[name]
		authHint := ""
		if p.Token != "" {
			authHint = "****"
		}

		tbl.AddRow(name, p.URL, authHint)
	}
	fmt.Print(tbl.String())

	return nil
}

func runConfigRemoveProfile(_ *cobra.Command, args []string) error {
	name := args[0]

	if err := config.RemoveProfile(flagConfigPath, name); err != nil {
		return err
	}

	printSuccess("Removed profile %q", name)

	return nil
}

func findServerPID() (int, error) {
	cfg, _, err := config.Load(flagConfigPath)
	if err != nil {
		return 0, fmt.Errorf("load config: %w", err)
	}

	pidPath := config.PIDFilePath(cfg.DataDir)
	data, err := os.ReadFile(pidPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("no PID file found at %s (is the server running?)", pidPath)
		}

		return 0, fmt.Errorf("read PID file %s: %w", pidPath, err)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("invalid PID in %s: %w", pidPath, err)
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return 0, fmt.Errorf("stale PID file %s: process %d not found", pidPath, pid)
	}
	if err := proc.Signal(syscall.Signal(0)); err != nil {
		return 0, fmt.Errorf("stale PID file %s: process %d is not running", pidPath, pid)
	}

	return pid, nil
}
