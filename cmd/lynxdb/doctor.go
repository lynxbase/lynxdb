package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
)

func init() {
	rootCmd.AddCommand(newDoctorCmd())
}

func newDoctorCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check environment and diagnose issues",
		Long:  `Runs a series of checks on the binary, config, data directory, server connectivity, and shell completion to help diagnose problems.`,
		Example: `  lynxdb doctor
  lynxdb doctor --server http://logs:3100`,
		RunE: runDoctor,
	}
}

type checkResult struct {
	name    string
	ok      bool
	warning bool
	detail  string
}

func runDoctor(_ *cobra.Command, _ []string) error {
	var results []checkResult

	results = append(results, checkBinary(), checkConfig(), checkDataDir())
	results = append(results, checkServer()...)
	results = append(results, checkCompletion())

	if isJSONFormat() {
		return printDoctorJSON(results)
	}

	return printDoctorHuman(results)
}

func checkBinary() checkResult {
	detail := fmt.Sprintf("%s (%s/%s, %s)", buildinfo.Version, runtime.GOOS, runtime.GOARCH, runtime.Version())

	return checkResult{name: "Binary", ok: true, detail: detail}
}

func checkConfig() checkResult {
	cfg, cfgPath, _, warnings, err := config.LoadWithOverrides(flagConfigPath)
	if err != nil {
		return checkResult{name: "Config", ok: false, detail: fmt.Sprintf("error: %s", err)}
	}

	_ = cfg

	if cfgPath == "" {
		return checkResult{name: "Config", ok: true, detail: "(defaults, no config file)"}
	}

	if len(warnings) > 0 {
		return checkResult{
			name:    "Config",
			ok:      true,
			warning: true,
			detail:  fmt.Sprintf("%s (%d warnings)", cfgPath, len(warnings)),
		}
	}

	return checkResult{name: "Config", ok: true, detail: fmt.Sprintf("%s (valid)", cfgPath)}
}

func checkDataDir() checkResult {
	cfg, _, err := config.Load(flagConfigPath)
	if err != nil {
		return checkResult{name: "Data dir", ok: true, detail: "(defaults, in-memory)"}
	}

	if cfg.DataDir == "" {
		return checkResult{name: "Data dir", ok: true, detail: "(in-memory, no data dir configured)"}
	}

	info, err := os.Stat(cfg.DataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return checkResult{
				name:   "Data dir",
				ok:     false,
				detail: fmt.Sprintf("%s (does not exist)", cfg.DataDir),
			}
		}

		return checkResult{name: "Data dir", ok: false, detail: fmt.Sprintf("%s (%s)", cfg.DataDir, err)}
	}

	if !info.IsDir() {
		return checkResult{name: "Data dir", ok: false, detail: fmt.Sprintf("%s (not a directory)", cfg.DataDir)}
	}

	// Report free disk space (best-effort).
	freeBytes, err := diskFreeBytes(cfg.DataDir)
	if err == nil {
		return checkResult{
			name:   "Data dir",
			ok:     true,
			detail: fmt.Sprintf("%s (%s free)", cfg.DataDir, formatBytes(int64(freeBytes))),
		}
	}

	return checkResult{name: "Data dir", ok: true, detail: cfg.DataDir}
}

func checkServer() []checkResult {
	var results []checkResult

	c := client.NewClient(
		client.WithBaseURL(globalServer),
		client.WithAuthToken(resolveToken()),
		client.WithTimeout(3*time.Second),
	)

	ctx := context.Background()

	// Check health.
	health, err := c.Health(ctx)
	if err != nil {
		results = append(results, checkResult{
			name:   "Server",
			ok:     false,
			detail: fmt.Sprintf("not running on %s", globalServer),
		})

		return results
	}

	serverDetail := fmt.Sprintf("%s (%s)", globalServer, health.Status)

	// Fetch status for uptime.
	status, statusErr := c.Status(ctx)
	if statusErr == nil && status.UptimeSeconds > 0 {
		serverDetail = fmt.Sprintf("%s (%s, uptime %s)",
			globalServer, health.Status, formatDuration(int64(status.UptimeSeconds)))
	}

	results = append(results, checkResult{
		name:   "Server",
		ok:     health.Status == "healthy",
		detail: serverDetail,
	})

	// Fetch stats for additional info.
	results = append(results, checkServerStats(c)...)

	return results
}

// checkServerStats fetches /api/v1/stats and returns checks for events, storage,
// and retention warnings.
func checkServerStats(c *client.Client) []checkResult {
	var results []checkResult

	ctx := context.Background()

	stats, err := c.Stats(ctx)
	if err != nil {
		return results
	}

	// Events total + storage usage.
	results = append(results,
		checkResult{
			name:   "Events",
			ok:     true,
			detail: fmt.Sprintf("%s total", formatCountHuman(stats.TotalEvents)),
		},
		checkResult{
			name:   "Storage",
			ok:     true,
			detail: formatBytes(stats.StorageBytes),
		},
	)

	// Retention warning: if the oldest event is near the retention boundary.
	results = append(results, checkRetention(stats.OldestEvent)...)

	return results
}

// checkRetention reports a warning if the oldest event is close to the retention
// boundary, suggesting the user may be losing data.
func checkRetention(oldestEvent string) []checkResult {
	cfg, _, err := config.Load(flagConfigPath)
	if err != nil {
		return nil
	}

	retDur := cfg.Retention.Duration()
	if retDur == 0 {
		return nil
	}

	detail := cfg.Retention.String()

	// Check if oldest event timestamp is near the retention boundary.
	if oldestEvent != "" {
		if oldest, parseErr := time.Parse(time.RFC3339Nano, oldestEvent); parseErr == nil {
			ageSecs := time.Since(oldest).Seconds()
			if ageSecs > 0 {
				ageRatio := ageSecs / retDur.Seconds()
				if ageRatio > 0.9 {
					return []checkResult{{
						name:    "Retention",
						ok:      true,
						warning: true,
						detail:  fmt.Sprintf("%s — oldest event at boundary, consider increasing", detail),
					}}
				}
			}
		}
	}

	return []checkResult{{
		name:   "Retention",
		ok:     true,
		detail: detail,
	}}
}

func checkCompletion() checkResult {
	shell := os.Getenv("SHELL")

	switch {
	case strings.Contains(shell, "bash"):
		return checkCompletionFile("bash", "lynxdb completion bash >> ~/.bashrc")
	case strings.Contains(shell, "zsh"):
		return checkCompletionFile("zsh", "lynxdb completion zsh >> ~/.zshrc")
	case strings.Contains(shell, "fish"):
		return checkCompletionFile("fish",
			"lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish")
	default:
		return checkResult{
			name:    "Completion",
			ok:      true,
			warning: true,
			detail:  fmt.Sprintf("unknown shell %q, run 'lynxdb completion --help'", shell),
		}
	}
}

func checkCompletionFile(shell, installCmd string) checkResult {
	// Check if lynxdb appears in the shell's completion system.
	// Heuristic — check if the command exists in PATH.
	_, err := exec.LookPath("lynxdb")
	if err != nil {
		return checkResult{
			name:   "Completion",
			ok:     false,
			detail: fmt.Sprintf("not installed (run: %s)", installCmd),
		}
	}

	return checkResult{
		name:   "Completion",
		ok:     true,
		detail: fmt.Sprintf("%s detected (install: %s)", shell, installCmd),
	}
}

func printDoctorHuman(results []checkResult) error {
	fmt.Fprintln(os.Stdout)

	issues := 0
	warnings := 0

	t := ui.Stdout
	for _, r := range results {
		var icon string

		switch {
		case !r.ok:
			issues++
			icon = t.IconError()
		case r.warning:
			warnings++
			icon = t.IconWarn()
		default:
			icon = t.IconOK()
		}

		fmt.Fprintf(os.Stdout, "  %s %s  %s\n", icon, t.Bold.Render(fmt.Sprintf("%-12s", r.name)), r.detail)
	}

	fmt.Fprintln(os.Stdout)

	switch {
	case issues > 0:
		summary := fmt.Sprintf("%d issue(s) found.", issues)
		if warnings > 0 {
			summary += fmt.Sprintf(" %d warning(s).", warnings)
		}

		fmt.Fprintf(os.Stdout, "  %s %s\n", t.IconError(), t.Error.Render(summary))

		// Suggest next action when server is not running.
		for _, r := range results {
			if r.name == "Server" && !r.ok {
				fmt.Fprintf(os.Stdout, "  %s\n", t.Dim.Render("Run 'lynxdb server' to start."))

				break
			}
		}
	case warnings > 0:
		fmt.Fprintf(os.Stdout, "  %s %s\n", t.IconWarn(), t.Warning.Render(fmt.Sprintf("All checks passed (%d warning(s)).", warnings)))
	default:
		printSuccess("All checks passed.")
	}

	fmt.Fprintln(os.Stdout)

	if issues > 0 {
		return fmt.Errorf("%d check(s) failed", issues)
	}

	return nil
}

func printDoctorJSON(results []checkResult) error {
	type jsonCheck struct {
		Name    string `json:"name"`
		OK      bool   `json:"ok"`
		Warning bool   `json:"warning"`
		Detail  string `json:"detail"`
	}

	out := make([]jsonCheck, len(results))
	for i, r := range results {
		out[i] = jsonCheck{
			Name:    r.name,
			OK:      r.ok,
			Warning: r.warning,
			Detail:  r.detail,
		}
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("doctor.MarshalJSON: %w", err)
	}

	fmt.Println(string(b))

	return nil
}
