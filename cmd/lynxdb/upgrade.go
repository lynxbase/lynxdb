package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/upgrade"
)

func init() {
	rootCmd.AddCommand(newUpgradeCmd())
}

func newUpgradeCmd() *cobra.Command {
	var (
		flagCheck   bool
		flagVersion string
		flagForce   bool
		flagYes     bool
	)

	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade LynxDB to the latest version",
		Long: `Check for and install LynxDB updates.

By default, upgrades to the latest stable release. Use --version to install
a specific version, or --check to check without installing.`,
		Example: `  lynxdb upgrade              # Upgrade to latest
  lynxdb upgrade --check      # Check only, don't install
  lynxdb upgrade --version v0.6.0  # Install specific version
  lynxdb upgrade --yes         # Skip confirmation prompt`,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runUpgrade(flagCheck, flagVersion, flagForce, flagYes)
		},
	}

	cmd.Flags().BoolVar(&flagCheck, "check", false, "Check for updates without installing")
	cmd.Flags().StringVar(&flagVersion, "version", "", "Install a specific version (e.g., v0.6.0)")
	cmd.Flags().BoolVar(&flagForce, "force", false, "Skip 'already up to date' check")
	cmd.Flags().BoolVar(&flagYes, "yes", false, "Skip confirmation prompt")

	return cmd
}

func runUpgrade(check bool, version string, force, yes bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Dev guard: warn when running a development build.
	if buildinfo.IsDev() && !force {
		printWarning("Development build detected. Use 'go install' to update, or pass --force to override.")
		return nil
	}

	printMeta("Checking for updates...")

	var result *upgrade.CheckResult
	var err error

	if version != "" {
		// Specific version requested — fetch the versioned manifest.
		manifest, fetchErr := upgrade.FetchVersionedManifest(ctx, version)
		if fetchErr != nil {
			return fmt.Errorf("failed to fetch version info: %w", fetchErr)
		}
		result, err = upgrade.CheckAgainstManifest(manifest, buildinfo.Version)
		if err != nil {
			if errors.Is(err, upgrade.ErrPlatformNotFound) {
				return fmt.Errorf("no build for %s/%s in %s. Check available builds at https://github.com/lynxbase/lynxdb/releases/%s",
					runtime.GOOS, runtime.GOARCH, version, version)
			}
			return err
		}
		// Override: if a specific version was requested with --version, always
		// consider it an update if versions differ (even downgrade).
		if result.LatestVersion != buildinfo.Version {
			result.UpdateAvail = true
			// Re-fetch the artifact for this platform.
			if result.Artifact == nil {
				key := upgrade.PlatformKey()
				if a, ok := manifest.Artifacts[key]; ok {
					result.Artifact = &a
				}
			}
		}
	} else {
		result, err = upgrade.Check(ctx, buildinfo.Version)
		if err != nil {
			if errors.Is(err, upgrade.ErrPlatformNotFound) {
				return fmt.Errorf("no build for %s/%s. Check available builds at https://github.com/lynxbase/lynxdb/releases",
					runtime.GOOS, runtime.GOARCH)
			}
			return fmt.Errorf("update check failed: %w", err)
		}
	}

	if !result.UpdateAvail && !force {
		printSuccess("Already up to date (%s)", buildinfo.Version)
		return nil
	}

	if check {
		if result.UpdateAvail {
			printSuccess("Update available: %s -> %s", result.CurrentVersion, result.LatestVersion)
			if result.ChangelogURL != "" {
				printMeta("Changelog: %s", result.ChangelogURL)
			}
			for _, notice := range result.Notices {
				printWarning("%s", notice)
			}
			// Exit code 1 = update available (for scripting).
			os.Exit(1)
		}
		printSuccess("Already up to date (%s)", buildinfo.Version)
		return nil
	}

	for _, notice := range result.Notices {
		printWarning("%s", notice)
	}

	targetVersion := result.LatestVersion
	if version != "" {
		targetVersion = version
	}

	if isTTY() && !yes {
		msg := fmt.Sprintf("Upgrade LynxDB %s -> %s?", buildinfo.Version, targetVersion)
		if !confirmAction(msg) {
			printHint("Aborted.")
			os.Exit(exitAborted)
		}
	}

	if result.Artifact == nil {
		return fmt.Errorf("no artifact available for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	printMeta("Downloading lynxdb %s for %s/%s...", targetVersion, runtime.GOOS, runtime.GOARCH)

	var lastPercent int
	progressFn := func(downloaded, total int64) {
		if total > 0 {
			percent := int(downloaded * 100 / total)
			if percent != lastPercent && percent%10 == 0 {
				printMeta("  %s / %s (%d%%)", formatBytes(downloaded), formatBytes(total), percent)
				lastPercent = percent
			}
		}
	}

	archivePath, err := upgrade.DownloadWithProgress(ctx, result.Artifact, progressFn)
	if err != nil {
		if errors.Is(err, upgrade.ErrChecksumMismatch) {
			printWarning("Downloaded file checksum does not match. The file may be corrupted or tampered with.")
			printWarning("Please try again or download manually from GitHub.")
			os.Exit(1)
		}
		return fmt.Errorf("download failed: %w", err)
	}
	defer os.Remove(archivePath)

	printSuccess("Checksum verified")

	if err := upgrade.Install(archivePath); err != nil {
		if os.IsPermission(err) {
			return fmt.Errorf("permission denied. Try: sudo lynxdb upgrade")
		}
		return fmt.Errorf("installation failed: %w", err)
	}

	printSuccess("Upgraded: %s -> %s", buildinfo.Version, targetVersion)
	printNextSteps("Restart any running lynxdb processes")

	return nil
}
