package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/auth"
	"github.com/lynxbase/lynxdb/pkg/config"
)

var (
	flagAuthEnabled  bool
	flagTLSEnabled   bool
	flagTLSCert      string
	flagTLSKey       string
	flagMaxQueryPool string
	flagSpillDir     string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the LynxDB server",
	RunE:  runServer,
}

func init() {
	serverCmd.Flags().StringVar(&flagAddr, "addr", "", "Listen address (overrides config)")
	serverCmd.Flags().StringVar(&flagDataDir, "data-dir", "", "Root directory for data storage (overrides config)")
	serverCmd.Flags().StringVar(&flagS3Bucket, "s3-bucket", "", "S3 bucket for warm/cold storage")
	serverCmd.Flags().StringVar(&flagS3Region, "s3-region", "", "AWS region")
	serverCmd.Flags().StringVar(&flagS3Prefix, "s3-prefix", "", "Key prefix in S3")
	serverCmd.Flags().StringVar(&flagCompactionInterval, "compaction-interval", "", "Compaction check interval")
	serverCmd.Flags().StringVar(&flagTieringInterval, "tiering-interval", "", "Tier evaluation interval")
	serverCmd.Flags().StringVar(&flagCacheMaxMB, "cache-max-mb", "", "Max cache size (e.g. 1gb, 512mb)")
	serverCmd.Flags().StringVar(&flagLogLevel, "log-level", "", "Log level: debug, info, warn, error")
	serverCmd.Flags().BoolVar(&flagAuthEnabled, "auth", false, "Enable API key authentication")
	serverCmd.Flags().BoolVar(&flagTLSEnabled, "tls", false, "Enable TLS (auto-generates self-signed cert if no --tls-cert)")
	serverCmd.Flags().StringVar(&flagTLSCert, "tls-cert", "", "Path to TLS certificate PEM file")
	serverCmd.Flags().StringVar(&flagTLSKey, "tls-key", "", "Path to TLS private key PEM file")
	serverCmd.Flags().StringVar(&flagMaxQueryPool, "max-query-pool", "", "Global query memory pool (e.g., 2gb, 4gb)")
	serverCmd.Flags().StringVar(&flagSpillDir, "spill-dir", "", "Directory for temporary spill files (default: OS temp dir)")

	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, cfgPath, envOverrides, warnings, err := config.LoadWithOverrides(flagConfigPath)
	if err != nil {
		return err
	}

	// Print warnings for unknown keys.
	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", w)
	}

	// Apply CLI flag overrides (only when explicitly set).
	_, cliOverrides, err := applyCLIOverrides(cmd, cfg)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	// Track auth/TLS flags in the overrides list for the startup banner.
	if cmd.Flags().Changed("auth") {
		cliOverrides = append(cliOverrides, "--auth")
	}
	if cmd.Flags().Changed("tls") {
		cliOverrides = append(cliOverrides, "--tls")
	}
	if cmd.Flags().Changed("tls-cert") {
		cliOverrides = append(cliOverrides, "--tls-cert")
	}
	if cmd.Flags().Changed("tls-key") {
		cliOverrides = append(cliOverrides, "--tls-key")
	}

	// Write PID file so "config reload" can find us.
	pidPath := config.PIDFilePath(cfg.DataDir)
	if err := writePIDFile(pidPath); err != nil {
		return err
	}
	defer removePIDFile(pidPath)

	// Print startup banner to stdout.
	printStartupBanner(cfgPath, cfg, envOverrides, cliOverrides)

	// Bootstrap auth if --auth flag or config is set.
	var keyStore *auth.KeyStore

	authEnabled := flagAuthEnabled || cfg.Auth.Enabled
	if authEnabled && cfg.DataDir != "" {
		keyStore, err = bootstrapAuth(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("auth init: %w", err)
		}
	}

	// Bootstrap TLS if --tls flag, --tls-cert/--tls-key, or config is set.
	var tlsCfg *tls.Config

	tlsCfg, err = bootstrapTLS(cmd, cfg)
	if err != nil {
		return fmt.Errorf("tls init: %w", err)
	}

	// Set up slog with LevelVar for hot-reloadable log level.
	var levelVar slog.LevelVar
	levelVar.Set(parseLogLevel(cfg.LogLevel))
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &levelVar}))

	srv, err := rest.NewServer(rest.Config{
		Addr:          cfg.Listen,
		DataDir:       cfg.DataDir,
		Retention:     time.Duration(cfg.Retention),
		KeyStore:      keyStore,
		TLSConfig:     tlsCfg,
		Storage:       cfg.Storage,
		Logger:        logger,
		Query:         cfg.Query,
		Ingest:        cfg.Ingest,
		HTTP:          cfg.HTTP,
		Server:        cfg.Server,
		Views:         cfg.Views,
		BufferManager: cfg.BufferManager,
	})
	if err != nil {
		return fmt.Errorf("server init: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range sigCh {
			if sig == syscall.SIGHUP {
				logger.Info("SIGHUP received, reloading config")
				newCfg, _, reloadErr := config.Load(flagConfigPath)
				if reloadErr != nil {
					logger.Error("config reload failed", "error", reloadErr)

					continue
				}
				if reloadErr := newCfg.Validate(); reloadErr != nil {
					logger.Error("config reload validation failed", "error", reloadErr)

					continue
				}
				applyHotReload(logger, cfg, newCfg, &levelVar, srv)
				cfg = newCfg

				continue
			}
			logger.Info("received signal, shutting down", "signal", sig)
			cancel()

			return
		}
	}()

	scheme := "http"
	if tlsCfg != nil {
		scheme = "https"
	}

	printNextSteps(
		"lynxdb demo                      Generate sample data",
		"lynxdb ingest access.log         Ingest a log file",
		fmt.Sprintf("open %s://%s\t   Web UI", scheme, cfg.Listen),
	)

	logAttrs := []any{"version", buildinfo.Version, "addr", cfg.Listen}
	if cfgPath != "" {
		logAttrs = append(logAttrs, "config", cfgPath)
	}
	if tlsCfg != nil {
		logAttrs = append(logAttrs, "tls", true)
	}
	if authEnabled {
		logAttrs = append(logAttrs, "auth", true)
	}
	logger.Info("starting LynxDB", logAttrs...)

	return srv.Start(ctx)
}

func writePIDFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create PID file directory: %w", err)
	}

	return os.WriteFile(path, []byte(strconv.Itoa(os.Getpid())), 0o600)
}

func removePIDFile(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("failed to remove PID file", "path", path, "error", err)
	}
}

func printStartupBanner(cfgPath string, cfg *config.Config, envOverrides []config.Override, cliFlags []string) {
	t := ui.Stdout

	// Config source.
	if cfgPath != "" {
		fmt.Println(t.KeyValue("Config", cfgPath))
	} else {
		fmt.Println(t.KeyValue("Config", t.Dim.Render("(defaults)")))
	}

	// Collect all override names.
	var overrideNames []string
	for _, o := range envOverrides {
		overrideNames = append(overrideNames, o.Source)
	}
	overrideNames = append(overrideNames, cliFlags...)
	if len(overrideNames) > 0 {
		fmt.Println(t.KeyValue("Overrides", strings.Join(overrideNames, ", ")))
	}

	// Data dir.
	if cfg.DataDir != "" {
		fmt.Println(t.KeyValue("Data", cfg.DataDir))
	} else {
		fmt.Println(t.KeyValue("Data", t.Dim.Render("(in-memory)")))
	}

	// Listen address.
	fmt.Println(t.KeyValue("Listen", cfg.Listen))
	fmt.Println()
}

func applyHotReload(logger *slog.Logger, old, updated *config.Config, levelVar *slog.LevelVar, srv *rest.Server) {
	// Log level — hot reloadable.
	if old.LogLevel != updated.LogLevel {
		levelVar.Set(parseLogLevel(updated.LogLevel))
		logger.Info("reloaded log_level", "old", old.LogLevel, "new", updated.LogLevel)
	}

	// Engine hot-reloadable fields (max_concurrent, query config).
	srv.Engine().ReloadConfig(updated)

	// Retention — hot reloadable.
	if old.Retention != updated.Retention {
		logger.Info("reloaded retention", "old", old.Retention.String(), "new", updated.Retention.String())
	}

	// Warn about restart-required fields.
	if old.Listen != updated.Listen {
		logger.Warn("listen changed, restart required", "old", old.Listen, "new", updated.Listen)
	}
	if old.DataDir != updated.DataDir {
		logger.Warn("data_dir changed, restart required", "old", old.DataDir, "new", updated.DataDir)
	}
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// bootstrapAuth opens the key store and generates a root key if none exist.
func bootstrapAuth(dataDir string) (*auth.KeyStore, error) {
	authDir := filepath.Join(dataDir, "auth")
	if err := os.MkdirAll(authDir, 0o700); err != nil {
		return nil, fmt.Errorf("create auth dir: %w", err)
	}

	ks, err := auth.OpenKeyStore(authDir)
	if err != nil {
		return nil, err
	}

	if ks.IsEmpty() {
		created, createErr := ks.CreateKey("root", true)
		if createErr != nil {
			return nil, fmt.Errorf("generate root key: %w", createErr)
		}

		t := ui.Stderr
		fmt.Fprintln(os.Stderr)
		t.PrintWarning(false, "Auth enabled — no API keys exist. Generated root key:")
		fmt.Fprintf(os.Stderr, "\n    %s\n\n", t.Bold.Render(created.Token))
		fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render("Save this key now. It will NOT be shown again."))
		fmt.Fprintf(os.Stderr, "  %s\n", t.Dim.Render("Use it to authenticate:  lynxdb login"))
		fmt.Fprintf(os.Stderr, "  %s\n\n", t.Dim.Render("Or generate new keys:    lynxdb auth create-key"))
	} else {
		fmt.Fprintln(os.Stderr, ui.Stderr.KeyValue("Auth", fmt.Sprintf("enabled (%d keys)", ks.Len())))
	}

	return ks, nil
}

// bootstrapTLS sets up TLS based on CLI flags and config.
// Returns nil if TLS is not enabled.
func bootstrapTLS(cmd *cobra.Command, cfg *config.Config) (*tls.Config, error) {
	// Determine if TLS is requested via flags or config.
	hasCertFlag := cmd.Flags().Changed("tls-cert") || cmd.Flags().Changed("tls-key")
	tlsEnabled := flagTLSEnabled || cfg.TLS.Enabled || hasCertFlag

	if !tlsEnabled {
		return nil, nil
	}

	// Resolve cert and key file paths: CLI flags override config.
	certFile := cfg.TLS.CertFile
	keyFile := cfg.TLS.KeyFile

	if cmd.Flags().Changed("tls-cert") {
		certFile = flagTLSCert
	}
	if cmd.Flags().Changed("tls-key") {
		keyFile = flagTLSKey
	}

	// Validate: both or neither must be set.
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}

	t := ui.Stdout

	if certFile != "" && keyFile != "" {
		// Bring-your-own cert.
		tlsCert, err := auth.LoadCertificate(certFile, keyFile)
		if err != nil {
			return nil, err
		}

		// Parse leaf for fingerprint display.
		leaf, parseErr := parseCertLeaf(&tlsCert)
		if parseErr != nil {
			return nil, parseErr
		}

		fp := auth.CertFingerprint(leaf)
		fmt.Println(t.KeyValue("TLS", certFile))
		fmt.Println(t.KeyValue("Fingerprint", fp))

		return &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
			MinVersion:   tls.VersionTLS12,
		}, nil
	}

	// Auto-generate self-signed cert.
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("--tls requires --data-dir (needed to persist self-signed certificate)")
	}

	tlsCert, fp, err := auth.LoadOrGenerateCert(cfg.DataDir)
	if err != nil {
		return nil, err
	}

	fmt.Println(t.KeyValue("TLS", "self-signed"))
	fmt.Println(t.KeyValue("Fingerprint", fp))
	fmt.Println(t.KeyValue("Certificate", filepath.Join(cfg.DataDir, "tls", "server.crt")))

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// parseCertLeaf parses the leaf certificate from a tls.Certificate.
func parseCertLeaf(cert *tls.Certificate) (*x509.Certificate, error) {
	if cert.Leaf != nil {
		return cert.Leaf, nil
	}

	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("tls: certificate chain is empty")
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("tls: parse leaf certificate: %w", err)
	}

	cert.Leaf = leaf

	return leaf, nil
}
