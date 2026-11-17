package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
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
	flagAuthEnabled                    bool
	flagTLSEnabled                     bool
	flagTLSCert                        string
	flagTLSKey                         string
	flagProfileRuntime                 bool
	flagSyslog                         string
	flagSyslogUDP                      string
	flagSyslogTCP                      string
	flagSyslogTLS                      bool
	flagSyslogParser                   string
	flagSyslogIndex                    string
	flagIngestESEnabled                bool
	flagIngestESVersion                string
	flagOTLPHTTPListen                 string
	flagOTLPGRPCListen                 string
	flagOTLPGRPCMaxRecvBytes           string
	flagIngestMaxCompressedBodyBytes   string
	flagIngestMaxDecompressedBodyBytes string
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the LynxDB server",
	RunE:  runServer,
}

func init() {
	serverCmd.Flags().StringVar(&flagAddr, "addr", "", "Listen address (overrides config)")
	serverCmd.Flags().StringVar(&flagDataDir, "data-dir", "", "Root directory for data storage (overrides config)")
	serverCmd.Flags().StringVar(&flagLogLevel, "log-level", "", "Log level: debug, info, warn, error")
	serverCmd.Flags().BoolVar(&flagAuthEnabled, "auth", false, "Enable API key authentication")
	serverCmd.Flags().BoolVar(&flagTLSEnabled, "tls", false, "Enable TLS (auto-generates self-signed cert if no --tls-cert)")
	serverCmd.Flags().StringVar(&flagTLSCert, "tls-cert", "", "Path to TLS certificate PEM file")
	serverCmd.Flags().StringVar(&flagTLSKey, "tls-key", "", "Path to TLS private key PEM file")
	serverCmd.Flags().BoolVar(&flagProfileRuntime, "profile-runtime", false, "Enable mutex and block profiling (~2-5% overhead)")
	serverCmd.Flags().StringVar(&flagSyslog, "syslog", "", "Enable UDP and TCP syslog on address (default port 5514 when omitted)")
	serverCmd.Flags().StringVar(&flagSyslogUDP, "syslog-udp", "", "Enable UDP syslog on address")
	serverCmd.Flags().StringVar(&flagSyslogTCP, "syslog-tcp", "", "Enable TCP syslog on address")
	serverCmd.Flags().BoolVar(&flagSyslogTLS, "syslog-tls", false, "Wrap TCP syslog with server TLS (default port 6514 when omitted)")
	serverCmd.Flags().StringVar(&flagSyslogParser, "syslog-parser", "", "Syslog parser dialect: auto, rfc5424, rfc3164, raw")
	serverCmd.Flags().StringVar(&flagSyslogIndex, "syslog-index", "", "Target index for syslog events")
	serverCmd.Flags().BoolVar(&flagIngestESEnabled, "ingest-es-enabled", true, "Enable Elasticsearch-compatible bulk endpoint")
	serverCmd.Flags().StringVar(&flagIngestESVersion, "ingest-es-version", "", "ES version advertised in handshake")
	serverCmd.Flags().StringVar(&flagOTLPHTTPListen, "otlp-http-listen", "", "OTLP HTTP listen address (empty disables)")
	serverCmd.Flags().StringVar(&flagOTLPGRPCListen, "otlp-grpc-listen", "", "OTLP gRPC listen address (empty disables)")
	serverCmd.Flags().StringVar(&flagOTLPGRPCMaxRecvBytes, "otlp-grpc-max-recv-bytes", "", "OTLP gRPC max receive message size")
	serverCmd.Flags().StringVar(&flagIngestMaxCompressedBodyBytes, "ingest-max-compressed-body-bytes", "", "Max compressed shipper body size")
	serverCmd.Flags().StringVar(&flagIngestMaxDecompressedBodyBytes, "ingest-max-decompressed-body-bytes", "", "Max decompressed shipper body size")

	rootCmd.AddCommand(serverCmd)
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, cfgPath, envOverrides, warnings, err := config.LoadWithOverrides(flagConfigPath)
	if err != nil {
		return err
	}

	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", w)
	}

	_, cliOverrides, err := applyCLIOverrides(cmd, cfg)
	if err != nil {
		return err
	}

	// Apply syslog CLI flag overrides.
	if cmd.Flags().Changed("syslog") {
		addr := normalizeSyslogAddr(flagSyslog, "5514")
		cfg.Syslog.UDP = addr
		cfg.Syslog.TCP = addr
		cliOverrides = append(cliOverrides, "--syslog")
	}
	if cmd.Flags().Changed("syslog-udp") {
		cfg.Syslog.UDP = normalizeSyslogAddr(flagSyslogUDP, "5514")
		cliOverrides = append(cliOverrides, "--syslog-udp")
	}
	if cmd.Flags().Changed("syslog-tcp") {
		defaultPort := "5514"
		if cfg.Syslog.TLS || flagSyslogTLS {
			defaultPort = "6514"
		}
		cfg.Syslog.TCP = normalizeSyslogAddr(flagSyslogTCP, defaultPort)
		cliOverrides = append(cliOverrides, "--syslog-tcp")
	}
	if cmd.Flags().Changed("syslog-tls") {
		cfg.Syslog.TLS = flagSyslogTLS
		cliOverrides = append(cliOverrides, "--syslog-tls")
	}
	if cmd.Flags().Changed("syslog-parser") {
		cfg.Syslog.Parser = flagSyslogParser
		cliOverrides = append(cliOverrides, "--syslog-parser")
	}
	if cmd.Flags().Changed("syslog-index") {
		cfg.Syslog.Index = flagSyslogIndex
		cliOverrides = append(cliOverrides, "--syslog-index")
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

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

	pidPath := config.PIDFilePath(cfg.DataDir)
	if err := writePIDFile(pidPath); err != nil {
		return err
	}
	defer removePIDFile(pidPath)

	printStartupBanner(cfgPath, cfg, envOverrides, cliOverrides)

	var keyStore *auth.KeyStore

	authEnabled := flagAuthEnabled || cfg.Auth.Enabled
	if authEnabled && cfg.DataDir != "" {
		keyStore, err = bootstrapAuth(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("auth init: %w", err)
		}
	}

	var tlsCfg *tls.Config

	tlsCfg, err = bootstrapTLS(cmd, cfg)
	if err != nil {
		return fmt.Errorf("tls init: %w", err)
	}

	var levelVar slog.LevelVar
	levelVar.Set(parseLogLevel(cfg.LogLevel))
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: &levelVar}))

	if flagProfileRuntime {
		runtime.SetMutexProfileFraction(5)
		runtime.SetBlockProfileRate(1000)
		logger.Info("runtime profiling enabled", "mutex_fraction", 5, "block_rate_ns", 1000)
	}

	srv, err := rest.NewServer(rest.Config{
		Addr:          cfg.Listen,
		DataDir:       cfg.DataDir,
		Retention:     time.Duration(cfg.Retention),
		RuntimeConfig: cfg,
		KeyStore:      keyStore,
		TLSConfig:     tlsCfg,
		Logger:        logger,
		LevelVar:      &levelVar,
		Query:         cfg.Query,
		Ingest:        cfg.Ingest,
		HTTP:          cfg.HTTP,
		Syslog:        cfg.Syslog,
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
				restartRequired, reloadErr := srv.ReloadConfig(newCfg)
				if reloadErr != nil {
					logger.Error("config reload failed", "error", reloadErr)

					continue
				}
				if len(restartRequired) > 0 {
					logger.Warn("config reload left restart-required changes unapplied", "fields", restartRequired)
				}
				cfg = newCfg

				continue
			}
			logger.Info("received signal, shutting down", "signal", sig)
			cancel()

			return
		}
	}()

	printNextSteps(
		"lynxdb ingest access.log         Ingest a log file",
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

	if cfgPath != "" {
		fmt.Println(t.KeyValue("Config", cfgPath))
	} else {
		fmt.Println(t.KeyValue("Config", t.Dim.Render("(defaults)")))
	}

	var overrideNames []string
	for _, o := range envOverrides {
		overrideNames = append(overrideNames, o.Source)
	}
	overrideNames = append(overrideNames, cliFlags...)
	if len(overrideNames) > 0 {
		fmt.Println(t.KeyValue("Overrides", strings.Join(overrideNames, ", ")))
	}

	if cfg.DataDir != "" {
		fmt.Println(t.KeyValue("Data", cfg.DataDir))
	} else {
		fmt.Println(t.KeyValue("Data", t.Dim.Render("(in-memory)")))
	}

	fmt.Println(t.KeyValue("Listen", cfg.Listen))
	fmt.Println()
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

func normalizeSyslogAddr(addr, defaultPort string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ":" + defaultPort
	}
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	if strings.HasPrefix(addr, ":") {
		return addr + defaultPort
	}
	if _, err := strconv.Atoi(addr); err == nil {
		return ":" + addr
	}
	if strings.Contains(addr, ":") {
		return addr
	}
	return net.JoinHostPort(addr, defaultPort)
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
		fmt.Fprintf(os.Stderr, "  %s\n\n", t.Dim.Render("Or generate new keys:    lynxdb auth create"))
	} else {
		fmt.Fprintln(os.Stderr, ui.Stderr.KeyValue("Auth", fmt.Sprintf("enabled (%d keys)", ks.Len())))
	}

	return ks, nil
}

// bootstrapTLS sets up TLS based on CLI flags and config.
// Returns nil if TLS is not enabled.
func bootstrapTLS(cmd *cobra.Command, cfg *config.Config) (*tls.Config, error) {
	hasCertFlag := cmd.Flags().Changed("tls-cert") || cmd.Flags().Changed("tls-key")
	tlsEnabled := flagTLSEnabled || cfg.TLS.Enabled || hasCertFlag

	if !tlsEnabled {
		return nil, nil
	}

	certFile := cfg.TLS.CertFile
	keyFile := cfg.TLS.KeyFile

	if cmd.Flags().Changed("tls-cert") {
		certFile = flagTLSCert
	}
	if cmd.Flags().Changed("tls-key") {
		keyFile = flagTLSKey
	}

	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("both --tls-cert and --tls-key must be provided together")
	}

	t := ui.Stdout

	if certFile != "" && keyFile != "" {
		tlsCert, err := auth.LoadCertificate(certFile, keyFile)
		if err != nil {
			return nil, err
		}

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
