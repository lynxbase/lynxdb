package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/client"
)

var shipperConfigRemote string

//go:embed shippers/fixtures/*
var shipperConfigFS embed.FS

type shipperTemplateData struct {
	Remote string
}

var shipperConfigTemplatePaths = map[string]string{
	"filebeat":   "shippers/fixtures/filebeat.yml",
	"fluent-bit": "shippers/fixtures/fluentbit.conf",
	"vector":     "shippers/fixtures/vector.yaml",
	"otelcol":    "shippers/fixtures/otelcol.yaml",
	"splunk-hec": "shippers/fixtures/splunk-hec.txt",
}

func init() {
	rootCmd.AddCommand(newShippersCmd())
}

func newShippersCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "shippers",
		Short: "Inspect and configure log shippers",
		RunE:  runShippersList,
	}
	configCmd := &cobra.Command{
		Use:   "config <tool>",
		Short: "Print a copy-pasteable shipper config",
		Args:  cobra.ExactArgs(1),
		RunE:  runShippersConfig,
	}
	configCmd.Flags().StringVar(&shipperConfigRemote, "remote", "", "LynxDB endpoint to render into the config")

	testCmd := &cobra.Command{
		Use:   "test <tool>",
		Short: "Send one synthetic event through a shipper-compatible endpoint",
		Args:  cobra.ExactArgs(1),
		RunE:  runShippersTest,
	}
	testCmd.Flags().StringVar(&shipperConfigRemote, "remote", "", "LynxDB endpoint to test")
	cmd.AddCommand(configCmd, testCmd)
	return cmd
}

func runShippersList(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	shippers, err := apiClient().Shippers(ctx)
	if err != nil {
		return err
	}

	if isJSONFormat() {
		b, _ := json.MarshalIndent(shippers, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	if len(shippers) == 0 {
		fmt.Fprintln(os.Stdout, "No shippers observed yet.")
		return nil
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "TOOL\tVERSION\tSTATUS\tLAST SEEN\tEVENTS/MIN\tENDPOINT")
	for _, s := range shippers {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			s.Tool,
			emptyDash(s.Version),
			s.Status,
			formatShipperLastSeen(s.LastSeenAt),
			formatCountHuman(s.EventsPerMin),
			s.Endpoint,
		)
	}
	return tw.Flush()
}

func runShippersConfig(_ *cobra.Command, args []string) error {
	tool := normalizeShipperTool(args[0])
	path, ok := shipperConfigTemplatePaths[tool]
	if !ok {
		return fmt.Errorf("unknown shipper %q. Use one of: filebeat, fluent-bit, vector, otelcol, splunk-hec", args[0])
	}
	tmpl, err := shipperConfigFS.ReadFile(path)
	if err != nil {
		return err
	}

	remote := shipperConfigRemote
	if remote == "" {
		remote = globalServer
	}
	out, err := renderShipperConfig(string(tmpl), shipperTemplateData{Remote: strings.TrimRight(remote, "/")})
	if err != nil {
		return err
	}
	fmt.Print(out)
	return nil
}

func runShippersTest(_ *cobra.Command, args []string) error {
	tool := normalizeShipperTool(args[0])
	if _, ok := shipperConfigTemplatePaths[tool]; !ok {
		return fmt.Errorf("unknown shipper %q. Use one of: filebeat, fluent-bit, vector, otelcol, splunk-hec", args[0])
	}
	remote := shipperConfigRemote
	if remote == "" {
		remote = globalServer
	}
	remote = strings.TrimRight(remote, "/")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	marker := fmt.Sprintf("lynxdb-shipper-test-%d", time.Now().UnixNano())
	if err := sendSyntheticShipperEvent(ctx, remote, tool, marker); err != nil {
		return err
	}
	if err := waitForSyntheticEvent(ctx, remote, marker); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "OK %s roundtrip succeeded\n", tool)
	return nil
}

func renderShipperConfig(tmpl string, data shipperTemplateData) (string, error) {
	t, err := template.New("shipper").Funcs(template.FuncMap{
		"host": templateHost,
		"port": templatePort,
	}).Parse(tmpl)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func normalizeShipperTool(tool string) string {
	switch strings.ToLower(strings.TrimSpace(tool)) {
	case "fluentbit", "fluent-bit":
		return "fluent-bit"
	case "otel", "otelcol", "otel-collector", "opentelemetry-collector":
		return "otelcol"
	case "splunk", "splunk-hec", "hec":
		return "splunk-hec"
	default:
		return strings.ToLower(strings.TrimSpace(tool))
	}
}

func formatShipperLastSeen(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	age := time.Since(t)
	switch {
	case age < time.Minute:
		return fmt.Sprintf("%ds ago", int(age.Seconds()))
	case age < time.Hour:
		return fmt.Sprintf("%dm ago", int(age.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(age.Hours()))
	}
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func templateHost(remote string) string {
	hostPort := strings.TrimPrefix(strings.TrimPrefix(remote, "http://"), "https://")
	if i := strings.IndexByte(hostPort, '/'); i >= 0 {
		hostPort = hostPort[:i]
	}
	if i := strings.LastIndexByte(hostPort, ':'); i >= 0 {
		return hostPort[:i]
	}
	return hostPort
}

func templatePort(remote string) string {
	hostPort := strings.TrimPrefix(strings.TrimPrefix(remote, "http://"), "https://")
	if i := strings.IndexByte(hostPort, '/'); i >= 0 {
		hostPort = hostPort[:i]
	}
	if i := strings.LastIndexByte(hostPort, ':'); i >= 0 {
		return hostPort[i+1:]
	}
	if strings.HasPrefix(remote, "https://") {
		return "443"
	}
	return "80"
}

func sendSyntheticShipperEvent(ctx context.Context, remote, tool, marker string) error {
	var path, contentType, userAgent, auth string
	var body string
	switch tool {
	case "filebeat":
		path = "/_bulk"
		contentType = "application/x-ndjson"
		userAgent = "Filebeat/8.15.0"
		body = bulkSyntheticBody("filebeat", marker)
	case "fluent-bit":
		path = "/_bulk"
		contentType = "application/x-ndjson"
		userAgent = "Fluent-Bit v3.1.4"
		body = bulkSyntheticBody("fluent-bit", marker)
	case "vector":
		path = "/_bulk"
		contentType = "application/x-ndjson"
		userAgent = "Vector/0.40.0"
		body = bulkSyntheticBody("vector", marker)
	case "otelcol":
		path = "/api/v1/otlp/v1/logs"
		contentType = "application/json"
		userAgent = "opentelemetry-collector-contrib/0.105.0"
		body = fmt.Sprintf(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":{"stringValue":%q}}]}]}]}`, marker)
	case "splunk-hec":
		path = "/services/collector/event"
		contentType = "application/json"
		auth = "Splunk synthetic"
		body = fmt.Sprintf(`{"event":%q,"source":"splunk-hec"}`, marker)
	default:
		return fmt.Errorf("unsupported shipper %q", tool)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, remote+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	} else if token := resolveToken(); token != "" && strings.HasPrefix(path, "/api/") {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s synthetic ingest failed: status %d: %s", tool, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func waitForSyntheticEvent(ctx context.Context, remote, marker string) error {
	c := client.NewClient(
		client.WithBaseURL(remote),
		client.WithAuthToken(resolveToken()),
	)
	query := fmt.Sprintf("FROM main | search %q | head 1", marker)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		result, err := c.QuerySync(ctx, query, "", "")
		if err == nil && result.Events != nil && len(result.Events.Events) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("synthetic event was not query-visible before timeout")
		case <-ticker.C:
		}
	}
}

func bulkSyntheticBody(source, marker string) string {
	return fmt.Sprintf("{\"index\":{\"_index\":%q}}\n{\"message\":%q}\n", source, marker)
}
