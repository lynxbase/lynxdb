package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
)

type shipperDoctorReport struct {
	Listeners []shipperListenerCheck      `json:"listeners"`
	Recent    []client.ShipperObservation `json:"recent"`
	Warnings  []string                    `json:"warnings,omitempty"`
}

type shipperListenerCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type shipperDoctorContext struct {
	Metrics             map[string]float64
	StagingMaxBytes     float64
	ESAdvertisedVersion string
}

func newDoctorShippersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shippers",
		Short: "Diagnose log shipper compatibility",
		RunE:  runDoctorShippers,
	}
}

func runDoctorShippers(_ *cobra.Command, _ []string) error {
	ctx := context.Background()
	c := apiClient()

	listeners, metrics, err := fetchShipperMetrics(ctx)
	if err != nil {
		return formatShipperDoctorConnectError(err)
	}
	recent, err := c.Shippers(ctx)
	if err != nil {
		return err
	}
	reportCtx := shipperDoctorContext{Metrics: metrics}
	if cfg, err := c.GetConfig(ctx); err == nil {
		reportCtx.StagingMaxBytes = configByteSize(cfg, "ingest", "staging", "max_bytes")
		reportCtx.ESAdvertisedVersion = configString(cfg, "ingest", "es_compat", "advertised_version")
	}
	report := buildShipperDoctorReport(listeners, recent, reportCtx)

	if isJSONFormat() {
		b, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	printShipperDoctorReport(report)
	return nil
}

func buildShipperDoctorReport(listeners []shipperListenerCheck, recent []client.ShipperObservation, ctx shipperDoctorContext) shipperDoctorReport {
	report := shipperDoctorReport{Listeners: listeners, Recent: recent}
	if len(recent) == 0 {
		report.Warnings = append(report.Warnings, "no shipper traffic observed yet")
	}

	var sawHEC bool
	for _, s := range recent {
		if s.Tool == "filebeat" && strings.HasPrefix(s.Version, "8.") && majorVersion(ctx.ESAdvertisedVersion) > 0 && majorVersion(ctx.ESAdvertisedVersion) < 8 {
			report.Warnings = append(report.Warnings, `Filebeat 8.x may reject advertised Elasticsearch version `+ctx.ESAdvertisedVersion+`: set allow_older_versions: true or ingest.es_compat.advertised_version: "8.15.0"`)
		}
		if s.Tool == "splunk-hec" || strings.HasPrefix(s.Endpoint, "/services/collector") {
			sawHEC = true
		}
	}
	if len(recent) > 0 && !sawHEC {
		report.Warnings = append(report.Warnings, "Splunk HEC has not been exercised recently")
	}
	if ctx.StagingMaxBytes > 0 {
		stagingBytes := ctx.Metrics["lynxdb_ingest_staging_bytes"]
		ratio := stagingBytes / ctx.StagingMaxBytes
		if ratio >= 0.95 {
			report.Warnings = append(report.Warnings, fmt.Sprintf("staging buffer %.0f%% full - ingest is under severe backpressure", ratio*100))
		} else if ratio >= 0.80 {
			report.Warnings = append(report.Warnings, fmt.Sprintf("staging buffer %.0f%% full - investigate downstream ingest latency", ratio*100))
		}
	}
	return report
}

func printShipperDoctorReport(report shipperDoctorReport) {
	fmt.Fprintln(os.Stdout, "LynxDB doctor - shipper diagnostics")
	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Listener health:")
	for _, l := range report.Listeners {
		fmt.Fprintf(os.Stdout, "  %-10s %s\n", l.Name, l.Status)
	}

	fmt.Fprintln(os.Stdout)
	fmt.Fprintln(os.Stdout, "Recent ingest:")
	if len(report.Recent) == 0 {
		fmt.Fprintln(os.Stdout, "  none")
	} else {
		for _, s := range report.Recent {
			fmt.Fprintf(os.Stdout, "  %-12s %-8s %-8s %-8s %s\n",
				shipperNameVersion(s),
				s.Status,
				formatShipperLastSeen(s.LastSeenAt),
				formatCountHuman(s.EventsPerMin)+"/min",
				s.Endpoint,
			)
		}
	}

	if len(report.Warnings) > 0 {
		fmt.Fprintln(os.Stdout)
		fmt.Fprintln(os.Stdout, "Diagnostics:")
		for _, w := range report.Warnings {
			fmt.Fprintf(os.Stdout, "  WARN %s\n", w)
		}
	}
}

func fetchShipperMetrics(ctx context.Context) ([]shipperListenerCheck, map[string]float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(globalServer, "/")+"/metrics", nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return nil, nil, fmt.Errorf("metrics request failed: status %d", resp.StatusCode)
	}

	listeners := map[string]float64{}
	metrics := map[string]float64{}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		name := metricName(line)
		metrics[name] = value
		if strings.HasPrefix(line, "lynxdb_ingest_listener_up{") {
			listener := metricLabel(line, "listener")
			if listener != "" {
				listeners[listener] = value
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return []shipperListenerCheck{
		{Name: "ES bulk", Status: boundStatus(listeners["es"] > 0)},
		{Name: "OTLP HTTP", Status: boundStatus(listeners["otlp_http"] > 0)},
		{Name: "OTLP gRPC", Status: boundStatus(listeners["otlp_grpc"] > 0)},
	}, metrics, nil
}

func fetchShipperListenerChecks(ctx context.Context) ([]shipperListenerCheck, error) {
	listeners, _, err := fetchShipperMetrics(ctx)
	return listeners, err
}

func formatShipperDoctorConnectError(err error) error {
	msg := err.Error()
	if strings.Contains(msg, "server gave HTTP response to HTTPS client") {
		return fmt.Errorf("metrics request failed: LynxDB appears to be serving plain HTTP; use http:// for --server or terminate TLS in front of LynxDB")
	}
	return err
}

func metricName(line string) string {
	end := strings.IndexAny(line, "{ \t")
	if end < 0 {
		return line
	}
	return line[:end]
}

func metricLabel(line, name string) string {
	needle := name + `="`
	start := strings.Index(line, needle)
	if start < 0 {
		return ""
	}
	start += len(needle)
	end := strings.IndexByte(line[start:], '"')
	if end < 0 {
		return ""
	}
	return line[start : start+end]
}

func boundStatus(bound bool) string {
	if bound {
		return "bound"
	}
	return "not bound"
}

func majorVersion(version string) int {
	if version == "" {
		return 0
	}
	head, _, _ := strings.Cut(version, ".")
	n, _ := strconv.Atoi(head)
	return n
}

func configString(cfg client.ConfigResult, path ...string) string {
	v := nestedConfigValue(map[string]interface{}(cfg), path...)
	s, _ := v.(string)
	return s
}

func configByteSize(cfg client.ConfigResult, path ...string) float64 {
	v := nestedConfigValue(map[string]interface{}(cfg), path...)
	switch v := v.(type) {
	case string:
		b, err := config.ParseByteSize(v)
		if err != nil {
			return 0
		}
		return float64(b)
	case float64:
		return v
	default:
		return 0
	}
}

func nestedConfigValue(m map[string]interface{}, path ...string) interface{} {
	var current interface{} = m
	for _, key := range path {
		next, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current = next[key]
	}
	return current
}

func shipperNameVersion(s client.ShipperObservation) string {
	if s.Version == "" {
		return s.Tool
	}
	return s.Tool + "/" + s.Version
}
