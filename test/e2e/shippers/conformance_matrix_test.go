//go:build e2e && e2e_matrix

package shippers

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type conformanceCell struct {
	Name string
	Skip string
	Run  func(*testing.T)
}

func TestE2E_ConformanceMatrix(t *testing.T) {
	for _, cell := range conformanceCells() {
		cell := cell
		t.Run(cell.Name, func(t *testing.T) {
			if cell.Skip != "" {
				t.Skip(cell.Skip)
			}
			cell.Run(t)
		})
	}
}

func conformanceCells() []conformanceCell {
	return []conformanceCell{
		{Name: "filebeat/8.15/none", Run: runMatrixFilebeat("docker.elastic.co/beats/filebeat:8.15.0")},
		{Name: "fluent-bit/3.x/gzip", Run: runMatrixFluentBit("cr.fluentbit.io/fluent/fluent-bit:3.1", false)},
		{Name: "fluent-bit/3.x/logstash_format", Run: runMatrixFluentBit("cr.fluentbit.io/fluent/fluent-bit:3.1", true)},
		{Name: "vector/0.40/zstd_bulk", Run: runMatrixVector("timberio/vector:0.40.0-alpine", "bulk")},
		{Name: "vector/0.40/zstd_data_stream", Run: runMatrixVector("timberio/vector:0.40.0-alpine", "data_stream")},
		{Name: "otelcol/0.105/http_proto_gzip", Run: runMatrixOtelColHTTP("otel/opentelemetry-collector-contrib:0.105.0")},
		{Name: "otelcol/0.105/grpc_gzip", Run: runMatrixOtelColGRPC("otel/opentelemetry-collector-contrib:0.105.0")},
		{Name: "splunk-hec/curl/ack", Run: runMatrixSplunkHEC()},

		{Name: "filebeat/7.17/none", Skip: "quarantined: legacy version cell not yet promoted to nightly enforcement"},
		{Name: "filebeat/8.10/none", Skip: "quarantined: intermediate version cell not yet promoted to nightly enforcement"},
		{Name: "filebeat/9.0/none", Skip: "quarantined: image availability varies by registry state"},
		{Name: "fluent-bit/2.x/gzip", Skip: "quarantined: legacy version cell not yet promoted to nightly enforcement"},
		{Name: "vector/0.30/zstd_bulk", Skip: "quarantined: legacy sink config shape differs from current fixture"},
		{Name: "vector/0.45/zstd_bulk", Skip: "quarantined: future-version cell pending registry pin"},
		{Name: "vector/latest/zstd_bulk", Skip: "quarantined: latest tag is intentionally tracked outside PR gates"},
		{Name: "otelcol/0.95/http_proto_gzip", Skip: "quarantined: legacy collector config validation pending"},
		{Name: "otelcol/latest/http_proto_gzip", Skip: "quarantined: latest tag is intentionally tracked outside PR gates"},
		{Name: "splunk-uf/9.x/hec", Skip: "quarantined: Splunk Universal Forwarder image requires license/bootstrap secrets in CI"},
	}
}

func runMatrixFilebeat(image string) func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeFixture(t, 100)
		config := fmt.Sprintf(`
filebeat.inputs:
  - type: filestream
    id: fixture
    paths: ["/var/log/fixture.log"]
output.elasticsearch:
  hosts: ["http://host.docker.internal:%d"]
  allow_older_versions: true
`, rig.ESPort)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"-e", "-strict.perms=false", "-c", "/usr/share/filebeat/filebeat.yml"},
			WaitingFor: wait.ForLog("Connection to backoff").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/var/log/fixture.log"),
				{Reader: strings.NewReader(config), ContainerFilePath: "/usr/share/filebeat/filebeat.yml", FileMode: 0o644},
			},
		})
		waitForSourceCount(t, rig, "filebeat-8.15.0", 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}

func runMatrixFluentBit(image string, logstashFormat bool) func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeFixture(t, 100)
		output := `    Logstash_Format Off
    Index test-fluentbit
`
		source := "test-fluentbit"
		if logstashFormat {
			output = `    Logstash_Format On
    Logstash_Prefix fluent-bit
    Logstash_DateFormat %Y.%m.%d
`
			source = "fluent-bit"
		}
		config := fmt.Sprintf(`
[INPUT]
    Name  tail
    Path  /var/log/fixture.log
    Tag   fixture
    Read_From_Head true

[OUTPUT]
    Name  es
    Match *
    Host  host.docker.internal
    Port  %d
    Suppress_Type_Name On
    Compress gzip
%s`, rig.ESPort, output)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"-c", "/fluent-bit/etc/fluent-bit.conf"},
			WaitingFor: wait.ForLog("[output:es:").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/var/log/fixture.log"),
				{Reader: strings.NewReader(config), ContainerFilePath: "/fluent-bit/etc/fluent-bit.conf", FileMode: 0o644},
			},
		})
		waitForSourceCount(t, rig, source, 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}

func runMatrixVector(image, mode string) func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeFixture(t, 100)
		extra := `    bulk:
      index: test-vector
`
		source := "test-vector"
		if mode == "data_stream" {
			extra = `    bulk:
      action: create
    data_stream:
      type: logs
      dataset: generic
      namespace: default
`
			source = "logs-generic-default"
		}
		config := fmt.Sprintf(`
sources:
  fixture:
    type: file
    include: ["/var/log/fixture.log"]
    read_from: beginning
sinks:
  lynxdb:
    type: elasticsearch
    inputs: [fixture]
    endpoints: ["http://host.docker.internal:%d"]
    api_version: v8
    mode: %s
    compression: zstd
    healthcheck:
      enabled: true
%s`, rig.ESPort, mode, extra)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"--config", "/etc/vector/vector.yaml"},
			WaitingFor: wait.ForLog("Vector has started").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/var/log/fixture.log"),
				{Reader: strings.NewReader(config), ContainerFilePath: "/etc/vector/vector.yaml", FileMode: 0o644},
			},
		})
		waitForSourceCount(t, rig, source, 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}

func runMatrixOtelColHTTP(image string) func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeFixture(t, 100)
		config := fmt.Sprintf(`
receivers:
  filelog:
    include: ["/var/log/fixture.log"]
    start_at: beginning
exporters:
  otlphttp:
    endpoint: "http://host.docker.internal:%d"
    compression: gzip
    encoding: proto
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlphttp]
`, rig.OTLPPort)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"--config=/etc/otelcol.yaml"},
			WaitingFor: wait.ForLog("Everything is ready. Begin running").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/var/log/fixture.log"),
				{Reader: strings.NewReader(config), ContainerFilePath: "/etc/otelcol.yaml", FileMode: 0o644},
			},
		})
		waitForSourceCount(t, rig, "otlp", 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}

func runMatrixOtelColGRPC(image string) func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeFixture(t, 100)
		config := fmt.Sprintf(`
receivers:
  filelog:
    include: ["/var/log/fixture.log"]
    start_at: beginning
exporters:
  otlp:
    endpoint: "host.docker.internal:%d"
    compression: gzip
    tls:
      insecure: true
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlp]
`, rig.OTLPGRPC)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      image,
			Cmd:        []string{"--config=/etc/otelcol.yaml"},
			WaitingFor: wait.ForLog("Everything is ready. Begin running").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/var/log/fixture.log"),
				{Reader: strings.NewReader(config), ContainerFilePath: "/etc/otelcol.yaml", FileMode: 0o644},
			},
		})
		waitForSourceCount(t, rig, "otlp", 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}

func runMatrixSplunkHEC() func(*testing.T) {
	return func(t *testing.T) {
		rig := StartLynxDB(t)
		fixture := writeHECFixture(t, 100)
		cmd := fmt.Sprintf(
			`resp=$(curl -fsS -H 'Authorization: Splunk token' -H 'X-Splunk-Request-Channel: matrix-channel' -H 'Content-Type: application/json' --data-binary @/tmp/hec.jsonl http://host.docker.internal:%d/services/collector/event) && ack=$(echo "$resp" | sed -n 's/.*"ackId":\([0-9][0-9]*\).*/\1/p') && test -n "$ack" && curl -fsS -H 'Authorization: Splunk token' -H 'X-Splunk-Request-Channel: matrix-channel' -H 'Content-Type: application/json' --data-binary "{\"acks\":[${ack}]}" http://host.docker.internal:%d/services/collector/ack && echo hec-success`,
			rig.ESPort,
			rig.ESPort,
		)
		ctr := runContainer(t, testcontainers.ContainerRequest{
			Image:      "curlimages/curl:8.10.1",
			Entrypoint: []string{"sh", "-c"},
			Cmd:        []string{cmd},
			WaitingFor: wait.ForLog("hec-success").WithStartupTimeout(60 * time.Second),
			Files: []testcontainers.ContainerFile{
				containerFile(fixture, "/tmp/hec.jsonl"),
			},
		})
		waitForSourceCount(t, rig, "splunk-hec", 100, func() string { return containerLogs(t, ctr) })
		assertNoShipperErrors(t, containerLogs(t, ctr))
	}
}
