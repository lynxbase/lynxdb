//go:build e2e

package shippers

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The shippers below send a key with the ingest scope to a server with auth
// on, each in the scheme it uses: Filebeat ApiKey, Vector Basic, the
// OpenTelemetry Collector Bearer over HTTP and gRPC, and HEC clients Splunk.

func TestE2E_Shipper_Auth_Filebeat(t *testing.T) {
	rig := StartLynxDBWithAuth(t)
	fixture := writeFixture(t, 100)
	config := fmt.Sprintf(`
filebeat.inputs:
  - type: filestream
    id: fixture
    paths: ["/var/log/fixture.log"]
output.elasticsearch:
  hosts: ["http://host.docker.internal:%d"]
  api_key: "%s"
  allow_older_versions: true
setup.template.enabled: false
setup.ilm.enabled: false
`, rig.ESPort, rig.IngestKey.APIKeyComposite)

	ctr := runContainer(t, testcontainers.ContainerRequest{
		Image: "docker.elastic.co/beats/filebeat:8.15.0",
		Cmd:   []string{"-e", "-strict.perms=false", "-c", "/usr/share/filebeat/filebeat.yml"},
		Files: []testcontainers.ContainerFile{
			containerFile(fixture, "/var/log/fixture.log"),
			{Reader: strings.NewReader(config), ContainerFilePath: "/usr/share/filebeat/filebeat.yml", FileMode: 0o644},
		},
	})

	waitForCount(t, rig, countQuery("filebeat-8.15.0", holds("message", fixtureText)), 100, func() string { return containerLogs(t, ctr) })
	assertNoShipperErrors(t, containerLogs(t, ctr))
}

func TestE2E_Shipper_Auth_Vector(t *testing.T) {
	rig := StartLynxDBWithAuth(t)
	fixture := writeFixture(t, 100)
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
    mode: bulk
    bulk:
      index: vector-auth
    auth:
      strategy: basic
      user: lynxdb
      password: "%s"
    healthcheck:
      enabled: true
`, rig.ESPort, rig.IngestKey.Token)

	ctr := runContainer(t, testcontainers.ContainerRequest{
		Image:      "timberio/vector:0.40.0-alpine",
		Cmd:        []string{"--config", "/etc/vector/vector.yaml"},
		WaitingFor: wait.ForLog("Vector has started").WithStartupTimeout(60 * time.Second),
		Files: []testcontainers.ContainerFile{
			containerFile(fixture, "/var/log/fixture.log"),
			{Reader: strings.NewReader(config), ContainerFilePath: "/etc/vector/vector.yaml", FileMode: 0o644},
		},
	})

	waitForCount(t, rig, countQuery("vector-auth", holds("message", fixtureText)), 100, func() string { return containerLogs(t, ctr) })
	assertNoShipperErrors(t, containerLogs(t, ctr))
}

func TestE2E_Shipper_Auth_OtelCol(t *testing.T) {
	for _, tt := range []struct {
		name     string
		exporter func(rig *TestRig) string
	}{
		{"http", func(rig *TestRig) string {
			return fmt.Sprintf(`  otlphttp:
    endpoint: "http://host.docker.internal:%d"
    headers:
      Authorization: "Bearer %s"`, rig.OTLPPort, rig.IngestKey.Token)
		}},
		{"grpc", func(rig *TestRig) string {
			return fmt.Sprintf(`  otlp:
    endpoint: "host.docker.internal:%d"
    tls:
      insecure: true
    headers:
      authorization: "Bearer %s"`, rig.OTLPGRPC, rig.IngestKey.Token)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rig := StartLynxDBWithAuth(t)
			fixture := writeFixture(t, 100)
			exporter := tt.exporter(rig)
			name := strings.TrimSuffix(strings.Fields(exporter)[0], ":")
			config := fmt.Sprintf(`
receivers:
  filelog:
    include: ["/var/log/fixture.log"]
    start_at: beginning
exporters:
%s
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [%s]
`, exporter, name)

			ctr := runContainer(t, testcontainers.ContainerRequest{
				Image:      "otel/opentelemetry-collector-contrib:0.105.0",
				Cmd:        []string{"--config=/etc/otelcol.yaml"},
				WaitingFor: wait.ForLog("Everything is ready. Begin running").WithStartupTimeout(60 * time.Second),
				Files: []testcontainers.ContainerFile{
					containerFile(fixture, "/var/log/fixture.log"),
					{Reader: strings.NewReader(config), ContainerFilePath: "/etc/otelcol.yaml", FileMode: 0o644},
				},
			})

			waitForCount(t, rig, otlpFixtureCount(), 100, func() string { return containerLogs(t, ctr) })
			assertNoShipperErrors(t, containerLogs(t, ctr))
		})
	}
}

func TestE2E_Shipper_Auth_SplunkHEC(t *testing.T) {
	rig := StartLynxDBWithAuth(t)
	fixture := writeHECFixture(t, 100)
	cmd := fmt.Sprintf(
		`curl -fsS -H 'Authorization: Splunk %s' -H 'Content-Type: application/json' --data-binary @/tmp/hec.jsonl http://host.docker.internal:%d/services/collector/event && echo hec-success`,
		rig.IngestKey.Token, rig.ESPort,
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

	waitForCount(t, rig, hecFixtureCount(), 100, func() string { return containerLogs(t, ctr) })
	assertNoShipperErrors(t, containerLogs(t, ctr))
}
