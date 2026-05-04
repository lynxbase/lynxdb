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

func TestE2E_Shipper_OtelCol(t *testing.T) {
	for _, encoding := range []string{"proto", "json"} {
		t.Run(encoding, func(t *testing.T) {
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
    encoding: %s
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlphttp]
`, rig.OTLPPort, encoding)

			ctr := runContainer(t, testcontainers.ContainerRequest{
				Image:      "otel/opentelemetry-collector-contrib:0.105.0",
				Cmd:        []string{"--config=/etc/otelcol.yaml"},
				WaitingFor: wait.ForLog("Everything is ready. Begin running").WithStartupTimeout(60 * time.Second),
				Files: []testcontainers.ContainerFile{
					containerFile(fixture, "/var/log/fixture.log"),
					{Reader: strings.NewReader(config), ContainerFilePath: "/etc/otelcol.yaml", FileMode: 0o644},
				},
			})

			waitForSourceCount(t, rig, "otlp", 100, func() string { return containerLogs(t, ctr) })
			assertNoShipperErrors(t, containerLogs(t, ctr))
		})
	}
}

func TestE2E_Shipper_OtelCol_GRPC(t *testing.T) {
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
    tls:
      insecure: true
service:
  pipelines:
    logs:
      receivers: [filelog]
      exporters: [otlp]
`, rig.OTLPGRPC)

	ctr := runContainer(t, testcontainers.ContainerRequest{
		Image:      "otel/opentelemetry-collector-contrib:0.105.0",
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
