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

func TestE2E_Shipper_Vector(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		mode        string
		extra       string
		source      string
	}{
		{
			name:        "bulk_none",
			compression: "none",
			mode:        "bulk",
			extra: `    bulk:
      index: test-vector
`,
			source: "test-vector",
		},
		{
			name:        "bulk_zstd",
			compression: "zstd",
			mode:        "bulk",
			extra: `    bulk:
      index: test-vector
`,
			source: "test-vector",
		},
		{
			name:        "data_stream",
			compression: "zstd",
			mode:        "data_stream",
			extra: `    bulk:
      action: create
    data_stream:
      type: logs
      dataset: generic
      namespace: default
`,
			source: "logs-generic-default",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rig := StartLynxDB(t)
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
    mode: %s
    compression: %s
    healthcheck:
      enabled: true
%s`, rig.ESPort, tt.mode, tt.compression, tt.extra)

			ctr := runContainer(t, testcontainers.ContainerRequest{
				Image:      "timberio/vector:0.40.0-alpine",
				Cmd:        []string{"--config", "/etc/vector/vector.yaml"},
				WaitingFor: wait.ForLog("Vector has started").WithStartupTimeout(60 * time.Second),
				Files: []testcontainers.ContainerFile{
					containerFile(fixture, "/var/log/fixture.log"),
					{Reader: strings.NewReader(config), ContainerFilePath: "/etc/vector/vector.yaml", FileMode: 0o644},
				},
			})

			waitForSourceCount(t, rig, tt.source, 100, func() string { return containerLogs(t, ctr) })
			assertNoShipperErrors(t, containerLogs(t, ctr))
		})
	}
}
