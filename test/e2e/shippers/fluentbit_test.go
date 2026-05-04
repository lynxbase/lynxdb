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

func TestE2E_Shipper_FluentBit(t *testing.T) {
	tests := []struct {
		name   string
		output string
		source string
	}{
		{
			name: "plain_index",
			output: `    Logstash_Format Off
    Index test-fluentbit
`,
			source: "test-fluentbit",
		},
		{
			name: "logstash_format",
			output: `    Logstash_Format On
    Logstash_Prefix fluent-bit
    Logstash_DateFormat %Y.%m.%d
`,
			source: "fluent-bit",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rig := StartLynxDB(t)
			fixture := writeFixture(t, 100)
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
%s`, rig.ESPort, tt.output)

			ctr := runContainer(t, testcontainers.ContainerRequest{
				Image:      "cr.fluentbit.io/fluent/fluent-bit:3.1",
				Cmd:        []string{"-c", "/fluent-bit/etc/fluent-bit.conf"},
				WaitingFor: wait.ForLog("[output:es:").WithStartupTimeout(60 * time.Second),
				Files: []testcontainers.ContainerFile{
					containerFile(fixture, "/var/log/fixture.log"),
					{Reader: strings.NewReader(config), ContainerFilePath: "/fluent-bit/etc/fluent-bit.conf", FileMode: 0o644},
				},
			})

			waitForSourceCount(t, rig, tt.source, 100, func() string { return containerLogs(t, ctr) })
			assertNoShipperErrors(t, containerLogs(t, ctr))
		})
	}
}
