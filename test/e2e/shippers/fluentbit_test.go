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
		// index is the index, or index glob, the documents land in.
		index string
		// indexName is a pattern that the name of that index matches.
		indexName string
	}{
		{
			name: "plain_index",
			output: `    Logstash_Format Off
    Index test-fluentbit
`,
			index:     "test-fluentbit",
			indexName: `^test-fluentbit$`,
		},
		{
			name: "logstash_format",
			output: `    Logstash_Format On
    Logstash_Prefix fluent-bit
    Logstash_DateFormat %Y.%m.%d
`,
			index:     "fluent-bit-*",
			indexName: `^fluent-bit-\d{4}\.\d{2}\.\d{2}$`,
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

			// The tail input puts each line in the log field of a record.
			waitForCount(t, rig, countQuery(tt.index, holds("log", fixtureText)), 100, func() string { return containerLogs(t, ctr) })
			requireIndexNames(t, rig, tt.indexName)
			assertNoShipperErrors(t, containerLogs(t, ctr))
		})
	}
}
