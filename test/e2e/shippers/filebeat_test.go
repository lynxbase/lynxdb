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

func TestE2E_Shipper_Filebeat(t *testing.T) {
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
		Image:      "docker.elastic.co/beats/filebeat:8.15.0",
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
