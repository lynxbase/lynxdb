//go:build e2e

package shippers

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestE2E_Shipper_SplunkHEC(t *testing.T) {
	rig := StartLynxDB(t)
	fixture := writeHECFixture(t, 100)
	cmd := fmt.Sprintf(
		`resp=$(curl -fsS -H 'Authorization: Splunk token' -H 'X-Splunk-Request-Channel: e2e-channel' -H 'Content-Type: application/json' --data-binary @/tmp/hec.jsonl http://host.docker.internal:%d/services/collector/event) && ack=$(echo "$resp" | sed -n 's/.*"ackId":\([0-9][0-9]*\).*/\1/p') && test -n "$ack" && curl -fsS -H 'Authorization: Splunk token' -H 'X-Splunk-Request-Channel: e2e-channel' -H 'Content-Type: application/json' --data-binary "{\"acks\":[${ack}]}" http://host.docker.internal:%d/services/collector/ack && echo hec-success`,
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

func writeHECFixture(t *testing.T, lines int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hec.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create HEC fixture: %v", err)
	}
	defer f.Close()
	for i := 0; i < lines; i++ {
		fmt.Fprintf(f, `{"event":{"message":"splunk hec fixture %03d","level":"info","seq":%d},"index":"splunk-hec","sourcetype":"_json","host":"hec-fixture"}`+"\n", i, i)
	}
	return path
}
