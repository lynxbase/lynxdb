//go:build e2e

package shippers

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func writeFixture(t *testing.T, lines int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.log")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create fixture: %v", err)
	}
	defer f.Close()
	for i := 0; i < lines; i++ {
		fmt.Fprintf(f, `{"message":"shipper fixture %03d","level":"info","seq":%d}`+"\n", i, i)
	}
	return path
}

func containerFile(hostPath, containerPath string) testcontainers.ContainerFile {
	return testcontainers.ContainerFile{
		HostFilePath:      hostPath,
		ContainerFilePath: containerPath,
		FileMode:          0o644,
	}
}
