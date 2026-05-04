//go:build e2e

package shippers

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/api/rest"
	"github.com/lynxbase/lynxdb/pkg/client"
	"github.com/lynxbase/lynxdb/pkg/config"
	container "github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

type TestRig struct {
	Server   *rest.Server
	Client   *client.Client
	ESPort   int
	OTLPPort int
	OTLPGRPC int
	DataDir  string

	cancel    context.CancelFunc
	startDone chan struct{}
}

func StartLynxDB(t *testing.T) *TestRig {
	t.Helper()

	cfg := config.DefaultConfig()
	listenHost := shipperListenHost()
	cfg.Listen = listenHost + ":0"
	cfg.DataDir = t.TempDir()
	cfg.Ingest.OTLP.HTTPListen = listenHost + ":0"
	cfg.Ingest.OTLP.GRPCListen = listenHost + ":0"
	cfg.Storage.CompactionInterval = time.Hour
	cfg.Storage.TieringInterval = time.Hour

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv, err := rest.NewServer(rest.Config{
		Addr:          cfg.Listen,
		DataDir:       cfg.DataDir,
		Storage:       cfg.Storage,
		Logger:        logger,
		RuntimeConfig: cfg,
		Ingest:        cfg.Ingest,
		Query:         cfg.Query,
		HTTP:          cfg.HTTP,
		Server:        cfg.Server,
		Views:         cfg.Views,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	rig := &TestRig{Server: srv, DataDir: cfg.DataDir, cancel: cancel, startDone: make(chan struct{})}
	go func() {
		defer close(rig.startDone)
		if err := srv.Start(ctx); err != nil && ctx.Err() == nil {
			t.Logf("server stopped with error: %v", err)
		}
	}()
	srv.WaitReady()

	esPort, err := portOf(srv.Addr())
	if err != nil {
		t.Fatalf("ES addr %q: %v", srv.Addr(), err)
	}
	otlpPort, err := portOf(srv.OTLPHTTPAddr())
	if err != nil {
		t.Fatalf("OTLP addr %q: %v", srv.OTLPHTTPAddr(), err)
	}
	otlpGRPCPort, err := portOf(srv.OTLPGRPCAddr())
	if err != nil {
		t.Fatalf("OTLP gRPC addr %q: %v", srv.OTLPGRPCAddr(), err)
	}
	rig.ESPort = esPort
	rig.OTLPPort = otlpPort
	rig.OTLPGRPC = otlpGRPCPort
	rig.Client = client.NewClient(
		client.WithBaseURL("http://127.0.0.1:"+fmt.Sprint(esPort)),
		client.WithTimeout(60*time.Second),
	)

	t.Cleanup(func() {
		cancel()
		select {
		case <-rig.startDone:
		case <-time.After(30 * time.Second):
			t.Fatal("server did not shut down within 30s")
		}
	})
	return rig
}

func shipperListenHost() string {
	if runtime.GOOS == "linux" {
		return "0.0.0.0"
	}
	return "127.0.0.1"
}

func ConfigureContainerNetworking(req *testcontainers.ContainerRequest) {
	if runtime.GOOS != "linux" {
		return
	}
	old := req.HostConfigModifier
	req.HostConfigModifier = func(h *container.HostConfig) {
		if old != nil {
			old(h)
		}
		h.ExtraHosts = append(h.ExtraHosts, "host.docker.internal:host-gateway")
	}
}

func runContainer(t *testing.T, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	ConfigureContainerNetworking(&req)
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container %s: %v", req.Image, err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(ctr); err != nil {
			t.Logf("terminate container: %v", err)
		}
	})
	return ctr
}

func containerLogs(t *testing.T, ctr testcontainers.Container) string {
	t.Helper()
	rc, err := ctr.Logs(context.Background())
	if err != nil {
		t.Fatalf("container logs: %v", err)
	}
	defer rc.Close()
	b, _ := io.ReadAll(rc)
	return string(b)
}

func assertNoShipperErrors(t *testing.T, logs string) {
	t.Helper()
	lower := strings.ToLower(logs)
	for _, needle := range []string{
		`"log.level":"error"`,
		`"log.level":"warn"`,
		`"status":4`,
		`"status":5`,
		`"status_code":4`,
		`"status_code":5`,
		"panic",
	} {
		if strings.Contains(lower, needle) {
			t.Fatalf("shipper logs contain %q:\n%s", needle, logs)
		}
	}
}

func portOf(addr string) (int, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	var n int
	if _, err := fmt.Sscanf(port, "%d", &n); err != nil {
		return 0, err
	}
	return n, nil
}
