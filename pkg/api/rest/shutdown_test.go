package rest

import (
	"net/http"
	"testing"
	"time"
)

func TestShutdownCancelsRequestsAfterDrainTimeout(t *testing.T) {
	cfg := Config{Addr: "127.0.0.1:0", Logger: testLogger()}
	cfg.HTTP.ShutdownTimeout = 50 * time.Millisecond
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	exited := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv.httpServer.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		close(exited)
	})
	stop := serveTestServer(t, srv)
	defer stop()
	go func() {
		resp, err := http.Get("http://" + srv.Addr() + "/")
		if err == nil {
			resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not reach the server")
	}
	stop()
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("request still active after shutdown")
	}
}
