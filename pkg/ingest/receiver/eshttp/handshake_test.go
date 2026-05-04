package eshttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"unsafe"
)

func TestHandshake_Build_DefaultVersion_MatchesSchema(t *testing.T) {
	h := newTestHandshake(t, Config{
		AdvertisedVersion: "8.15.0",
		ClusterName:       "lynxdb",
		DataDir:           "/tmp/lynxdb-a",
	})

	var body map[string]interface{}
	if err := json.Unmarshal(h.Body(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body["cluster_name"] != "lynxdb" {
		t.Fatalf("cluster_name = %v, want lynxdb", body["cluster_name"])
	}
	if body["tagline"] != "You Know, for Logs" {
		t.Fatalf("tagline = %v", body["tagline"])
	}
	version := body["version"].(map[string]interface{})
	if version["number"] != "8.15.0" {
		t.Fatalf("version.number = %v, want 8.15.0", version["number"])
	}
	for _, key := range []string{
		"build_flavor",
		"build_type",
		"build_hash",
		"build_date",
		"build_snapshot",
		"lucene_version",
		"minimum_wire_compatibility_version",
		"minimum_index_compatibility_version",
	} {
		if _, ok := version[key]; !ok {
			t.Fatalf("version.%s missing", key)
		}
	}
}

func TestHandshake_Build_CustomVersion_AppearsInBody(t *testing.T) {
	h := newTestHandshake(t, Config{
		AdvertisedVersion: "9.0.1",
		ClusterName:       "lynxdb",
		DataDir:           "/tmp/lynxdb-a",
	})

	var body struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := json.Unmarshal(h.Body(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Version.Number != "9.0.1" {
		t.Fatalf("version.number = %q, want 9.0.1", body.Version.Number)
	}
}

func TestHandshake_InvalidVersion_ReturnsError(t *testing.T) {
	if _, err := NewHandshake(Config{AdvertisedVersion: "8.x", ClusterName: "lynxdb"}); err == nil {
		t.Fatal("expected invalid version error")
	}
}

func TestHandshake_HEAD_NoBody_StatusOK(t *testing.T) {
	h := newTestHandshake(t, Config{
		AdvertisedVersion: "8.15.0",
		ClusterName:       "lynxdb",
		DataDir:           "/tmp/lynxdb-a",
	})
	req := httptest.NewRequest(http.MethodHead, "/", nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	resp := rr.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if rr.Body.Len() != 0 {
		t.Fatalf("body length = %d, want 0", rr.Body.Len())
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product = %q", got)
	}
}

func TestHandshake_NodeName_HostnamePidShape(t *testing.T) {
	name := nodeName()
	if !regexp.MustCompile(`^.+-\d+$`).MatchString(name) {
		t.Fatalf("nodeName = %q, want host-pid shape", name)
	}
}

func TestClusterUUID_DataDirOnly_Stable(t *testing.T) {
	cfg := Config{DataDir: "/tmp/lynxdb-a"}
	if got, want := deriveClusterUUID(cfg), deriveClusterUUID(cfg); got != want {
		t.Fatalf("uuid not stable: %q != %q", got, want)
	}
}

func TestClusterUUID_DifferentDataDir_DifferentUUID(t *testing.T) {
	a := deriveClusterUUID(Config{DataDir: "/tmp/lynxdb-a"})
	b := deriveClusterUUID(Config{DataDir: "/tmp/lynxdb-b"})
	if a == b {
		t.Fatalf("uuid should differ for different data dirs: %q", a)
	}
}

func TestClusterUUID_S3Bucket_TakesPrecedence(t *testing.T) {
	a := deriveClusterUUID(Config{DataDir: "/tmp/a", S3Bucket: "logs-prod"})
	b := deriveClusterUUID(Config{DataDir: "/tmp/b", S3Bucket: "logs-prod"})
	if a != b {
		t.Fatalf("s3 bucket should take precedence: %q != %q", a, b)
	}
}

func TestClusterUUID_Length22_Base64URL(t *testing.T) {
	uuid := deriveClusterUUID(Config{DataDir: "/tmp/lynxdb-a"})
	if len(uuid) != 22 {
		t.Fatalf("len = %d, want 22", len(uuid))
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+$`).MatchString(uuid) {
		t.Fatalf("uuid = %q, want base64url", uuid)
	}
}

func TestConcurrent_ESHandshake_BodyOnce_NoRace(t *testing.T) {
	h := newTestHandshake(t, Config{
		AdvertisedVersion: "8.15.0",
		ClusterName:       "lynxdb",
		DataDir:           "/tmp/lynxdb-a",
	})

	const workers = 100
	ptrs := make(chan uintptr, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			body := h.Body()
			if len(body) == 0 {
				t.Error("empty body")
				return
			}
			ptrs <- uintptr(unsafe.Pointer(&body[0]))
		}()
	}
	wg.Wait()
	close(ptrs)

	var first uintptr
	for ptr := range ptrs {
		if first == 0 {
			first = ptr
			continue
		}
		if ptr != first {
			t.Fatalf("Body returned different backing arrays: %x != %x", ptr, first)
		}
	}
}

func newTestHandshake(t *testing.T, cfg Config) *Handshake {
	t.Helper()
	h, err := NewHandshake(cfg)
	if err != nil {
		t.Fatalf("NewHandshake: %v", err)
	}
	return h
}
