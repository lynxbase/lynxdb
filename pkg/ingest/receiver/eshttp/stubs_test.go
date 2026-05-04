package eshttp

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStub_XPackInfo_ReturnsLicense(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.XPackInfo), http.MethodGet, "/_xpack")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product = %q", got)
	}
	license := body["license"].(map[string]interface{})
	if license["status"] != "active" || license["type"] != "basic" {
		t.Fatalf("license = %#v, want active basic", license)
	}
	if _, ok := body["features"].(map[string]interface{}); !ok {
		t.Fatalf("features missing or wrong type: %#v", body["features"])
	}
}

func TestStub_License_ReturnsActiveBasic(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.XPackLicense), http.MethodGet, "/_license")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	license := body["license"].(map[string]interface{})
	if license["status"] != "active" || license["type"] != "basic" || license["mode"] != "basic" {
		t.Fatalf("license = %#v, want active basic", license)
	}
}

func TestStub_IndexTemplatePut_Acknowledged(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.Acknowledged), http.MethodPut, "/_index_template/foo")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body["acknowledged"] != true {
		t.Fatalf("acknowledged = %#v, want true", body["acknowledged"])
	}
}

func TestStub_IndexTemplateGet_EmptyList(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.IndexTemplates), http.MethodGet, "/_index_template/foo")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	templates := body["index_templates"].([]interface{})
	if len(templates) != 0 {
		t.Fatalf("index_templates len = %d, want 0", len(templates))
	}
}

func TestStub_ILMPolicy_Returns404(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.NotFound), http.MethodGet, "/_ilm/policy/foo")

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	if len(body) != 0 {
		t.Fatalf("body = %#v, want empty object", body)
	}
}

func TestStub_CatTemplates_ReturnsEmptyArray(t *testing.T) {
	stubs := newTestStubs(t)
	req := httptest.NewRequest(http.MethodGet, "/_cat/templates", nil)
	rr := httptest.NewRecorder()

	stubs.EmptyArray(rr, req)

	resp := rr.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body []interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("body len = %d, want 0", len(body))
	}
}

func TestStub_NodesHTTP_ReturnsNodeInfo(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.NodesHTTP), http.MethodGet, "/_nodes/http")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body["cluster_name"] != "lynxdb" {
		t.Fatalf("cluster_name = %#v, want lynxdb", body["cluster_name"])
	}
	nodes := body["nodes"].(map[string]interface{})
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
}

func TestStub_ClusterHealth_ReturnsGreen(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.ClusterHealth), http.MethodGet, "/_cluster/health")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body["status"] != "green" {
		t.Fatalf("status = %#v, want green", body["status"])
	}
	if body["number_of_nodes"] != float64(1) {
		t.Fatalf("number_of_nodes = %#v, want 1", body["number_of_nodes"])
	}
}

func TestStub_EmptySearch_ReturnsNoHits(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.EmptySearch), http.MethodGet, "/_search")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	hits := body["hits"].(map[string]interface{})
	total := hits["total"].(map[string]interface{})
	if total["value"] != float64(0) {
		t.Fatalf("hits.total.value = %#v, want 0", total["value"])
	}
	if len(hits["hits"].([]interface{})) != 0 {
		t.Fatalf("hits.hits = %#v, want empty", hits["hits"])
	}
}

func TestStub_Authenticated_ReturnsTrue(t *testing.T) {
	stubs := newTestStubs(t)
	resp, body := serveStub(t, http.HandlerFunc(stubs.Authenticated), http.MethodPost, "/_security/user/_authenticate")

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if body["authenticated"] != true {
		t.Fatalf("authenticated = %#v, want true", body["authenticated"])
	}
}

func TestStub_HeadOK_NoBody(t *testing.T) {
	stubs := newTestStubs(t)
	req := httptest.NewRequest(http.MethodHead, "/logs", nil)
	rr := httptest.NewRecorder()

	stubs.HeadOK(rr, req)

	resp := rr.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Elastic-Product"); got != "Elasticsearch" {
		t.Fatalf("X-Elastic-Product = %q", got)
	}
}

func newTestStubs(t *testing.T) *Stubs {
	t.Helper()
	stubs, err := NewStubs(Config{
		AdvertisedVersion: "8.15.0",
		ClusterName:       "lynxdb",
		DataDir:           "/tmp/lynxdb-a",
	})
	if err != nil {
		t.Fatalf("NewStubs: %v", err)
	}
	return stubs
}

func serveStub(t *testing.T, h http.Handler, method, path string) (*http.Response, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	resp := rr.Result()
	t.Cleanup(func() { resp.Body.Close() })

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return resp, body
}
