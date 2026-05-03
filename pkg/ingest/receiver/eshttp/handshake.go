package eshttp

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
)

type Config struct {
	AdvertisedVersion string
	ClusterName       string
	DataDir           string
	S3Bucket          string
}

var versionRE = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

func (c Config) Validate() error {
	if !versionRE.MatchString(c.AdvertisedVersion) {
		return fmt.Errorf("ingest.es_compat.advertised_version %q must match X.Y.Z", c.AdvertisedVersion)
	}
	if c.ClusterName == "" {
		return fmt.Errorf("ingest.es_compat.cluster_name must not be empty")
	}
	return nil
}

type Handshake struct {
	cfg         Config
	nodeName    string
	clusterUUID string

	once sync.Once
	body []byte
}

func NewHandshake(cfg Config) (*Handshake, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Handshake{
		cfg:         cfg,
		nodeName:    nodeName(),
		clusterUUID: deriveClusterUUID(cfg),
	}, nil
}

func (h *Handshake) Body() []byte {
	h.once.Do(func() {
		h.body = h.build()
	})
	return h.body
}

func (h *Handshake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(h.Body())
}

func (h *Handshake) build() []byte {
	payload := map[string]interface{}{
		"name":         "lynxdb-" + h.nodeName,
		"cluster_name": h.cfg.ClusterName,
		"cluster_uuid": h.clusterUUID,
		"version": map[string]interface{}{
			"number":                              h.cfg.AdvertisedVersion,
			"build_flavor":                        "default",
			"build_type":                          "tarball",
			"build_hash":                          "lynxdb",
			"build_date":                          "2025-01-01T00:00:00.000Z",
			"build_snapshot":                      false,
			"lucene_version":                      "9.7.0",
			"minimum_wire_compatibility_version":  "7.17.0",
			"minimum_index_compatibility_version": "7.0.0",
		},
		"tagline": "You Know, for Logs",
	}
	b, _ := json.Marshal(payload)
	return b
}

func nodeName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid())
}

func deriveClusterUUID(cfg Config) string {
	seed := "fs:" + cfg.DataDir
	if cfg.S3Bucket != "" {
		seed = "s3:" + cfg.S3Bucket
	}
	sum := sha256.Sum256([]byte(seed))
	return base64.RawURLEncoding.EncodeToString(sum[:])[:22]
}
