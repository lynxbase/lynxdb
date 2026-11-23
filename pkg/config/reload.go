package config

import (
	"reflect"
	"sort"
	"strings"
)

// ReloadChanges classifies config fields changed by a reload attempt.
type ReloadChanges struct {
	HotReloaded     []string
	RestartRequired []string
}

// hotReloadableExactFields are the settings that every part of the server
// reads again for each request or batch. The others are read when the
// server starts: the database takes retention and the query slots and job
// lifetime when it opens, the listeners their limits and handshake when they
// are built, and the syslog receiver the ingest mode.
var hotReloadableExactFields = map[string]struct{}{
	"log_level":                 {},
	"ingest.max_batch_size":     {},
	"ingest.max_line_bytes":     {},
	"ingest.es_compat.enabled":  {},
	"ingest.splunk_hec.enabled": {},
	"http.shutdown_timeout":     {},
	"syslog.index":              {},
	"syslog.sourcetype":         {},
	"syslog.default_timezone":   {},
	"syslog.default_hostname":   {},
	"syslog.batch_size":         {},
	"syslog.batch_timeout":      {},
}

var hotReloadablePrefixes = []string{
	"query.",
}

var nonHotReloadablePrefixes = []string{
	"profiles",
}

var nonHotReloadableExactFields = map[string]struct{}{
	"query.max_concurrent":            {},
	"query.job_ttl":                   {},
	"http.idle_timeout":               {},
	"http.read_header_timeout":        {},
	"http.rate_limit":                 {},
	"ingest.otlp.http_listen":         {},
	"ingest.otlp.grpc_listen":         {},
	"ingest.otlp.grpc_max_recv_bytes": {},
	"ingest.max_body_size":            {},
}

// Reload returns the configuration a running server takes from newCfg:
// oldCfg with the hot-reloadable changes of newCfg, so that it reports only
// settings in effect, and the changes classified.
func Reload(oldCfg, newCfg *Config) (*Config, ReloadChanges) {
	out := *oldCfg
	applyHotReloadable(reflect.ValueOf(&out).Elem(), reflect.ValueOf(*newCfg), "")

	return &out, ClassifyReloadChanges(oldCfg, newCfg)
}

func applyHotReloadable(dst, src reflect.Value, prefix string) {
	if !shouldRecurseReloadStruct(dst.Type()) {
		if isHotReloadablePath(prefix) && !shouldIgnoreReloadPath(prefix) {
			dst.Set(src)
		}

		return
	}
	for i := 0; i < dst.NumField(); i++ {
		field := dst.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		name := taggedFieldName(field)
		if name == "" {
			continue
		}
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}
		applyHotReloadable(dst.Field(i), src.Field(i), path)
	}
}

// ClassifyReloadChanges returns the changed config fields grouped by whether
// they take effect immediately or require a restart.
func ClassifyReloadChanges(oldCfg, newCfg *Config) ReloadChanges {
	if oldCfg == nil || newCfg == nil {
		return ReloadChanges{}
	}

	paths := changedConfigPaths(reflect.ValueOf(*oldCfg), reflect.ValueOf(*newCfg), "")
	sort.Strings(paths)

	out := ReloadChanges{
		HotReloaded:     make([]string, 0, len(paths)),
		RestartRequired: make([]string, 0, len(paths)),
	}
	for _, path := range paths {
		if shouldIgnoreReloadPath(path) {
			continue
		}
		if isHotReloadablePath(path) {
			out.HotReloaded = append(out.HotReloaded, path)
		} else {
			out.RestartRequired = append(out.RestartRequired, path)
		}
	}

	return out
}

func changedConfigPaths(oldV, newV reflect.Value, prefix string) []string {
	if !oldV.IsValid() || !newV.IsValid() {
		if oldV.IsValid() == newV.IsValid() {
			return nil
		}

		return []string{prefix}
	}

	if oldV.Kind() == reflect.Pointer || newV.Kind() == reflect.Pointer {
		if oldV.IsNil() || newV.IsNil() {
			if oldV.IsNil() && newV.IsNil() {
				return nil
			}

			return []string{prefix}
		}

		return changedConfigPaths(oldV.Elem(), newV.Elem(), prefix)
	}

	if oldV.Type() != newV.Type() {
		return []string{prefix}
	}

	if shouldRecurseReloadStruct(oldV.Type()) {
		var out []string
		for i := 0; i < oldV.NumField(); i++ {
			field := oldV.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			name := taggedFieldName(field)
			if name == "" {
				continue
			}

			fieldPath := name
			if prefix != "" {
				fieldPath = prefix + "." + name
			}
			out = append(out, changedConfigPaths(oldV.Field(i), newV.Field(i), fieldPath)...)
		}

		return out
	}

	if reflect.DeepEqual(oldV.Interface(), newV.Interface()) {
		return nil
	}

	return []string{prefix}
}

func shouldRecurseReloadStruct(t reflect.Type) bool {
	return t.Kind() == reflect.Struct && t.PkgPath() == "github.com/lynxbase/lynxdb/pkg/config"
}

func taggedFieldName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return ""
	}
	if idx := strings.IndexByte(tag, ','); idx >= 0 {
		tag = tag[:idx]
	}
	if tag != "" {
		return tag
	}

	return strings.ToLower(field.Name)
}

func shouldIgnoreReloadPath(path string) bool {
	for _, prefix := range nonHotReloadablePrefixes {
		if path == prefix || strings.HasPrefix(path, prefix+".") {
			return true
		}
	}

	return false
}

func isHotReloadablePath(path string) bool {
	if _, ok := nonHotReloadableExactFields[path]; ok {
		return false
	}
	if _, ok := hotReloadableExactFields[path]; ok {
		return true
	}
	for _, prefix := range hotReloadablePrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}
