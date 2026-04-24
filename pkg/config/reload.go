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

var hotReloadableExactFields = map[string]struct{}{
	"log_level":                        {},
	"retention":                        {},
	"storage.compaction_rate_limit_mb": {},
	"ingest.mode":                      {},
	"ingest.max_batch_size":            {},
	"ingest.max_line_bytes":            {},
	"http.shutdown_timeout":            {},
}

var hotReloadablePrefixes = []string{
	"query.",
	"tail.",
}

var nonHotReloadablePrefixes = []string{
	"profiles",
}

var nonHotReloadableExactFields = map[string]struct{}{
	"http.idle_timeout":             {},
	"http.read_header_timeout":      {},
	"http.rate_limit":               {},
	"ingest.max_body_size":          {},
	"ingest.fsync":                  {},
	"ingest.dedup_enabled":          {},
	"ingest.dedup_capacity":         {},
	"query.global_query_pool_bytes": {},
	"query.spill_dir":               {},
	"query.max_temp_dir_size_bytes": {},
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
