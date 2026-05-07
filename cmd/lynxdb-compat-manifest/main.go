package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type syncManifest struct {
	RsigmaVersion string                       `json:"rsigma_version"`
	Queries       []syncManifestEntry          `json:"queries"`
	Fixtures      map[string]syncManifestEntry `json:"fixtures"`
}

type syncManifestEntry struct {
	Fixture string   `json:"fixture,omitempty"`
	Line    int      `json:"line"`
	RuleID  string   `json:"rule_id"`
	Title   string   `json:"title"`
	Level   string   `json:"level"`
	Tags    []string `json:"tags"`
}

type matchesFile struct {
	MatchCount int `json:"match_count"`
}

type compatManifest struct {
	RsigmaVersion string          `json:"rsigma_version"`
	LynxDBVersion string          `json:"lynxdb_version"`
	Fixtures      []compatFixture `json:"fixtures"`
}

type compatFixture struct {
	Name               string   `json:"name"`
	RuleID             string   `json:"rule_id"`
	Title              string   `json:"title,omitempty"`
	Level              string   `json:"level,omitempty"`
	Tags               []string `json:"tags,omitempty"`
	SPL2               string   `json:"spl2"`
	Format             string   `json:"format"`
	Shapes             []string `json:"shapes"`
	ExpectedMatchCount int      `json:"expected_match_count"`
}

func main() {
	goldenDir := flag.String("golden-dir", filepath.Join("pkg", "sigmaqueries", "testdata", "golden"), "golden corpus directory")
	lynxdbVersion := flag.String("lynxdb-version", "dev", "LynxDB version to record")
	output := flag.String("output", filepath.Join("pkg", "sigmaqueries", "compat_manifest.json"), "output manifest path")
	flag.Parse()

	manifest, err := buildCompatManifest(*goldenDir, *lynxdbVersion)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err := writeCompatManifest(*output, manifest); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func buildCompatManifest(goldenDir, lynxdbVersion string) (*compatManifest, error) {
	syncData, err := readSyncManifest(filepath.Join(goldenDir, "manifest.json"))
	if err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(goldenDir, "*.spl2"))
	if err != nil {
		return nil, fmt.Errorf("glob spl2 fixtures: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no SPL2 fixtures under %s", goldenDir)
	}

	out := &compatManifest{
		RsigmaVersion: syncData.RsigmaVersion,
		LynxDBVersion: lynxdbVersion,
		Fixtures:      make([]compatFixture, 0, len(files)),
	}
	for _, path := range files {
		name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		format := fixtureFormat(name)
		baseName := fixtureBaseName(name)
		entry := syncData.Fixtures[baseName]

		spl2, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		matchCount, err := readMatchCount(filepath.Join(goldenDir, baseName+".matches.json"))
		if err != nil {
			return nil, err
		}

		out.Fixtures = append(out.Fixtures, compatFixture{
			Name:               name,
			RuleID:             entry.RuleID,
			Title:              entry.Title,
			Level:              entry.Level,
			Tags:               entry.Tags,
			SPL2:               strings.TrimSpace(string(spl2)),
			Format:             format,
			Shapes:             detectShapes(strings.TrimSpace(string(spl2))),
			ExpectedMatchCount: matchCount,
		})
	}

	return out, nil
}

func readSyncManifest(path string) (*syncManifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sync manifest: %w", err)
	}

	var manifest syncManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("decode sync manifest: %w", err)
	}
	if manifest.Fixtures == nil {
		return nil, fmt.Errorf("sync manifest has no fixtures map")
	}

	return &manifest, nil
}

func readMatchCount(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("read match fixture %s: %w", path, err)
	}

	var matches matchesFile
	if err := json.Unmarshal(data, &matches); err != nil {
		return 0, fmt.Errorf("decode match fixture %s: %w", path, err)
	}

	return matches.MatchCount, nil
}

func writeCompatManifest(path string, manifest *compatManifest) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir output dir: %w", err)
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal compat manifest: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write compat manifest: %w", err)
	}

	return nil
}

func fixtureFormat(name string) string {
	if strings.HasSuffix(name, "_minimal") {
		return "minimal"
	}

	return "default"
}

func fixtureBaseName(name string) string {
	name = strings.TrimSuffix(name, "_minimal")
	name = strings.TrimSuffix(name, "_index")

	return name
}

func detectShapes(query string) []string {
	shapes := []string{"search.predicate"}
	if strings.Contains(query, " =~ ") {
		shapes = append(shapes, "where.regex")
	}
	if strings.Contains(query, "cidrmatch(") {
		shapes = append(shapes, "where.cidrmatch")
	}
	if strings.Contains(query, " IN (") {
		shapes = append(shapes, "search.in")
	}
	if strings.Contains(query, " AND ") {
		shapes = append(shapes, "search.boolean.and")
	}
	if strings.Contains(query, " OR ") {
		shapes = append(shapes, "search.boolean.or")
	}
	if strings.Contains(query, "NOT ") {
		shapes = append(shapes, "search.boolean.not")
	}
	if strings.Contains(query, "=*") || strings.Contains(query, "*\"") {
		shapes = append(shapes, "search.wildcard")
	}
	if strings.Contains(query, ">=") || strings.Contains(query, "<=") || strings.Contains(query, ">") || strings.Contains(query, "<") {
		shapes = append(shapes, "search.comparison")
	}

	return shapes
}
