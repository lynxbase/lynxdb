package columnar

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
)

// sortKeysOf returns the sort keys that the parts of an index were written
// with, once each.
func sortKeysOf(t *testing.T, db *DB, index string) [][]string {
	t.Helper()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	snap, ok := db.Snapshot(index)
	if !ok {
		t.Fatalf("no index %s", index)
	}
	defer snap.Release()
	var keys [][]string
	for _, h := range snap.Parts() {
		if len(keys) == 0 || !reflect.DeepEqual(keys[len(keys)-1], h.SortKey()) {
			keys = append(keys, h.SortKey())
		}
	}

	return keys
}

func TestCreateGivesAnIndexSettingsOfItsOwnThatOutliveTheConfig(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	want := Settings{SortKey: []string{"n"}, TimeFields: []string{"ts"}, Partition: 24 * time.Hour}
	got, created, err := db.Create("bench", Settings{SortKey: []string{"n"}, Partition: 24 * time.Hour})
	if err != nil || !created || !reflect.DeepEqual(got, want) {
		t.Fatalf("create: %+v, created %v, %v", got, created, err)
	}
	for _, again := range []Settings{{SortKey: []string{"n"}, Partition: 24 * time.Hour}, {}, {TimeFields: []string{"ts"}}} {
		if got, created, err := db.Create("bench", again); err != nil || created || !reflect.DeepEqual(got, want) {
			t.Fatalf("create %+v again: %+v, created %v, %v", again, got, created, err)
		}
	}
	for _, other := range []Settings{{SortKey: []string{"host"}}, {SortKey: []string{}}, {TimeFields: []string{"time"}}, {Partition: time.Hour}} {
		if got, _, err := db.Create("bench", other); !errors.Is(err, ErrSettingsConflict) || !reflect.DeepEqual(got, want) {
			t.Fatalf("create %+v over other settings: %+v, %v", other, got, err)
		}
	}
	if _, err := appendTo(db, "bench", lines("bench", 0, 50)); err != nil {
		t.Fatal(err)
	}
	if _, err := appendTo(db, "web", lines("web", 0, 50)); err != nil {
		t.Fatal(err)
	}
	if keys := sortKeysOf(t, db, "bench"); !reflect.DeepEqual(keys, [][]string{{"n"}}) {
		t.Fatalf("bench is written with the sort keys %v", keys)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Another configuration leaves the indexes that exist as they are.
	cfg := testConfig(dir)
	cfg.Store.SortKey, cfg.Store.TimeField, cfg.Store.TimeFields, cfg.Store.Partition = []string{"level"}, "", []string{"time"}, 2*time.Hour
	if db, err = Open(cfg); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for index, want := range map[string]Settings{
		"bench": want,
		"web":   {SortKey: []string{"host"}, TimeFields: []string{"ts"}, Partition: time.Hour},
	} {
		if st, ok := db.Index(index); !ok || !reflect.DeepEqual(st.Settings, want) {
			t.Fatalf("%s after reopening: %+v, %v", index, st.Settings, ok)
		}
	}
	if _, err := appendTo(db, "bench", lines("bench", 50, 50)); err != nil {
		t.Fatal(err)
	}
	if keys := sortKeysOf(t, db, "bench"); !reflect.DeepEqual(keys, [][]string{{"n"}}) {
		t.Fatalf("bench is written with the sort keys %v after reopening", keys)
	}
	if _, err := appendTo(db, "new", lines("new", 0, 10)); err != nil {
		t.Fatal(err)
	}
	if st, _ := db.Index("new"); !reflect.DeepEqual(st.Settings, Settings{SortKey: []string{"level"}, TimeFields: []string{"time"}, Partition: 2 * time.Hour}) {
		t.Fatalf("an index made by ingest: %+v", st.Settings)
	}
}

func TestADirectoryWithoutSettingsIsNotAnIndex(t *testing.T) {
	dir := t.TempDir()
	s, err := colstore.OpenStore(filepath.Join(dir, "bare"), testConfig(dir).Store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(lines("bare", 0, 20)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(testConfig(dir)); err == nil || !strings.Contains(err.Error(), settingsName) {
		if err == nil {
			db.Close()
		}
		t.Fatalf("a directory without settings opens as an index: %v", err)
	}
}

func TestAnIndexWhoseCreationDidNotFinishIsGone(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".web"+newSuffix), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := Open(testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := os.Stat(filepath.Join(dir, ".web"+newSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the directory of an unfinished creation is left: %v", err)
	}
	if _, created, err := db.Create("web", Settings{}); err != nil || !created {
		t.Fatalf("create after an unfinished creation: %v, created %v", err, created)
	}
}

func TestCreateRefusesSettingsAnIndexCannotTake(t *testing.T) {
	db, err := Open(testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, bad := range []Settings{
		{SortKey: []string{""}},
		{SortKey: []string{"a..b"}},
		{SortKey: []string{".a"}},
		{TimeFields: []string{"t", "t"}},
		{TimeFields: []string{"a\tb"}},
		{Partition: time.Second},
		{Partition: 400 * 24 * time.Hour},
		{Partition: -time.Hour},
	} {
		if _, _, err := db.Create("x", bad); !errors.Is(err, ErrSettings) {
			t.Fatalf("create with %+v: %v", bad, err)
		}
	}
	if _, _, err := db.Create("../x", Settings{}); !errors.Is(err, ErrIndexName) {
		t.Fatalf("create with a path for a name: %v", err)
	}
	if names := db.Names(); len(names) != 0 {
		t.Fatalf("refused creates left indexes %v", names)
	}
}
