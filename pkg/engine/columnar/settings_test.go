package columnar

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
	"github.com/lynxbase/lynxdb/pkg/ppl"
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
	off, on := false, true
	want := Settings{SortKey: []string{"n"}, TimeFields: []string{"ts"}, Partition: 24 * time.Hour, PartDictionary: &off}
	got, created, err := db.Create("bench", Settings{SortKey: []string{"n"}, Partition: 24 * time.Hour})
	if err != nil || !created || !reflect.DeepEqual(got, want) {
		t.Fatalf("create: %+v, created %v, %v", got, created, err)
	}
	for _, again := range []Settings{{SortKey: []string{"n"}, Partition: 24 * time.Hour}, {}, {TimeFields: []string{"ts"}}, {PartDictionary: &off}} {
		if got, created, err := db.Create("bench", again); err != nil || created || !reflect.DeepEqual(got, want) {
			t.Fatalf("create %+v again: %+v, created %v, %v", again, got, created, err)
		}
	}
	for _, other := range []Settings{{SortKey: []string{"host"}}, {SortKey: []string{}}, {TimeFields: []string{"time"}}, {Partition: time.Hour}, {PartDictionary: &on}} {
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
	cfg.Store.Part.Dictionary = true
	if db, err = Open(cfg); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for index, want := range map[string]Settings{
		"bench": want,
		"web":   {SortKey: []string{"host"}, TimeFields: []string{"ts"}, Partition: time.Hour, PartDictionary: &off},
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
	if st, _ := db.Index("new"); !reflect.DeepEqual(st.Settings, Settings{SortKey: []string{"level"}, TimeFields: []string{"time"}, Partition: 2 * time.Hour, PartDictionary: &on}) {
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

// accountLines returns n events from number from on, a millisecond apart,
// each of one of 1500 accounts whose identifiers repeat across the blocks of
// a part.
func accountLines(from, n int) []byte {
	const base32 = "abcdefghijklmnopqrstuvwxyz234567"
	var b strings.Builder
	for i := from; i < from+n; i++ {
		r := rand.New(rand.NewSource(int64(i * 7919 % 1500)))
		id := make([]byte, 24)
		for j := range id {
			id[j] = base32[r.Intn(32)]
		}
		at := testNow.Add(time.Duration(i) * time.Millisecond).UnixMicro()
		fmt.Fprintf(&b, `{"ts":%d,"host":"h%d","n":%d,"acct":"did:plc:%s"}`+"\n", at, i%3, i, id)
	}

	return []byte(b.String())
}

// partsOf returns the parts of an index and how many of them hold a
// dictionary.
func partsOf(t *testing.T, db *DB, index string) (parts, withDictionary int) {
	t.Helper()
	snap, ok := db.Snapshot(index)
	if !ok {
		t.Fatalf("no index %s", index)
	}
	defer snap.Release()
	for _, h := range snap.Parts() {
		parts++
		if h.Shares() {
			withDictionary++
		}
	}

	return parts, withDictionary
}

// blocksConfig is testConfig with parts of blocks small enough for the
// accounts of accountLines to repeat across the blocks of a part, which is
// where a part dictionary pays.
func blocksConfig(dir string) Config {
	cfg := testConfig(dir)
	cfg.Store.BlockRows = 1024

	return cfg
}

func TestPartDictionaryIsASettingOfAnIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(blocksConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	off, on := false, true
	if got, created, err := db.Create("coded", Settings{PartDictionary: &on}); err != nil || !created || !got.partDictionary() {
		t.Fatalf("create with part dictionaries: %+v, created %v, %v", got, created, err)
	}
	if got, created, err := db.Create("plain", Settings{}); err != nil || !created || got.PartDictionary == nil || *got.PartDictionary {
		t.Fatalf("create without the setting: %+v, created %v, %v", got, created, err)
	}
	for index, same := range map[string]*bool{"coded": &on, "plain": &off} {
		if _, created, err := db.Create(index, Settings{PartDictionary: same}); err != nil || created {
			t.Fatalf("create %s again with its setting: created %v, %v", index, created, err)
		}
	}
	for index, other := range map[string]*bool{"coded": &off, "plain": &on} {
		if _, _, err := db.Create(index, Settings{PartDictionary: other}); !errors.Is(err, ErrSettingsConflict) {
			t.Fatalf("create %s with the other setting: %v", index, err)
		}
	}

	// The same events in both, in runs that a compaction merges.
	load := func(db *DB, from int) {
		t.Helper()
		for run := range 3 {
			for _, index := range []string{"coded", "plain"} {
				if _, err := appendTo(db, index, accountLines(from+run*8000, 8000)); err != nil {
					t.Fatal(err)
				}
			}
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// check requires parts with dictionaries in coded alone, a part of few
	// rows has none, and the same answers of both, which coded gives from
	// the codes of its parts.
	check := func(db *DB, step string, events int) {
		t.Helper()
		if parts, with := partsOf(t, db, "coded"); with == 0 {
			t.Fatalf("%s: none of the %d parts of coded holds a dictionary", step, parts)
		}
		if parts, with := partsOf(t, db, "plain"); parts == 0 || with != 0 {
			t.Fatalf("%s: %d of the %d parts of plain hold a dictionary", step, with, parts)
		}
		for _, q := range []struct {
			text                 string
			aggregated, filtered bool
		}{
			{`stats count() as n by acct | sort - n, acct | head 5`, true, false},
			{`stats count() as n, distinct_count(acct) as accounts`, true, false},
			{`where like(acct, 'did:plc:a%') | stats count() as n by host | sort host`, false, true},
			{`where acct >= 'did:plc:x' | sort - n | head 3 | fields n, acct`, false, true},
		} {
			var answers [2]string
			for i, index := range []string{"coded", "plain"} {
				compiled, err := ppl.Compile("source=" + index + " | " + q.text)
				if err != nil {
					t.Fatalf("%s: %v", q.text, err)
				}
				res, err := db.Query(context.Background(), compiled.Plan, Options{Now: testNow.Add(time.Hour)})
				if err != nil || res.table == nil {
					t.Fatalf("%s: %s: %v", step, q.text, err)
				}
				answers[i] = render(res.Maps())
				st := res.table.Stats
				if byCode := index == "coded"; (st.AggregatedByCode > 0) != (byCode && q.aggregated) || (st.ColumnsFilteredByCode > 0) != (byCode && q.filtered) {
					t.Errorf("%s: %s over %s: %d blocks aggregated and %d columns filtered by code", step, q.text, index, st.AggregatedByCode, st.ColumnsFilteredByCode)
				}
			}
			if answers[0] != answers[1] || answers[0] == "" {
				t.Errorf("%s: %s\n coded %s\n plain %s", step, q.text, answers[0], answers[1])
			}
		}
		if got := ask(t, db, `source=coded | stats count() as n`); got != fmt.Sprintf("n=%d", events) {
			t.Fatalf("%s: coded holds %s", step, got)
		}
	}
	load(db, 0)
	check(db, "after the flushes", 24000)
	for _, index := range []string{"coded", "plain"} {
		if _, _, err := db.Compact(index); err != nil {
			t.Fatal(err)
		}
	}
	check(db, "after the compaction", 24000)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// A configuration that gives new indexes the other setting leaves both
	// as they are, and so do the merges under it.
	for _, dictionary := range []bool{true, false} {
		cfg := blocksConfig(dir)
		cfg.Store.Part.Dictionary = dictionary
		if db, err = Open(cfg); err != nil {
			t.Fatal(err)
		}
		for index, want := range map[string]bool{"coded": true, "plain": false} {
			if st, ok := db.Index(index); !ok || st.Settings.partDictionary() != want {
				t.Fatalf("%s reopened under part dictionaries %v: %+v, %v", index, dictionary, st.Settings, ok)
			}
		}
		events := 48000
		if !dictionary {
			events = 72000
		}
		load(db, events-24000)
		for _, index := range []string{"coded", "plain"} {
			if _, _, err := db.Compact(index); err != nil {
				t.Fatal(err)
			}
		}
		check(db, fmt.Sprintf("reopened under part dictionaries %v", dictionary), events)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSettingsFileWithoutPartDictionaryIsAnIndexWithoutThem(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "old"), 0o755); err != nil {
		t.Fatal(err)
	}
	file := `{"sort_key": [], "time_fields": ["ts"], "partition": "1h0m0s"}`
	if err := os.WriteFile(filepath.Join(dir, "old", settingsName), []byte(file), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := blocksConfig(dir)
	cfg.Store.Part.Dictionary = true
	db, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	off := false
	want := Settings{SortKey: []string{}, TimeFields: []string{"ts"}, Partition: time.Hour, PartDictionary: &off}
	if st, ok := db.Index("old"); !ok || !reflect.DeepEqual(st.Settings, want) {
		t.Fatalf("the index of a settings file without part_dictionary: %+v, %v", st.Settings, ok)
	}
	// The same events give the parts of an index made under this
	// configuration dictionaries.
	for _, index := range []string{"old", "new"} {
		if _, err := appendTo(db, index, accountLines(0, 12000)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if parts, with := partsOf(t, db, "old"); parts == 0 || with != 0 {
		t.Fatalf("%d of the %d parts of the index hold a dictionary", with, parts)
	}
	if parts, with := partsOf(t, db, "new"); with == 0 {
		t.Fatalf("none of the %d parts of an index made by ingest holds a dictionary", parts)
	}
}
