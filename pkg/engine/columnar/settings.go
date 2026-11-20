package columnar

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/colstore"
)

// Settings is the design of an index, fixed when the index is created: the
// fields whose values order its rows, the fields its event times are taken
// from, tried in their order, and the span of time whose events its
// partitions keep apart, zero for one partition.
type Settings struct {
	SortKey    []string
	TimeFields []string
	Partition  time.Duration
}

// ErrSettings reports settings that an index cannot take.
var ErrSettings = errors.New("columnar: invalid index settings")

// ErrSettingsConflict reports a request for settings that an index that
// exists does not have.
var ErrSettingsConflict = errors.New("columnar: the index exists with other settings")

const (
	// settingsName is the file in the directory of an index that holds its
	// settings.
	settingsName = "settings.json"
	// newSuffix ends the name of the directory of an index being created.
	newSuffix = ".new"
	// maxSettingsFields bounds the paths of a sort key and of time fields.
	maxSettingsFields = 32
	// minPartition and maxPartition bound the span of a partition that a
	// request may set: a shorter one scatters the events over many small
	// parts, a longer one keeps retention from dropping anything.
	minPartition = time.Minute
	maxPartition = 366 * 24 * time.Hour
)

// Validate reports an error, wrapping ErrSettings, unless every field path
// is set, holds no empty segment and no control character and is in its
// list once, each list holds at most 32 paths, and the partition span is
// zero, which leaves it unset, or a minute to 366 days.
func (s Settings) Validate() error {
	for _, list := range []struct {
		name  string
		paths []string
	}{{"sort_key", s.SortKey}, {"time_fields", s.TimeFields}} {
		if len(list.paths) > maxSettingsFields {
			return fmt.Errorf("%w: %s holds %d fields, at most %d are allowed", ErrSettings, list.name, len(list.paths), maxSettingsFields)
		}
		for i, path := range list.paths {
			if err := validPath(path); err != nil {
				return fmt.Errorf("%w: %s field %q %v", ErrSettings, list.name, path, err)
			}
			if slices.Contains(list.paths[:i], path) {
				return fmt.Errorf("%w: %s names %q twice", ErrSettings, list.name, path)
			}
		}
	}
	if s.Partition != 0 && (s.Partition < minPartition || s.Partition > maxPartition) {
		return fmt.Errorf("%w: partition %s is not a span from 1m to 366d", ErrSettings, s.Partition)
	}

	return nil
}

// validPath reports why path cannot name a field.
func validPath(path string) error {
	if path == "" {
		return errors.New("is empty")
	}
	if !utf8.ValidString(path) {
		return errors.New("is not UTF-8")
	}
	for _, segment := range strings.Split(path, ".") {
		if segment == "" {
			return errors.New("has an empty segment")
		}
	}
	if strings.ContainsFunc(path, unicode.IsControl) {
		return errors.New("holds a control character")
	}

	return nil
}

// fill returns the settings with the fields that s leaves unset, a nil
// list or a zero partition, taken from defaults.
func (s Settings) fill(defaults Settings) Settings {
	if s.SortKey == nil {
		s.SortKey = defaults.SortKey
	}
	if s.TimeFields == nil {
		s.TimeFields = defaults.TimeFields
	}
	if s.Partition == 0 {
		s.Partition = defaults.Partition
	}

	return s
}

// conflict names the first setting that want sets and has differs in,
// with the value has holds, or returns "" when they agree in every setting
// that want sets.
func (want Settings) conflict(has Settings) string {
	switch {
	case want.SortKey != nil && !slices.Equal(want.SortKey, has.SortKey):
		return fmt.Sprintf("sort_key %v, not %v", has.SortKey, want.SortKey)
	case want.TimeFields != nil && !slices.Equal(want.TimeFields, has.TimeFields):
		return fmt.Sprintf("time_fields %v, not %v", has.TimeFields, want.TimeFields)
	case want.Partition != 0 && want.Partition != has.Partition:
		return fmt.Sprintf("partition %s, not %s", has.Partition, want.Partition)
	}

	return ""
}

// storeSettings returns the settings that store options give an index.
func storeSettings(o colstore.StoreOptions) Settings {
	s := Settings{SortKey: slices.Clone(o.SortKey), Partition: max(o.Partition, 0)}
	if o.TimeField != "" {
		s.TimeFields = append(s.TimeFields, o.TimeField)
	}
	s.TimeFields = append(s.TimeFields, o.TimeFields...)
	if s.SortKey == nil {
		s.SortKey = []string{}
	}
	if s.TimeFields == nil {
		s.TimeFields = []string{}
	}

	return s
}

// options returns base with the design of an index set to s.
func (s Settings) options(base colstore.StoreOptions) colstore.StoreOptions {
	base.SortKey, base.TimeField, base.TimeFields, base.Partition = s.SortKey, "", s.TimeFields, s.Partition

	return base
}

// settingsFile is the form of the settings file.
type settingsFile struct {
	SortKey    []string `json:"sort_key"`
	TimeFields []string `json:"time_fields"`
	Partition  string   `json:"partition"`
}

// readSettings reads the settings of the index in dir.
func readSettings(dir string) (Settings, error) {
	data, err := os.ReadFile(filepath.Join(dir, settingsName))
	if errors.Is(err, os.ErrNotExist) {
		return Settings{}, fmt.Errorf("%s holds no %s and is not an index", dir, settingsName)
	}
	if err != nil {
		return Settings{}, err
	}
	var f settingsFile
	if err := json.Unmarshal(data, &f); err != nil {
		return Settings{}, fmt.Errorf("%s: %w", settingsName, err)
	}
	partition, err := time.ParseDuration(f.Partition)
	if err != nil {
		return Settings{}, fmt.Errorf("%s: partition: %w", settingsName, err)
	}
	s := Settings{SortKey: f.SortKey, TimeFields: f.TimeFields, Partition: partition}
	if s.SortKey == nil {
		s.SortKey = []string{}
	}
	if s.TimeFields == nil {
		s.TimeFields = []string{}
	}

	return s, nil
}

// makeIndexDir makes the directory of index under root with s as its
// settings: it writes them in a directory aside, named for the index with
// a dot before and newSuffix after, syncs them and renames that directory
// into place.
func makeIndexDir(root, index string, s Settings) error {
	aside := filepath.Join(root, "."+index+newSuffix)
	if err := os.RemoveAll(aside); err != nil {
		return err
	}
	if err := os.Mkdir(aside, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(settingsFile{SortKey: s.SortKey, TimeFields: s.TimeFields, Partition: s.Partition.String()}, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(aside, settingsName))
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close(), syncDir(aside)); err != nil {
		return err
	}
	if err := os.Rename(aside, filepath.Join(root, index)); err != nil {
		return err
	}

	return syncDir(root)
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}

	return errors.Join(d.Sync(), d.Close())
}
