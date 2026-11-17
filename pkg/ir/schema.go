package ir

import (
	"sort"
	"strings"
)

// Column describes one output column of an operator.
type Column struct {
	Name     string
	Type     Type
	Nullable bool
}

// Schema is the output schema of an operator.
//
// A closed schema lists every column a row can have. An open schema lists
// the columns known at planning time; rows may carry further columns of type
// Any, as schema-on-read sources do.
type Schema struct {
	Columns []Column
	Open    bool
}

// OpenSchema returns an open schema with the given known columns.
func OpenSchema(cols ...Column) Schema { return Schema{Columns: cols, Open: true} }

// ClosedSchema returns a closed schema with the given columns.
func ClosedSchema(cols ...Column) Schema { return Schema{Columns: cols} }

// Lookup returns the column with the given name.
func (s Schema) Lookup(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Resolve returns the type a reference to name has in this schema. The
// second result is false when the schema is closed and has no such column.
func (s Schema) Resolve(name string) (Type, bool) {
	if c, ok := s.Lookup(name); ok {
		return c.Type, true
	}
	if s.Open {
		return Any, true
	}
	if dot := strings.IndexByte(name, '.'); dot > 0 {
		if c, ok := s.Lookup(name[:dot]); ok && (c.Type.Kind == KindObject || c.Type.Kind == KindAny) {
			return Any, true
		}
	}
	return Any, false
}

// Names returns the known column names in order.
func (s Schema) Names() []string {
	out := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = c.Name
	}
	return out
}

// With returns a copy of s where the column named c.Name is replaced by c, or
// c is appended when it is not present.
func (s Schema) With(c Column) Schema {
	cols := make([]Column, 0, len(s.Columns)+1)
	replaced := false
	for _, old := range s.Columns {
		if old.Name == c.Name {
			cols = append(cols, c)
			replaced = true
			continue
		}
		cols = append(cols, old)
	}
	if !replaced {
		cols = append(cols, c)
	}
	return Schema{Columns: cols, Open: s.Open}
}

// SchemaBuilder derives a schema by successive With calls on one copy of
// the columns, finding columns of wide schemas through an index so that the
// cost stays linear in the number of columns.
type SchemaBuilder struct {
	cols []Column
	open bool
	// index maps names to positions once the schema is wide; dups is set
	// when the columns repeat a name, which rules the index out.
	index map[string]int
	dups  bool
	finds int
}

// wideSchema is the column count from which a SchemaBuilder indexes names,
// once a few lookups have shown that the index pays for itself.
const wideSchema = 32

// Init starts the builder from a copy of in with room for extra columns.
func (b *SchemaBuilder) Init(in Schema, extra int) {
	b.cols = make([]Column, len(in.Columns), len(in.Columns)+extra)
	copy(b.cols, in.Columns)
	b.open, b.index, b.dups, b.finds = in.Open, nil, false, 0
}

func (b *SchemaBuilder) find(name string) int {
	b.finds++
	if b.index == nil && !b.dups && len(b.cols) > wideSchema && b.finds > 8 {
		idx := make(map[string]int, 2*len(b.cols))
		for i, c := range b.cols {
			if _, ok := idx[c.Name]; ok {
				b.dups = true
				break
			}
			idx[c.Name] = i
		}
		if !b.dups {
			b.index = idx
		}
	}
	if b.index != nil {
		if i, ok := b.index[name]; ok {
			return i
		}
		return -1
	}
	for i := range b.cols {
		if b.cols[i].Name == name {
			return i
		}
	}
	return -1
}

// With replaces the columns named c.Name by c, or appends c, as
// Schema.With does.
func (b *SchemaBuilder) With(c Column) {
	if b.dups {
		replaced := false
		for i := range b.cols {
			if b.cols[i].Name == c.Name {
				b.cols[i] = c
				replaced = true
			}
		}
		if !replaced {
			b.cols = append(b.cols, c)
		}
		return
	}
	if i := b.find(c.Name); i >= 0 {
		b.cols[i] = c
		return
	}
	if b.index != nil {
		b.index[c.Name] = len(b.cols)
	}
	b.cols = append(b.cols, c)
}

// Lookup returns the first column with the given name.
func (b *SchemaBuilder) Lookup(name string) (Column, bool) {
	if i := b.find(name); i >= 0 {
		return b.cols[i], true
	}
	return Column{}, false
}

// Schema returns the schema built so far; later With calls modify its
// columns in place.
func (b *SchemaBuilder) Schema() Schema { return Schema{Columns: b.cols, Open: b.open} }

// Without returns a copy of s without the named columns.
func (s Schema) Without(names ...string) Schema {
	drop := make(map[string]bool, len(names))
	for _, n := range names {
		drop[n] = true
	}
	cols := make([]Column, 0, len(s.Columns))
	for _, c := range s.Columns {
		if !drop[c.Name] {
			cols = append(cols, c)
		}
	}
	return Schema{Columns: cols, Open: s.Open}
}

// Clone returns a deep copy of the column slice.
func (s Schema) Clone() Schema {
	return Schema{Columns: append([]Column(nil), s.Columns...), Open: s.Open}
}

// MatchNames returns the known columns whose names match a pattern that may
// contain the wildcard '*', in schema order.
func (s Schema) MatchNames(pattern string) []string {
	if !IsNamePattern(pattern) {
		if _, ok := s.Lookup(pattern); ok {
			return []string{pattern}
		}
		return nil
	}
	var out []string
	for _, c := range s.Columns {
		if MatchName(pattern, c.Name) {
			out = append(out, c.Name)
		}
	}
	return out
}

// IsNamePattern reports whether a column name is a wildcard pattern.
func IsNamePattern(name string) bool { return strings.ContainsRune(name, '*') }

// MatchName matches a column name against a wildcard pattern where '*'
// matches any run of characters, including dots.
func MatchName(pattern, name string) bool {
	if !IsNamePattern(pattern) {
		return pattern == name
	}
	parts := strings.Split(pattern, "*")
	if !strings.HasPrefix(name, parts[0]) {
		return false
	}
	rest := name[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(rest, mid)
		if i < 0 {
			return false
		}
		rest = rest[i+len(mid):]
	}
	return strings.HasSuffix(rest, last)
}

// UnionSchemas merges schemas by column name for set operations: columns
// keep first-seen order, types unify, and the result is open when any input
// is open.
func UnionSchemas(schemas ...Schema) Schema {
	var out Schema
	idx := map[string]int{}
	seen := map[string]int{}
	for _, s := range schemas {
		if s.Open {
			out.Open = true
		}
		for _, c := range s.Columns {
			seen[c.Name]++
			if i, ok := idx[c.Name]; ok {
				out.Columns[i].Type = Unify(out.Columns[i].Type, c.Type)
				out.Columns[i].Nullable = out.Columns[i].Nullable || c.Nullable
				continue
			}
			idx[c.Name] = len(out.Columns)
			out.Columns = append(out.Columns, c)
		}
	}
	for i := range out.Columns {
		if seen[out.Columns[i].Name] < len(schemas) {
			out.Columns[i].Nullable = true
		}
	}
	return out
}

// SortedNames returns the known column names sorted lexicographically.
func (s Schema) SortedNames() []string {
	n := s.Names()
	sort.Strings(n)
	return n
}
