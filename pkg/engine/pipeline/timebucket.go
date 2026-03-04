package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

// BinIterator implements BIN timestamp bucketing.
type BinIterator struct {
	child   Iterator
	field   string
	alias   string
	spanDur time.Duration
}

// NewBinIterator creates a time bucketing operator.
func NewBinIterator(child Iterator, field, alias string, span time.Duration) *BinIterator {
	if alias == "" {
		alias = field
	}

	return &BinIterator{child: child, field: field, alias: alias, spanDur: span}
}

func (b *BinIterator) Init(ctx context.Context) error {
	return b.child.Init(ctx)
}

// timestamp formats tried by tryParseTimestamp, most specific first.
var tsFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000-0700",
	"2006-01-02T15:04:05-0700",
	"2006-01-02T15:04:05.000Z",
	"2006-01-02 15:04:05",
}

// tryParseTimestamp tries to parse s as a timestamp using common formats.
func tryParseTimestamp(s string) (time.Time, bool) {
	s = strings.TrimLeft(s, "\" ")
	for _, fmt := range tsFormats {
		t, err := time.Parse(fmt, s)
		if err == nil {
			return t, true
		}
		// Try with prefix (string may be longer than format)
		if len(s) > len(fmt) {
			t, err = time.Parse(fmt, s[:len(fmt)])
			if err == nil {
				return t, true
			}
		}
	}

	return time.Time{}, false
}

func (b *BinIterator) Next(ctx context.Context) (*Batch, error) {
	batch, err := b.child.Next(ctx)
	if batch == nil || err != nil {
		return nil, err
	}

	col, ok := batch.Columns[b.field]
	if !ok {
		return batch, nil
	}

	aliasCol := make([]event.Value, batch.Len)
	spanNanos := b.spanDur.Nanoseconds()
	if spanNanos == 0 {
		spanNanos = time.Hour.Nanoseconds() // default to 1h if span unset
	}
	for i := 0; i < batch.Len; i++ {
		v := col[i]
		if v.IsNull() {
			aliasCol[i] = event.NullValue()

			continue
		}
		var ts time.Time
		switch v.Type() {
		case event.FieldTypeTimestamp:
			ts = v.AsTimestamp()
		case event.FieldTypeInt:
			ts = time.Unix(0, v.AsInt())
		case event.FieldTypeString:
			parsed, ok := tryParseTimestamp(v.AsString())
			if !ok {
				aliasCol[i] = v

				continue
			}
			ts = parsed
		default:
			aliasCol[i] = v

			continue
		}
		bucketNano := (ts.UnixNano() / spanNanos) * spanNanos
		aliasCol[i] = event.TimestampValue(time.Unix(0, bucketNano).UTC())
	}
	batch.Columns[b.alias] = aliasCol

	return batch, nil
}

func (b *BinIterator) Close() error { return b.child.Close() }

func (b *BinIterator) Schema() []FieldInfo { return b.child.Schema() }
