package part

import (
	"slices"

	"github.com/lynxbase/lynxdb/pkg/colstore/shred"
)

// A stamp is a time written as RFC 3339 in one layout: 2006-01-02T15:04:05,
// then a point and frac digits when frac is set, then a zone kept as the
// bytes it is written with, Z or an offset. Its head number is the time the
// digits say, read as UTC whatever the zone, in units of 10^-frac seconds;
// a stamp spells back from it byte for byte.
const (
	stampDate = len("2006-01-02T15:04:05")
	// maxStampZone bounds the bytes of a zone.
	maxStampZone = len("+00:00")
)

var pow10 = [...]int64{1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9}

// stampLayout returns the fraction digits and the zone of v, a stamp in
// some layout, and false when v is none.
func stampLayout(v []byte) (frac int, zone []byte, ok bool) {
	if len(v) < stampDate+1 || v[4] != '-' || v[7] != '-' || v[10] != 'T' || v[13] != ':' || v[16] != ':' {
		return 0, nil, false
	}
	rest := v[stampDate:]
	if rest[0] == '.' {
		for frac = 1; frac < len(rest) && rest[frac] >= '0' && rest[frac] <= '9'; frac++ {
		}
		frac--
		if frac == 0 || frac > 9 {
			return 0, nil, false
		}
		rest = rest[frac+1:]
	}
	switch {
	case len(rest) == 1 && rest[0] == 'Z':
	case len(rest) == maxStampZone && (rest[0] == '+' || rest[0] == '-') && rest[3] == ':':
	default:
		return 0, nil, false
	}

	return frac, rest, true
}

// daysIn returns the days of month m of year y.
func daysIn(y, m int64) int64 {
	switch m {
	case 2:
		if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
			return 29
		}

		return 28
	case 4, 6, 9, 11:
		return 30
	}

	return 31
}

// daysFromCivil returns the days from 1970-01-01 to the date, after Howard
// Hinnant's algorithm, for years 0 to 9999.
func daysFromCivil(y, m, d int64) int64 {
	if m <= 2 {
		y--
	}
	era := floorDiv(y, 400)
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy

	return era*146097 + doe - 719468
}

// civilFromDays is the inverse of daysFromCivil.
func civilFromDays(z int64) (y, m, d int64) {
	z += 719468
	era := floorDiv(z, 146097)
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = yoe + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = doy - (153*mp+2)/5 + 1
	m = mp + 3
	if mp >= 10 {
		m = mp - 9
	}
	if m <= 2 {
		y++
	}

	return y, m, d
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}

	return q
}

// digitsAt returns the number that the n digits of v from at on spell, and
// false when one is not a digit.
func digitsAt(v []byte, at, n int) (int64, bool) {
	x := int64(0)
	for _, b := range v[at : at+n] {
		if b < '0' || b > '9' {
			return 0, false
		}
		x = x*10 + int64(b-'0')
	}

	return x, true
}

// parseStamp returns the head number of v, a stamp of frac fraction digits
// and the zone, and false when v is none or would not spell back as it is.
func parseStamp(v []byte, frac int, zone []byte) (int64, bool) {
	end := stampDate
	if frac > 0 {
		end += 1 + frac
	}
	if len(v) != end+len(zone) || v[4] != '-' || v[7] != '-' || v[10] != 'T' || v[13] != ':' || v[16] != ':' ||
		(frac > 0 && v[stampDate] != '.') || string(v[end:]) != string(zone) {
		return 0, false
	}
	y, ok1 := digitsAt(v, 0, 4)
	mo, ok2 := digitsAt(v, 5, 2)
	d, ok3 := digitsAt(v, 8, 2)
	h, ok4 := digitsAt(v, 11, 2)
	mi, ok5 := digitsAt(v, 14, 2)
	s, ok6 := digitsAt(v, 17, 2)
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 || mo < 1 || mo > 12 || d < 1 || d > daysIn(y, mo) || h > 23 || mi > 59 || s > 59 {
		return 0, false
	}
	f := int64(0)
	if frac > 0 {
		var ok bool
		if f, ok = digitsAt(v, stampDate+1, frac); !ok {
			return 0, false
		}
	}
	secs := daysFromCivil(y, mo, d)*86400 + h*3600 + mi*60 + s
	// Nanoseconds reach only from 1677 to 2262.
	if limit := (1<<63 - 1) / pow10[frac]; secs >= limit || secs <= -limit {
		return 0, false
	}

	return secs*pow10[frac] + f, true
}

// stampSpeller writes stamps of one layout, remembering the date it wrote
// last, which most stamps of a block share.
type stampSpeller struct {
	frac      int
	zone      []byte
	day       int64
	date      [10]byte
	dateKnown bool
}

// put writes the stamp of head number x at the start of out, which has the
// room of one.
func (s *stampSpeller) put(out []byte, x int64) {
	secs := floorDiv(x, pow10[s.frac])
	f := x - secs*pow10[s.frac]
	day := floorDiv(secs, 86400)
	sod := secs - day*86400
	if !s.dateKnown || day != s.day {
		y, m, d := civilFromDays(day)
		s.date = [10]byte{
			byte('0' + y/1000%10), byte('0' + y/100%10), byte('0' + y/10%10), byte('0' + y%10), '-',
			byte('0' + m/10), byte('0' + m%10), '-', byte('0' + d/10), byte('0' + d%10),
		}
		s.day, s.dateKnown = day, true
	}
	copy(out, s.date[:])
	h, mi, sec := sod/3600, sod/60%60, sod%60
	out[10], out[11], out[12], out[13] = 'T', byte('0'+h/10), byte('0'+h%10), ':'
	out[14], out[15], out[16] = byte('0'+mi/10), byte('0'+mi%10), ':'
	out[17], out[18] = byte('0'+sec/10), byte('0'+sec%10)
	at := stampDate
	if s.frac > 0 {
		out[at] = '.'
		for j := at + s.frac; j > at; j-- {
			out[j] = byte('0' + f%10)
			f /= 10
		}
		at += 1 + s.frac
	}
	copy(out[at:], s.zone)
}

// promisingStamps reports whether most of an even sample of the column's
// values are stamps of one layout, finds the reference, among nothing, the
// times and refs, that their head numbers follow most closely, and sets the
// shape of the column to the layout.
func (s *timedScratch) promisingStamps(c *shred.Column, times []int64, refs []timedRef) bool {
	n := c.Count
	type layout struct {
		frac int
		zone string
	}
	var seen [timedSample]layout
	m := 0
	for j := range s.picked {
		s.picked[j] = j * (n - 1) / (timedSample - 1)
		if frac, zone, ok := stampLayout(c.Bytes(s.picked[j])); ok {
			seen[m] = layout{frac, string(zone)}
			m++
		}
	}
	if m < timedSample-timedSample/maxIrregularShare {
		return false
	}
	s.reason = ""
	best, run := seen[0], 0
	for _, l := range seen[:m] {
		k := 0
		for _, o := range seen[:m] {
			if o == l {
				k++
			}
		}
		if k > run {
			best, run = l, k
		}
	}
	if run < timedSample-timedSample/maxIrregularShare {
		s.reason = "times of several layouts in the sample"

		return false
	}
	s.form, s.frac, s.zone = formStamp, best.frac, append(s.zone[:0], best.zone...)
	s.stride = stampDate + len(best.zone)
	if best.frac > 0 {
		s.stride += 1 + best.frac
	}
	s.from, s.to, s.headDigits, s.radixBits = 0, s.stride, 0, 0
	k := 0
	for _, i := range s.picked {
		if x, ok := parseStamp(c.Bytes(i), s.frac, s.zone); ok {
			s.sample[k], s.sampleRows[k] = uint64(x), c.Row(i)
			k++
		}
	}
	if k < timedSample/2 {
		s.reason = "few sample times parse"

		return false
	}
	s.relate(k, times, refs)
	if s.cost >= 1<<30 {
		s.reason = "no reference in the sample"

		return false
	}
	// What the general codec would take is not known here; the trials
	// compare the forms.
	s.headPacked = 64

	return s.cost+minTimedGain <= s.headPacked
}

// scanStamps reads the head number of every value of the column that is a
// stamp of the shape's layout.
func (s *timedScratch) scanStamps(c *shred.Column) {
	n := c.Count
	s.heads, s.regular = grow(s.heads, n), grow(s.regular, n)
	s.irregular = 0
	for i := 0; i < n; i++ {
		x, ok := parseStamp(c.Bytes(i), s.frac, s.zone)
		s.heads[i], s.regular[i] = uint64(x), ok
		if !ok {
			s.irregular++
		}
	}
}

// stampsZone reports whether zone, kept for a layout, is one.
func stampsZone(zone []byte) bool {
	return slices.Equal(zone, []byte("Z")) ||
		len(zone) == maxStampZone && (zone[0] == '+' || zone[0] == '-') && zone[3] == ':'
}
