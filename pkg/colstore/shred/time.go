package shred

import (
	"math"
	"strconv"
	"time"
	"unsafe"
)

// epochFloat converts an epoch with a fraction, in the unit that its size
// tells as EpochNanos does, to nanoseconds. A float holds about sixteen
// digits, so the fraction is taken to the microsecond.
func epochFloat(v float64) (int64, bool) {
	abs := math.Abs(v)
	if abs != abs || abs > 9e18 {
		return 0, false
	}
	whole, frac := math.Modf(v)
	switch {
	case abs < 1e11:
		return int64(whole)*1e9 + int64(math.Round(frac*1e6))*1e3, true
	case abs < 1e14:
		return int64(whole)*1e6 + int64(math.Round(frac*1e3))*1e3, true
	case abs < 1e17:
		return int64(whole)*1e3 + int64(math.Round(frac))*1e3, true
	}

	return int64(whole), true
}

// ParseTime reads a time as text: a date, a T or a space, a time of day
// with or without a fraction of a second, and a zone as Z or as an offset
// from UTC, without which the time is in UTC; or a number, as an epoch.
func ParseTime(s []byte) (int64, bool) {
	if t, ok := parseStamp(s); ok {
		return t, true
	}
	if len(s) == 0 || len(s) > 40 {
		return 0, false
	}
	text := unsafe.String(unsafe.SliceData(s), len(s))
	if n, err := strconv.ParseInt(text, 10, 64); err == nil {
		return EpochNanos(n), true
	}
	if f, err := strconv.ParseFloat(text, 64); err == nil {
		return epochFloat(f)
	}
	// The forms that the layout takes beyond the common one.
	if t, err := time.Parse(time.RFC3339Nano, text); err == nil {
		return t.UnixNano(), true
	}

	return 0, false
}

func digits2(s []byte) (int, bool) {
	a, b := s[0]-'0', s[1]-'0'

	return int(a)*10 + int(b), a <= 9 && b <= 9
}

var monthDays = [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

// parseStamp reads the common form of a time, YYYY-MM-DD and HH:MM:SS, and
// reports false for any other.
func parseStamp(s []byte) (int64, bool) {
	if len(s) < 19 || s[4] != '-' || s[7] != '-' || (s[10] != 'T' && s[10] != ' ' && s[10] != 't') || s[13] != ':' || s[16] != ':' {
		return 0, false
	}
	century, ok1 := digits2(s[0:])
	years, ok2 := digits2(s[2:])
	month, ok3 := digits2(s[5:])
	day, ok4 := digits2(s[8:])
	hour, ok5 := digits2(s[11:])
	minute, ok6 := digits2(s[14:])
	second, ok7 := digits2(s[17:])
	year := century*100 + years
	digits := ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7
	if !digits || month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 {
		return 0, false
	}
	most := monthDays[month]
	if month == 2 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		most = 29
	}
	if day > most {
		return 0, false
	}
	at, nanos := 19, int64(0)
	if at < len(s) && (s[at] == '.' || s[at] == ',') {
		at++
		start, scale := at, int64(100_000_000)
		for ; at < len(s) && s[at] >= '0' && s[at] <= '9'; at++ {
			nanos += int64(s[at]-'0') * scale
			scale /= 10
		}
		if at == start {
			return 0, false
		}
	}
	offset := 0
	switch rest := s[at:]; {
	case len(rest) == 0, len(rest) == 1 && (rest[0] == 'Z' || rest[0] == 'z'):
	case (len(rest) == 6 && rest[3] == ':') || len(rest) == 5:
		if rest[0] != '+' && rest[0] != '-' {
			return 0, false
		}
		hours, ok1 := digits2(rest[1:])
		minutes, ok2 := digits2(rest[len(rest)-2:])
		if !ok1 || !ok2 || hours > 23 || minutes > 59 {
			return 0, false
		}
		if offset = hours*3600 + minutes*60; rest[0] == '-' {
			offset = -offset
		}
	default:
		return 0, false
	}
	// The days from the epoch to a civil date, counted in eras of 400
	// years that start in March.
	y := year
	if month <= 2 {
		y--
	}
	era := y / 400
	yoe := y - era*400
	m := month - 3
	if month <= 2 {
		m = month + 9
	}
	doy := (153*m+2)/5 + day - 1
	days := int64(era*146097 + yoe*365 + yoe/4 - yoe/100 + doy - 719468)
	seconds := days*86400 + int64(hour*3600+minute*60+second) - int64(offset)

	return seconds*1e9 + nanos, true
}
