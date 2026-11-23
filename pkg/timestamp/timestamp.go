// Package timestamp reads the times that log text writes, in three forms:
//
//   - RFC 3339: 2025-01-15T08:00:00.105Z, with T, t or a space between the
//     date and the time of day, and Z, z, an offset such as +02:00 or +0200,
//     or no zone, which is UTC;
//   - syslog: Jan 15 08:00:00, Jan  5 08:00:00 or Jan 5 08:00:00, in UTC;
//   - access log: 15/Jan/2025:08:00:00 +0000, without the offset in UTC.
//
// In each form the seconds may have a fraction after a point or a comma,
// with any number of digits; digits past the nanosecond are dropped.
package timestamp

import "time"

// scanBytes is how far into a line Find looks for the start of a time.
const scanBytes = 100

// startsAfter marks the bytes after which Find looks for a time.
var startsAfter = [256]bool{' ': true, '\t': true, '"': true, '\'': true, '[': true, '(': true, '>': true, '=': true}

// Find returns the first time in line, in nanoseconds since the Unix epoch.
// It looks for one at the start of the line and after each space, tab,
// quote, '[', '(', '>' or '=' in its first 100 bytes. year is the year of a
// syslog time, as At takes it.
func Find(line string, year int) (int64, bool) {
	end := min(len(line), scanBytes)
	for at := 0; at < end; at++ {
		if at > 0 && !startsAfter[line[at-1]] {
			continue
		}
		if t, _, ok := At(line[at:], year); ok {
			return t, true
		}
	}

	return 0, false
}

// Parse reads s as a time that fills it, in nanoseconds since the Unix
// epoch; a syslog time is in the current year.
func Parse(s string) (int64, bool) {
	t, n, ok := At(s, 0)

	return t, ok && n == len(s)
}

// At reads the time at the start of s and returns it in nanoseconds since
// the Unix epoch with the length of its text. A syslog time, which names no
// year, is in year, or in the current year when year is 0.
func At(s string, year int) (int64, int, bool) {
	// RFC 3339, the form of most times, is read here without a further call.
	if len(s) < 19 || s[4] != '-' {
		return otherForm(s, year)
	}
	if s[7] != '-' || (s[10] != 'T' && s[10] != ' ' && s[10] != 't') || s[13] != ':' || s[16] != ':' {
		return 0, 0, false
	}
	century, ok1 := digits2(s[0:])
	years, ok2 := digits2(s[2:])
	month, ok3 := digits2(s[5:])
	day, ok4 := digits2(s[8:])
	hour, ok5 := digits2(s[11:])
	minute, ok6 := digits2(s[14:])
	second, ok7 := digits2(s[17:])
	y := century*100 + years
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6 && ok7) || !validDate(y, month, day) || hour > 23 || minute > 59 || second > 59 {
		return 0, 0, false
	}
	nanos, at := fraction(s, 19)
	offset := 0
	if at < len(s) {
		switch c := s[at]; c {
		case 'Z', 'z':
			at++
		case '+', '-':
			// utcOffset, written out here to save the call.
			width, minutesAt := 5, at+3
			if at+6 <= len(s) && s[at+3] == ':' {
				width, minutesAt = 6, at+4
			}
			if at+width > len(s) {
				break
			}
			hours, okh := digits2(s[at+1:])
			minutes, okm := digits2(s[minutesAt:])
			if !okh || !okm {
				break
			}
			if hours > 23 || minutes > 59 {
				return 0, 0, false
			}
			if offset = hours*3600 + minutes*60; c == '-' {
				offset = -offset
			}
			at += width
		}
	}

	return unixNanos(y, month, day, hour*3600+minute*60+second-offset, nanos), at, true
}

// otherForm reads a syslog or an access log time at the start of s.
func otherForm(s string, year int) (int64, int, bool) {
	// The shortest form is a syslog time such as "Jan 5 08:00:00".
	if len(s) < 14 {
		return 0, 0, false
	}
	if c := s[0]; c >= '0' && c <= '9' {
		return accessLog(s)
	}

	return syslog(s, year)
}

// accessLog reads 02/Jan/2006:15:04:05 -0700, the time of the common and
// combined log formats.
func accessLog(s string) (int64, int, bool) {
	if len(s) < 20 || s[2] != '/' || s[6] != '/' || s[11] != ':' || s[14] != ':' || s[17] != ':' {
		return 0, 0, false
	}
	day, ok1 := digits2(s[0:])
	month := monthOf(s[3:6])
	century, ok2 := digits2(s[7:])
	years, ok3 := digits2(s[9:])
	hour, ok4 := digits2(s[12:])
	minute, ok5 := digits2(s[15:])
	second, ok6 := digits2(s[18:])
	year := century*100 + years
	if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) || !validDate(year, month, day) || hour > 23 || minute > 59 || second > 59 {
		return 0, 0, false
	}
	nanos, at := fraction(s, 20)
	offset := 0
	if at+1 < len(s) && s[at] == ' ' && (s[at+1] == '+' || s[at+1] == '-') {
		off, next, ok := utcOffset(s, at+1)
		if !ok {
			return 0, 0, false
		}
		if next > at+1 {
			offset, at = off, next
		}
	}

	return unixNanos(year, month, day, hour*3600+minute*60+second-offset, nanos), at, true
}

// syslog reads Jan _2 15:04:05, the time of BSD syslog (RFC 3164).
func syslog(s string, year int) (int64, int, bool) {
	month := monthOf(s[0:3])
	if month == 0 || s[3] != ' ' {
		return 0, 0, false
	}
	var day, at int
	switch d4, d5 := s[4]-'0', s[5]-'0'; {
	case s[4] == ' ' && d5 <= 9:
		day, at = int(d5), 6
	case d4 <= 9 && d5 <= 9:
		day, at = int(d4)*10+int(d5), 6
	case d4 <= 9:
		day, at = int(d4), 5
	default:
		return 0, 0, false
	}
	if len(s) < at+9 || s[at] != ' ' || s[at+3] != ':' || s[at+6] != ':' {
		return 0, 0, false
	}
	hour, ok1 := digits2(s[at+1:])
	minute, ok2 := digits2(s[at+4:])
	second, ok3 := digits2(s[at+7:])
	if !(ok1 && ok2 && ok3) || hour > 23 || minute > 59 || second > 59 {
		return 0, 0, false
	}
	if year == 0 {
		year = time.Now().Year()
	}
	if !validDate(year, month, day) {
		return 0, 0, false
	}
	nanos, at := fraction(s, at+9)

	return unixNanos(year, month, day, hour*3600+minute*60+second, nanos), at, true
}

func digits2(s string) (int, bool) {
	a, b := s[0]-'0', s[1]-'0'

	return int(a)*10 + int(b), a <= 9 && b <= 9
}

var monthDays = [13]int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}

func validDate(year, month, day int) bool {
	if month < 1 || month > 12 || day < 1 {
		return false
	}
	most := monthDays[month]
	if month == 2 && year%4 == 0 && (year%100 != 0 || year%400 == 0) {
		most = 29
	}

	return day <= most
}

// monthOf returns the number of a month from its English abbreviation,
// or 0 for any other text.
func monthOf(m string) int {
	switch m {
	case "Jan":
		return 1
	case "Feb":
		return 2
	case "Mar":
		return 3
	case "Apr":
		return 4
	case "May":
		return 5
	case "Jun":
		return 6
	case "Jul":
		return 7
	case "Aug":
		return 8
	case "Sep":
		return 9
	case "Oct":
		return 10
	case "Nov":
		return 11
	case "Dec":
		return 12
	}

	return 0
}

// fraction reads the fraction of a second at s[at:], a point or a comma
// and digits, and returns it in nanoseconds with where the text after it
// starts; without one it returns 0 and at.
func fraction(s string, at int) (int64, int) {
	if at >= len(s) || (s[at] != '.' && s[at] != ',') {
		return 0, at
	}
	nanos, scale, i := int64(0), int64(100_000_000), at+1
	for ; i < len(s) && s[i]-'0' <= 9; i++ {
		nanos += int64(s[i]-'0') * scale
		scale /= 10
	}
	if i == at+1 {
		return 0, at
	}

	return nanos, i
}

// utcOffset reads an offset from UTC at s[at:], a sign and hh:mm or hhmm,
// and returns it in seconds east of UTC with where the text after it
// starts. Text that is not an offset leaves at where it is; ok is false
// for an offset out of range.
func utcOffset(s string, at int) (offset, next int, ok bool) {
	var hours, minutes, width int
	var okh, okm bool
	switch {
	case at+6 <= len(s) && s[at+3] == ':':
		hours, okh = digits2(s[at+1:])
		minutes, okm = digits2(s[at+4:])
		width = 6
	case at+5 <= len(s):
		hours, okh = digits2(s[at+1:])
		minutes, okm = digits2(s[at+3:])
		width = 5
	}
	if !okh || !okm {
		return 0, at, true
	}
	if hours > 23 || minutes > 59 {
		return 0, 0, false
	}
	if offset = hours*3600 + minutes*60; s[at] == '-' {
		offset = -offset
	}

	return offset, at + width, true
}

// unixNanos returns the nanoseconds from the Unix epoch to a civil date in
// UTC and seconds from its midnight, counting the days in eras of 400 years
// that start in March.
func unixNanos(year, month, day, seconds int, nanos int64) int64 {
	m := month - 3
	if month <= 2 {
		year--
		m = month + 9
	}
	era := year / 400
	yoe := year - era*400
	days := era*146097 + yoe*365 + yoe/4 - yoe/100 + (153*m+2)/5 + day - 1 - 719468

	return (int64(days)*86400+int64(seconds))*1e9 + nanos
}
