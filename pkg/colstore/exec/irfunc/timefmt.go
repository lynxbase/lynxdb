package irfunc

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// secondsTime is the instant f seconds after the epoch.
func secondsTime(f float64) time.Time {
	sec, frac := math.Modf(f)
	return time.Unix(int64(sec), int64(frac*1e9)).UTC()
}

// appendPadN writes n as fmt's %0<width>d does.
func appendPadN(dst []byte, n int64, width int) []byte {
	if n < 0 {
		dst = append(dst, '-')
		n = -n
		width--
	}
	var digits [20]byte
	s := strconv.AppendInt(digits[:0], n, 10)
	for i := len(s); i < width; i++ {
		dst = append(dst, '0')
	}

	return append(dst, s...)
}

// appendSpace2 writes n as fmt's %2d does.
func appendSpace2(dst []byte, n int64) []byte {
	if n >= 0 && n < 10 {
		dst = append(dst, ' ')
	}

	return strconv.AppendInt(dst, n, 10)
}

func ordinalSuffix(d int) string {
	if d%100 >= 11 && d%100 <= 13 {
		return "th"
	}
	switch d % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	}

	return "th"
}

func hour12(h int) int {
	h %= 12
	if h == 0 {
		return 12
	}

	return h
}

func ampm(h int) string {
	if h < 12 {
		return "AM"
	}

	return "PM"
}

// mysqlFormat writes t with MySQL DATE_FORMAT specifiers.
func mysqlFormat(b []byte, t time.Time, format string) []byte {
	for i := 0; i < len(format); i++ {
		ch := format[i]
		if ch != '%' || i+1 >= len(format) {
			b = append(b, ch)
			continue
		}
		i++
		switch format[i] {
		case 'a':
			b = append(b, t.Weekday().String()[:3]...)
		case 'b':
			b = append(b, t.Month().String()[:3]...)
		case 'c':
			b = strconv.AppendInt(b, int64(t.Month()), 10)
		case 'D':
			b = strconv.AppendInt(b, int64(t.Day()), 10)
			b = append(b, ordinalSuffix(t.Day())...)
		case 'd':
			b = appendPad2(b, int64(t.Day()))
		case 'e':
			b = strconv.AppendInt(b, int64(t.Day()), 10)
		case 'f':
			b = appendPadN(b, int64(t.Nanosecond()/1000), 6)
		case 'H':
			b = appendPad2(b, int64(t.Hour()))
		case 'h', 'I':
			b = appendPad2(b, int64(hour12(t.Hour())))
		case 'i':
			b = appendPad2(b, int64(t.Minute()))
		case 'j':
			b = appendPadN(b, int64(t.YearDay()), 3)
		case 'k':
			b = strconv.AppendInt(b, int64(t.Hour()), 10)
		case 'l':
			b = strconv.AppendInt(b, int64(hour12(t.Hour())), 10)
		case 'M':
			b = append(b, t.Month().String()...)
		case 'm':
			b = appendPad2(b, int64(t.Month()))
		case 'p':
			b = append(b, ampm(t.Hour())...)
		case 'r':
			b = appendPad2(b, int64(hour12(t.Hour())))
			b = append(b, ':')
			b = appendPad2(b, int64(t.Minute()))
			b = append(b, ':')
			b = appendPad2(b, int64(t.Second()))
			b = append(b, ' ')
			b = append(b, ampm(t.Hour())...)
		case 'S', 's':
			b = appendPad2(b, int64(t.Second()))
		case 'T':
			b = appendPad2(b, int64(t.Hour()))
			b = append(b, ':')
			b = appendPad2(b, int64(t.Minute()))
			b = append(b, ':')
			b = appendPad2(b, int64(t.Second()))
		case 'U':
			_, w := calcWeek(t, 4)
			b = appendPad2(b, int64(w))
		case 'u':
			_, w := calcWeek(t, 1)
			b = appendPad2(b, int64(w))
		case 'V':
			_, w := calcWeek(t, 6)
			b = appendPad2(b, int64(w))
		case 'v':
			_, w := calcWeek(t, 3)
			b = appendPad2(b, int64(w))
		case 'W':
			b = append(b, t.Weekday().String()...)
		case 'w':
			b = strconv.AppendInt(b, int64(t.Weekday()), 10)
		case 'X':
			y, _ := calcWeek(t, 6)
			b = appendPadN(b, int64(y), 4)
		case 'x':
			y, _ := calcWeek(t, 3)
			b = appendPadN(b, int64(y), 4)
		case 'Y':
			b = appendPadN(b, int64(t.Year()), 4)
		case 'y':
			b = appendPad2(b, int64(t.Year()%100))
		case '%':
			b = append(b, '%')
		default:
			b = append(b, format[i])
		}
	}

	return b
}

// posixFormat writes t with POSIX strftime specifiers.
func posixFormat(b []byte, t time.Time, format string) []byte {
	for i := 0; i < len(format); i++ {
		ch := format[i]
		if ch != '%' || i+1 >= len(format) {
			b = append(b, ch)
			continue
		}
		i++
		switch format[i] {
		case 'a':
			b = append(b, t.Weekday().String()[:3]...)
		case 'A':
			b = append(b, t.Weekday().String()...)
		case 'b', 'h':
			b = append(b, t.Month().String()[:3]...)
		case 'B':
			b = append(b, t.Month().String()...)
		case 'c':
			b = t.AppendFormat(b, "Mon Jan  2 15:04:05 2006")
		case 'C':
			b = appendPad2(b, int64(t.Year()/100))
		case 'd':
			b = appendPad2(b, int64(t.Day()))
		case 'D':
			b = t.AppendFormat(b, "01/02/06")
		case 'e':
			b = appendSpace2(b, int64(t.Day()))
		case 'F':
			b = t.AppendFormat(b, "2006-01-02")
		case 'H':
			b = appendPad2(b, int64(t.Hour()))
		case 'I':
			b = appendPad2(b, int64(hour12(t.Hour())))
		case 'j':
			b = appendPadN(b, int64(t.YearDay()), 3)
		case 'k':
			b = appendSpace2(b, int64(t.Hour()))
		case 'l':
			b = appendSpace2(b, int64(hour12(t.Hour())))
		case 'm':
			b = appendPad2(b, int64(t.Month()))
		case 'M':
			b = appendPad2(b, int64(t.Minute()))
		case 'N', 'Q':
			b = appendPadN(b, int64(t.Nanosecond()/1e6), 3)
		case 'p':
			b = append(b, ampm(t.Hour())...)
		case 's':
			b = strconv.AppendInt(b, t.Unix(), 10)
		case 'S':
			b = appendPad2(b, int64(t.Second()))
		case 'T':
			b = t.AppendFormat(b, "15:04:05")
		case 'u':
			wd := int64(t.Weekday())
			if wd == 0 {
				wd = 7
			}
			b = strconv.AppendInt(b, wd, 10)
		case 'w':
			b = strconv.AppendInt(b, int64(t.Weekday()), 10)
		case 'y':
			b = appendPad2(b, int64(t.Year()%100))
		case 'Y':
			b = appendPadN(b, int64(t.Year()), 4)
		case 'z':
			b = append(b, "+0000"...)
		case 'Z':
			b = append(b, "UTC"...)
		case '%':
			b = append(b, '%')
		default:
			b = append(b, '%', format[i])
		}
	}

	return b
}

var monthNames = []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}

var monthShort = func() []string {
	out := make([]string, 12)
	for i, m := range monthNames {
		out[i] = m[:3]
	}
	return out
}()

var meridiem = []string{"AM", "PM"}

// mysqlParse parses s with MySQL STR_TO_DATE specifiers.
func mysqlParse(s, format string) (time.Time, bool) {
	year, month, day := 0, 1, 1
	hour, minute, second, nanos := 0, 0, 0, 0
	pm, hasAMPM := false, false
	pos := 0
	num := func(maxDigits int) (int, bool) {
		start := pos
		for pos < len(s) && pos-start < maxDigits && s[pos] >= '0' && s[pos] <= '9' {
			pos++
		}
		if pos == start {
			return 0, false
		}
		n, _ := strconv.Atoi(s[start:pos])
		return n, true
	}
	word := func(names []string) (int, bool) {
		for i, n := range names {
			if len(s)-pos >= len(n) && strings.EqualFold(s[pos:pos+len(n)], n) {
				pos += len(n)
				return i, true
			}
		}
		return 0, false
	}
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i+1 >= len(format) {
			if pos >= len(s) || s[pos] != format[i] {
				return time.Time{}, false
			}
			pos++
			continue
		}
		i++
		var ok bool
		switch format[i] {
		case 'Y':
			year, ok = num(4)
		case 'y':
			year, ok = num(2)
			if year < 70 {
				year += 2000
			} else {
				year += 1900
			}
		case 'm', 'c':
			month, ok = num(2)
		case 'd', 'e':
			day, ok = num(2)
		case 'H', 'k', 'h', 'I', 'l':
			hour, ok = num(2)
		case 'i':
			minute, ok = num(2)
		case 'S', 's':
			second, ok = num(2)
		case 'f':
			start := pos
			var frac int
			frac, ok = num(9)
			if ok {
				nanos = frac * int(math.Pow10(9-(pos-start)))
			}
		case 'p':
			var idx int
			idx, ok = word(meridiem)
			pm, hasAMPM = idx == 1, true
		case 'M':
			var idx int
			idx, ok = word(monthNames)
			month = idx + 1
		case 'b':
			var idx int
			idx, ok = word(monthShort)
			month = idx + 1
		case 'j':
			var doy int
			doy, ok = num(3)
			month, day = 1, doy
		case 'T':
			hour, ok = num(2)
			if ok && pos < len(s) && s[pos] == ':' {
				pos++
				minute, ok = num(2)
				if ok && pos < len(s) && s[pos] == ':' {
					pos++
					second, ok = num(2)
				}
			}
		case '%':
			ok = pos < len(s) && s[pos] == '%'
			pos++
		default:
			return time.Time{}, false
		}
		if !ok {
			return time.Time{}, false
		}
	}
	if hasAMPM {
		hour %= 12
		if pm {
			hour += 12
		}
	}
	if month < 1 || month > 12 || day < 1 || hour > 23 || minute > 59 || second > 59 {
		return time.Time{}, false
	}

	return time.Date(year, time.Month(month), 1, hour, minute, second, nanos, time.UTC).AddDate(0, 0, day-1), true
}

// calcDaynr is MySQL's day number of a date: 0000-01-01 is day 1.
func calcDaynr(year, month, day int64) int64 {
	if year == 0 && month == 0 {
		return 0
	}
	delsum := 365*year + 31*(month-1) + day
	if month <= 2 {
		year--
	} else {
		delsum -= (month*4 + 23) / 10
	}
	temp := ((year/100 + 1) * 3) / 4

	return delsum + year/4 - temp
}

func toDays(t time.Time) int64 {
	y, m, d := t.Date()
	return calcDaynr(int64(y), int64(m), int64(d))
}

var unixDaynr = calcDaynr(1970, 1, 1)

func fromDays(n int64) time.Time {
	return time.Unix(0, 0).UTC().AddDate(0, 0, int(n-unixDaynr))
}

// weekMode maps a WEEK() mode argument to calcWeek behaviour flags.
func weekMode(mode int) int {
	f := mode & 7
	if f&1 == 0 {
		f ^= 4
	}

	return f
}

// calcWeek implements MySQL's week numbering; behaviour holds the flags
// monday-first (1), week-year (2) and first-weekday (4).
func calcWeek(t time.Time, behaviour int) (year, week int) {
	mondayFirst := behaviour&1 != 0
	weekYear := behaviour&2 != 0
	firstWeekday := behaviour&4 != 0
	y := t.Year()
	day := toDays(t)
	first := toDays(time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC))
	wd := weekdayNum(first, mondayFirst)
	if t.Month() == 1 && t.Day() <= 7-wd {
		if !weekYear && ((firstWeekday && wd != 0) || (!firstWeekday && wd >= 4)) {
			return y, 0
		}
		weekYear = true
		y--
		daysInYear := int64(365)
		if isLeap(y) {
			daysInYear = 366
		}
		first -= daysInYear
		wd = (wd + 53*7 - int(daysInYear)) % 7
	}
	var days int64
	if (firstWeekday && wd != 0) || (!firstWeekday && wd >= 4) {
		days = day - (first + int64(7-wd))
	} else {
		days = day - (first - int64(wd))
	}
	if weekYear && days >= 52*7 {
		wd = (wd + int(daysInYearOf(y))) % 7
		if (!firstWeekday && wd < 4) || (firstWeekday && wd == 0) {
			return y + 1, 1
		}
	}

	return y, int(days/7) + 1
}

func daysInYearOf(y int) int64 {
	if isLeap(y) {
		return 366
	}

	return 365
}

func isLeap(y int) bool { return y%4 == 0 && (y%100 != 0 || y%400 == 0) }

// weekdayNum numbers a MySQL day number's weekday from 0, starting at
// Monday or Sunday.
func weekdayNum(dayNr int64, mondayFirst bool) int {
	if mondayFirst {
		return int((dayNr + 5) % 7)
	}

	return int((dayNr + 6) % 7)
}
