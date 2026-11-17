package vm

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
)

func dayStart(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// timeArg interprets a value as an instant; a time-of-day duration is taken
// on the Unix epoch day.
func timeArg(v event.Value) (time.Time, bool) {
	if t, ok := timeOf(v); ok {
		return t, true
	}
	if v.Type() == event.FieldTypeDuration {
		return time.Unix(0, 0).UTC().Add(v.AsDuration()), true
	}
	return time.Time{}, false
}

func timeFn(f func(t time.Time) event.Value) NativeFunc {
	return strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		if !ok {
			return event.NullValue()
		}
		return f(t)
	})
}

func clockFn(f func(d time.Duration) event.Value) NativeFunc {
	return strict(func(a []event.Value) event.Value {
		d, ok := timeOfDay(a[0])
		if !ok {
			return event.NullValue()
		}
		return f(d)
	})
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

func loadZone(name string) (*time.Location, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	if name[0] == '+' || name[0] == '-' {
		sign := 1
		if name[0] == '-' {
			sign = -1
		}
		h, m, ok := strings.Cut(name[1:], ":")
		hh, err1 := strconv.Atoi(h)
		mm := 0
		var err2 error
		if ok {
			mm, err2 = strconv.Atoi(m)
		}
		if err1 != nil || err2 != nil || hh > 14 || mm > 59 {
			return nil, false
		}
		return time.FixedZone(name, sign*(hh*3600+mm*60)), true
	}
	loc, err := time.LoadLocation(name)
	return loc, err == nil
}

// wallIn rewrites t's UTC wall clock as the wall clock of the same instant
// read in zone from, converted to zone to.
func wallIn(t time.Time, from, to *time.Location) time.Time {
	inFrom := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), from)
	w := inFrom.In(to)
	return time.Date(w.Year(), w.Month(), w.Day(), w.Hour(), w.Minute(), w.Second(), w.Nanosecond(), time.UTC)
}

func addCalendar(t time.Time, n int64, unit string) (time.Time, bool) {
	switch unit {
	case "microsecond":
		return t.Add(time.Duration(n) * time.Microsecond), true
	case "millisecond":
		return t.Add(time.Duration(n) * time.Millisecond), true
	case "second":
		return t.Add(time.Duration(n) * time.Second), true
	case "minute":
		return t.Add(time.Duration(n) * time.Minute), true
	case "hour":
		return t.Add(time.Duration(n) * time.Hour), true
	case "day":
		return t.AddDate(0, 0, int(n)), true
	case "week":
		return t.AddDate(0, 0, 7*int(n)), true
	case "month":
		return addMonthsClamped(t, int(n)), true
	case "quarter":
		return addMonthsClamped(t, 3*int(n)), true
	case "year":
		return addMonthsClamped(t, 12*int(n)), true
	}
	return t, false
}

// addMonthsClamped adds months, clamping the day to the target month's
// last day as SQL date arithmetic does.
func addMonthsClamped(t time.Time, months int) time.Time {
	y, m, d := t.Date()
	first := time.Date(y, m+time.Month(months), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	last := first.AddDate(0, 1, -1).Day()
	if d > last {
		d = last
	}
	return time.Date(first.Year(), first.Month(), d, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
}

func diffUnits(a, b time.Time, unit string) (int64, bool) {
	d := b.Sub(a)
	switch unit {
	case "microsecond":
		return int64(d / time.Microsecond), true
	case "millisecond":
		return int64(d / time.Millisecond), true
	case "second":
		return int64(d / time.Second), true
	case "minute":
		return int64(d / time.Minute), true
	case "hour":
		return int64(d / time.Hour), true
	case "day":
		return int64(d / (24 * time.Hour)), true
	case "week":
		return int64(d / (7 * 24 * time.Hour)), true
	case "month", "quarter", "year":
		months := int64(b.Year()-a.Year())*12 + int64(b.Month()-a.Month())
		if months > 0 && addMonthsClamped(a, int(months)).After(b) {
			months--
		} else if months < 0 && addMonthsClamped(a, int(months)).Before(b) {
			months++
		}
		switch unit {
		case "quarter":
			return months / 3, true
		case "year":
			return months / 12, true
		}
		return months, true
	}
	return 0, false
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

func registerDatetimeFuncs() {
	irFuncs["now"] = func(c *irCompiler, _ *ir.Call) error {
		c.constant(event.TimestampValue(c.now))
		return nil
	}
	irFuncs["sysdate"] = natively(func([]event.Value) event.Value { return event.TimestampValue(time.Now().UTC()) })
	irFuncs["curdate"] = func(c *irCompiler, _ *ir.Call) error {
		c.constant(event.TimestampValue(dayStart(c.now)))
		return nil
	}
	irFuncs["curtime"] = func(c *irCompiler, _ *ir.Call) error {
		c.constant(event.DurationValue(c.now.Sub(dayStart(c.now))))
		return nil
	}
	irFuncs["unix_timestamp"] = func(c *irCompiler, n *ir.Call) error {
		if len(n.Args) == 0 {
			c.constant(event.FloatValue(float64(c.now.UnixNano()) / 1e9))
			return nil
		}
		return natively(timeFn(func(t time.Time) event.Value { return event.FloatValue(float64(t.UnixNano()) / 1e9) }))(c, n)
	}
	irFuncs["date"] = natively(timeFn(func(t time.Time) event.Value { return event.TimestampValue(dayStart(t)) }))
	irFuncs["time"] = natively(clockFn(func(d time.Duration) event.Value { return event.DurationValue(d) }))
	irFuncs["timestamp"] = natively(strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		if !ok {
			return event.NullValue()
		}
		if len(a) > 1 {
			d, ok := timeOfDay(a[1])
			if !ok {
				return event.NullValue()
			}
			t = t.Add(d)
		}
		return event.TimestampValue(t)
	}))
	irFuncs["datetime"] = natively(strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		if !ok {
			return event.NullValue()
		}
		if len(a) > 1 {
			tz, _ := str(a[1])
			loc, ok := loadZone(tz)
			if !ok {
				return event.NullValue()
			}
			t = wallIn(t, time.UTC, loc)
		}
		return event.TimestampValue(t)
	}))
	irFuncs["date_add"] = natively(strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		n, ok2 := numInt(a[1])
		unit, ok3 := str(a[2])
		if !ok || !ok2 || !ok3 {
			return event.NullValue()
		}
		r, ok := addCalendar(t, n, unit)
		if !ok {
			return event.NullValue()
		}
		return event.TimestampValue(r)
	}))
	addTime := func(sign time.Duration) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			d, ok := timeOfDay(a[1])
			if !ok {
				return event.NullValue()
			}
			if a[0].Type() == event.FieldTypeDuration {
				return event.DurationValue(a[0].AsDuration() + sign*d)
			}
			t, ok := timeArg(a[0])
			if !ok {
				return event.NullValue()
			}
			return event.TimestampValue(t.Add(sign * d))
		})
	}
	irFuncs["addtime"] = natively(addTime(1))
	irFuncs["subtime"] = natively(addTime(-1))
	irFuncs["convert_tz"] = natively(strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		f, _ := str(a[1])
		to, _ := str(a[2])
		from, ok1 := loadZone(f)
		dst, ok2 := loadZone(to)
		if !ok || !ok1 || !ok2 {
			return event.NullValue()
		}
		return event.TimestampValue(wallIn(t, from, dst))
	}))
	formatWith := func(f func(time.Time, string) (string, bool)) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			t, ok := timeArg(a[0])
			format, ok2 := str(a[1])
			if !ok || !ok2 {
				return event.NullValue()
			}
			s, ok := f(t, format)
			if !ok {
				return event.NullValue()
			}
			return event.StringValue(s)
		})
	}
	irFuncs["date_format"] = natively(formatWith(mysqlFormat))
	irFuncs["time_format"] = natively(formatWith(mysqlFormat))
	irFuncs["strftime"] = natively(strict(func(a []event.Value) event.Value {
		t, ok := timeArg(a[0])
		if !ok {
			if f, isNum := numFloat(a[0]); isNum && a[0].Type() != event.FieldTypeString {
				sec, frac := math.Modf(f)
				t, ok = time.Unix(int64(sec), int64(frac*1e9)).UTC(), true
			}
		}
		format, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.StringValue(posixFormat(t, format))
	}))
	irFuncs["str_to_date"] = natively(strict(func(a []event.Value) event.Value {
		s, ok := str(a[0])
		format, ok2 := str(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		t, ok := mysqlParse(s, format)
		if !ok {
			return event.NullValue()
		}
		return event.TimestampValue(t)
	}))
	irFuncs["get_format"] = natively(strict(func(a []event.Value) event.Value {
		kind, _ := str(a[0])
		locale, _ := str(a[1])
		f, ok := getFormats[strings.ToLower(kind)][strings.ToUpper(locale)]
		if !ok {
			return event.NullValue()
		}
		return event.StringValue(f)
	}))
	irFuncs["datediff"] = natively(strict(func(a []event.Value) event.Value {
		x, ok := timeArg(a[0])
		y, ok2 := timeArg(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.IntValue(toDays(x) - toDays(y))
	}))
	irFuncs["timediff"] = natively(strict(func(a []event.Value) event.Value {
		if a[0].Type() == event.FieldTypeTimestamp || a[1].Type() == event.FieldTypeTimestamp {
			x, ok := timeArg(a[0])
			y, ok2 := timeArg(a[1])
			if !ok || !ok2 {
				return event.NullValue()
			}
			return event.DurationValue(x.Sub(y))
		}
		x, ok := timeOfDay(a[0])
		y, ok2 := timeOfDay(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.DurationValue(x - y)
	}))
	irFuncs["timestampadd"] = natively(strict(func(a []event.Value) event.Value {
		unit, _ := str(a[0])
		n, ok := numInt(a[1])
		t, ok2 := timeArg(a[2])
		if !ok || !ok2 {
			return event.NullValue()
		}
		r, ok := addCalendar(t, n, strings.ToLower(unit))
		if !ok {
			return event.NullValue()
		}
		return event.TimestampValue(r)
	}))
	irFuncs["timestampdiff"] = natively(strict(func(a []event.Value) event.Value {
		unit, _ := str(a[0])
		x, ok := timeArg(a[1])
		y, ok2 := timeArg(a[2])
		if !ok || !ok2 {
			return event.NullValue()
		}
		d, ok := diffUnits(x, y, strings.ToLower(unit))
		if !ok {
			return event.NullValue()
		}
		return event.IntValue(d)
	}))
	part := func(f func(t time.Time) int64) irEmitter {
		return natively(timeFn(func(t time.Time) event.Value { return event.IntValue(f(t)) }))
	}
	irFuncs["year"] = part(func(t time.Time) int64 { return int64(t.Year()) })
	irFuncs["quarter"] = part(func(t time.Time) int64 { return int64((t.Month()-1)/3 + 1) })
	irFuncs["month"] = part(func(t time.Time) int64 { return int64(t.Month()) })
	irFuncs["day"] = part(func(t time.Time) int64 { return int64(t.Day()) })
	irFuncs["dayofweek"] = part(func(t time.Time) int64 { return int64(t.Weekday()) + 1 })
	irFuncs["weekday"] = part(func(t time.Time) int64 { return int64((t.Weekday() + 6) % 7) })
	irFuncs["dayofyear"] = part(func(t time.Time) int64 { return int64(t.YearDay()) })
	irFuncs["to_days"] = part(toDays)
	irFuncs["to_seconds"] = part(func(t time.Time) int64 {
		return (toDays(t)-1)*86400 + int64(t.Sub(dayStart(t))/time.Second)
	})
	clock := func(f func(d time.Duration) int64) irEmitter {
		return natively(clockFn(func(d time.Duration) event.Value { return event.IntValue(f(d)) }))
	}
	irFuncs["hour"] = clock(func(d time.Duration) int64 { return int64(d / time.Hour) })
	irFuncs["minute"] = clock(func(d time.Duration) int64 { return int64(d % time.Hour / time.Minute) })
	irFuncs["minute_of_day"] = clock(func(d time.Duration) int64 { return int64(d / time.Minute) })
	irFuncs["second"] = clock(func(d time.Duration) int64 { return int64(d % time.Minute / time.Second) })
	irFuncs["microsecond"] = clock(func(d time.Duration) int64 { return int64(d % time.Second / time.Microsecond) })
	irFuncs["time_to_sec"] = clock(func(d time.Duration) int64 { return int64(d / time.Second) })
	irFuncs["dayname"] = natively(timeFn(func(t time.Time) event.Value { return event.StringValue(t.Weekday().String()) }))
	irFuncs["monthname"] = natively(timeFn(func(t time.Time) event.Value { return event.StringValue(t.Month().String()) }))
	irFuncs["last_day"] = natively(timeFn(func(t time.Time) event.Value {
		return event.TimestampValue(time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC))
	}))
	weekFn := func(yearweek bool) NativeFunc {
		return strict(func(a []event.Value) event.Value {
			t, ok := timeArg(a[0])
			if !ok {
				return event.NullValue()
			}
			mode := int64(0)
			if len(a) > 1 {
				if mode, ok = numInt(a[1]); !ok || mode < 0 || mode > 7 {
					return event.NullValue()
				}
			}
			if yearweek {
				y, w := calcWeek(t, weekMode(int(mode))|2)
				return event.IntValue(int64(y*100 + w))
			}
			_, w := calcWeek(t, weekMode(int(mode)))
			return event.IntValue(int64(w))
		})
	}
	irFuncs["week"] = natively(weekFn(false))
	irFuncs["yearweek"] = natively(weekFn(true))
	irFuncs["extract"] = func(c *irCompiler, n *ir.Call) error {
		p, ok := constStringArg(n, 0)
		if !ok {
			return fmt.Errorf("vm: extract needs a constant part")
		}
		f, ok := extractParts[strings.ToLower(p)]
		if !ok {
			return fmt.Errorf("vm: unknown extract part %s", p)
		}
		if err := c.compile(n.Args[1]); err != nil {
			return err
		}
		c.native(timeFn(func(t time.Time) event.Value { return event.IntValue(f(t)) }), 1)
		return nil
	}
	irFuncs["from_days"] = natively(strict(func(a []event.Value) event.Value {
		n, ok := numInt(a[0])
		if !ok {
			return event.NullValue()
		}
		return event.TimestampValue(fromDays(n))
	}))
	irFuncs["from_unixtime"] = natively(strict(func(a []event.Value) event.Value {
		f, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		sec, frac := math.Modf(f)
		t := time.Unix(int64(sec), int64(frac*1e9)).UTC()
		if len(a) > 1 {
			format, _ := str(a[1])
			s, ok := mysqlFormat(t, format)
			if !ok {
				return event.NullValue()
			}
			return event.StringValue(s)
		}
		return event.TimestampValue(t)
	}))
	irFuncs["makedate"] = natively(strict(func(a []event.Value) event.Value {
		y, ok := numFloat(a[0])
		doy, ok2 := numFloat(a[1])
		if !ok || !ok2 || doy < 1 {
			return event.NullValue()
		}
		return event.TimestampValue(time.Date(int(math.Round(y)), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(math.Round(doy))-1))
	}))
	irFuncs["maketime"] = natively(strict(func(a []event.Value) event.Value {
		h, ok := numFloat(a[0])
		m, ok2 := numFloat(a[1])
		s, ok3 := numFloat(a[2])
		if !ok || !ok2 || !ok3 || m < 0 || m >= 60 || s < 0 || s >= 60 {
			return event.NullValue()
		}
		return event.DurationValue(time.Duration(math.Round(h))*time.Hour + time.Duration(math.Round(m))*time.Minute + time.Duration(s*float64(time.Second)))
	}))
	irFuncs["period_add"] = natively(strict(func(a []event.Value) event.Value {
		p, ok := numInt(a[0])
		n, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		months := periodMonths(p) + n
		return event.IntValue(months/12*100 + months%12 + 1)
	}))
	irFuncs["period_diff"] = natively(strict(func(a []event.Value) event.Value {
		p, ok := numInt(a[0])
		q, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return event.NullValue()
		}
		return event.IntValue(periodMonths(p) - periodMonths(q))
	}))
	irFuncs["sec_to_time"] = natively(strict(func(a []event.Value) event.Value {
		f, ok := numFloat(a[0])
		if !ok {
			return event.NullValue()
		}
		return event.DurationValue(time.Duration(f * float64(time.Second)))
	}))
	irFuncs["time_bucket"] = func(c *irCompiler, n *ir.Call) error {
		count, ok := constIntArg(n, 1)
		unit, ok2 := constStringArg(n, 2)
		if !ok || !ok2 || count <= 0 {
			return fmt.Errorf("vm: time_bucket needs a positive constant count and unit")
		}
		f, err := timeBucketer(count, unit)
		if err != nil {
			return err
		}
		if err := c.compile(n.Args[0]); err != nil {
			return err
		}
		c.native(strict(func(a []event.Value) event.Value {
			t, ok := timeArg(a[0])
			if !ok {
				return event.NullValue()
			}
			return event.TimestampValue(f(t))
		}), 1)
		return nil
	}
}

func timeBucketer(count int64, unit string) (func(time.Time) time.Time, error) {
	fixed := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}
	if d, ok := fixed[unit]; ok {
		width := time.Duration(count) * d
		return func(t time.Time) time.Time {
			ns := t.UnixNano()
			w := int64(width)
			q := ns / w
			if ns%w != 0 && ns < 0 {
				q--
			}
			return time.Unix(0, q*w).UTC()
		}, nil
	}
	months := map[string]int64{"mon": 1, "q": 3, "y": 12}
	m, ok := months[unit]
	if !ok {
		return nil, fmt.Errorf("vm: unknown time bucket unit %q", unit)
	}
	step := m * count
	return func(t time.Time) time.Time {
		idx := int64(t.Year())*12 + int64(t.Month()-1)
		b := idx / step * step
		if idx%step != 0 && idx < 0 {
			b -= step
		}
		return time.Date(int(b/12), time.Month(b%12+1), 1, 0, 0, 0, 0, time.UTC)
	}, nil
}

func periodMonths(p int64) int64 {
	y, m := p/100, p%100
	if y < 70 {
		y += 2000
	} else if y < 100 {
		y += 1900
	}
	return y*12 + m - 1
}

var extractParts = map[string]func(time.Time) int64{
	"microsecond": func(t time.Time) int64 { return int64(t.Nanosecond() / 1000) },
	"millisecond": func(t time.Time) int64 { return int64(t.Nanosecond() / 1e6) },
	"second":      func(t time.Time) int64 { return int64(t.Second()) },
	"minute":      func(t time.Time) int64 { return int64(t.Minute()) },
	"hour":        func(t time.Time) int64 { return int64(t.Hour()) },
	"day":         func(t time.Time) int64 { return int64(t.Day()) },
	"week":        func(t time.Time) int64 { _, w := calcWeek(t, weekMode(0)); return int64(w) },
	"month":       func(t time.Time) int64 { return int64(t.Month()) },
	"quarter":     func(t time.Time) int64 { return int64((t.Month()-1)/3 + 1) },
	"year":        func(t time.Time) int64 { return int64(t.Year()) },
	"second_microsecond": func(t time.Time) int64 {
		return int64(t.Second())*1e6 + int64(t.Nanosecond()/1000)
	},
	"minute_microsecond": func(t time.Time) int64 {
		return (int64(t.Minute())*100+int64(t.Second()))*1e6 + int64(t.Nanosecond()/1000)
	},
	"minute_second": func(t time.Time) int64 { return int64(t.Minute())*100 + int64(t.Second()) },
	"hour_microsecond": func(t time.Time) int64 {
		return (int64(t.Hour())*10000+int64(t.Minute())*100+int64(t.Second()))*1e6 + int64(t.Nanosecond()/1000)
	},
	"hour_second": func(t time.Time) int64 { return int64(t.Hour())*10000 + int64(t.Minute())*100 + int64(t.Second()) },
	"hour_minute": func(t time.Time) int64 { return int64(t.Hour())*100 + int64(t.Minute()) },
	"day_microsecond": func(t time.Time) int64 {
		return (int64(t.Day())*1000000+int64(t.Hour())*10000+int64(t.Minute())*100+int64(t.Second()))*1e6 + int64(t.Nanosecond()/1000)
	},
	"day_second": func(t time.Time) int64 {
		return int64(t.Day())*1000000 + int64(t.Hour())*10000 + int64(t.Minute())*100 + int64(t.Second())
	},
	"day_minute": func(t time.Time) int64 { return int64(t.Day())*10000 + int64(t.Hour())*100 + int64(t.Minute()) },
	"day_hour":   func(t time.Time) int64 { return int64(t.Day())*100 + int64(t.Hour()) },
	"year_month": func(t time.Time) int64 { return int64(t.Year())*100 + int64(t.Month()) },
}

var getFormats = map[string]map[string]string{
	"date":      {"USA": "%m.%d.%Y", "JIS": "%Y-%m-%d", "ISO": "%Y-%m-%d", "EUR": "%d.%m.%Y", "INTERNAL": "%Y%m%d"},
	"datetime":  {"USA": "%Y-%m-%d %H.%i.%s", "JIS": "%Y-%m-%d %H:%i:%s", "ISO": "%Y-%m-%d %H:%i:%s", "EUR": "%Y-%m-%d %H.%i.%s", "INTERNAL": "%Y%m%d%H%i%s"},
	"timestamp": {"USA": "%Y-%m-%d %H.%i.%s", "JIS": "%Y-%m-%d %H:%i:%s", "ISO": "%Y-%m-%d %H:%i:%s", "EUR": "%Y-%m-%d %H.%i.%s", "INTERNAL": "%Y%m%d%H%i%s"},
	"time":      {"USA": "%h:%i:%s %p", "JIS": "%H:%i:%s", "ISO": "%H:%i:%s", "EUR": "%H.%i.%s", "INTERNAL": "%H%i%s"},
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

// mysqlFormat formats t with MySQL DATE_FORMAT specifiers.
func mysqlFormat(t time.Time, format string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		ch := format[i]
		if ch != '%' || i+1 >= len(format) {
			b.WriteByte(ch)
			continue
		}
		i++
		switch format[i] {
		case 'a':
			b.WriteString(t.Weekday().String()[:3])
		case 'b':
			b.WriteString(t.Month().String()[:3])
		case 'c':
			b.WriteString(strconv.Itoa(int(t.Month())))
		case 'D':
			b.WriteString(strconv.Itoa(t.Day()) + ordinalSuffix(t.Day()))
		case 'd':
			fmt.Fprintf(&b, "%02d", t.Day())
		case 'e':
			b.WriteString(strconv.Itoa(t.Day()))
		case 'f':
			fmt.Fprintf(&b, "%06d", t.Nanosecond()/1000)
		case 'H':
			fmt.Fprintf(&b, "%02d", t.Hour())
		case 'h', 'I':
			fmt.Fprintf(&b, "%02d", hour12(t.Hour()))
		case 'i':
			fmt.Fprintf(&b, "%02d", t.Minute())
		case 'j':
			fmt.Fprintf(&b, "%03d", t.YearDay())
		case 'k':
			b.WriteString(strconv.Itoa(t.Hour()))
		case 'l':
			b.WriteString(strconv.Itoa(hour12(t.Hour())))
		case 'M':
			b.WriteString(t.Month().String())
		case 'm':
			fmt.Fprintf(&b, "%02d", int(t.Month()))
		case 'p':
			b.WriteString(ampm(t.Hour()))
		case 'r':
			fmt.Fprintf(&b, "%02d:%02d:%02d %s", hour12(t.Hour()), t.Minute(), t.Second(), ampm(t.Hour()))
		case 'S', 's':
			fmt.Fprintf(&b, "%02d", t.Second())
		case 'T':
			fmt.Fprintf(&b, "%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
		case 'U':
			_, w := calcWeek(t, 4)
			fmt.Fprintf(&b, "%02d", w)
		case 'u':
			_, w := calcWeek(t, 1)
			fmt.Fprintf(&b, "%02d", w)
		case 'V':
			_, w := calcWeek(t, 6)
			fmt.Fprintf(&b, "%02d", w)
		case 'v':
			_, w := calcWeek(t, 3)
			fmt.Fprintf(&b, "%02d", w)
		case 'W':
			b.WriteString(t.Weekday().String())
		case 'w':
			b.WriteString(strconv.Itoa(int(t.Weekday())))
		case 'X':
			y, _ := calcWeek(t, 6)
			fmt.Fprintf(&b, "%04d", y)
		case 'x':
			y, _ := calcWeek(t, 3)
			fmt.Fprintf(&b, "%04d", y)
		case 'Y':
			fmt.Fprintf(&b, "%04d", t.Year())
		case 'y':
			fmt.Fprintf(&b, "%02d", t.Year()%100)
		case '%':
			b.WriteByte('%')
		default:
			b.WriteByte(format[i])
		}
	}
	return b.String(), true
}

// posixFormat formats t with POSIX strftime specifiers.
func posixFormat(t time.Time, format string) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		ch := format[i]
		if ch != '%' || i+1 >= len(format) {
			b.WriteByte(ch)
			continue
		}
		i++
		switch format[i] {
		case 'a':
			b.WriteString(t.Weekday().String()[:3])
		case 'A':
			b.WriteString(t.Weekday().String())
		case 'b', 'h':
			b.WriteString(t.Month().String()[:3])
		case 'B':
			b.WriteString(t.Month().String())
		case 'c':
			b.WriteString(t.Format("Mon Jan  2 15:04:05 2006"))
		case 'C':
			fmt.Fprintf(&b, "%02d", t.Year()/100)
		case 'd':
			fmt.Fprintf(&b, "%02d", t.Day())
		case 'D':
			b.WriteString(t.Format("01/02/06"))
		case 'e':
			fmt.Fprintf(&b, "%2d", t.Day())
		case 'F':
			b.WriteString(t.Format("2006-01-02"))
		case 'H':
			fmt.Fprintf(&b, "%02d", t.Hour())
		case 'I':
			fmt.Fprintf(&b, "%02d", hour12(t.Hour()))
		case 'j':
			fmt.Fprintf(&b, "%03d", t.YearDay())
		case 'k':
			fmt.Fprintf(&b, "%2d", t.Hour())
		case 'l':
			fmt.Fprintf(&b, "%2d", hour12(t.Hour()))
		case 'm':
			fmt.Fprintf(&b, "%02d", int(t.Month()))
		case 'M':
			fmt.Fprintf(&b, "%02d", t.Minute())
		case 'N', 'Q':
			fmt.Fprintf(&b, "%03d", t.Nanosecond()/1e6)
		case 'p':
			b.WriteString(ampm(t.Hour()))
		case 's':
			b.WriteString(strconv.FormatInt(t.Unix(), 10))
		case 'S':
			fmt.Fprintf(&b, "%02d", t.Second())
		case 'T':
			b.WriteString(t.Format("15:04:05"))
		case 'u':
			wd := int(t.Weekday())
			if wd == 0 {
				wd = 7
			}
			b.WriteString(strconv.Itoa(wd))
		case 'w':
			b.WriteString(strconv.Itoa(int(t.Weekday())))
		case 'y':
			fmt.Fprintf(&b, "%02d", t.Year()%100)
		case 'Y':
			fmt.Fprintf(&b, "%04d", t.Year())
		case 'z':
			b.WriteString("+0000")
		case 'Z':
			b.WriteString("UTC")
		case '%':
			b.WriteByte('%')
		default:
			b.WriteByte('%')
			b.WriteByte(format[i])
		}
	}
	return b.String()
}

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
	months := []string{"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"}
	short := make([]string, 12)
	for i, m := range months {
		short[i] = m[:3]
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
			idx, ok = word([]string{"AM", "PM"})
			pm, hasAMPM = idx == 1, true
		case 'M':
			var idx int
			idx, ok = word(months)
			month = idx + 1
		case 'b':
			var idx int
			idx, ok = word(short)
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
