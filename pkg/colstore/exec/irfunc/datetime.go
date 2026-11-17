package irfunc

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lynxbase/lynxdb/pkg/ir"
)

// timeArg interprets a value as an instant; a duration is a time of day on
// the Unix epoch day.
func timeArg(v val) (time.Time, bool) {
	if t, ok := timeOf(v); ok {
		return t, true
	}
	if v.k == kDur {
		return time.Unix(0, 0).UTC().Add(v.dur()), true
	}

	return time.Time{}, false
}

func timeFn(f func(c *ctx, t time.Time) val) impl {
	return strict(func(c *ctx, a []val) val {
		t, ok := timeArg(a[0])
		if !ok {
			return null
		}
		return f(c, t)
	})
}

func clockFn(f func(d time.Duration) val) impl {
	return strict(func(_ *ctx, a []val) val {
		d, ok := timeOfDay(a[0])
		if !ok {
			return null
		}
		return f(d)
	})
}

// zones keeps the time zones a function has loaded.
type zones map[string]*time.Location

func (z zones) load(name string) (*time.Location, bool) {
	if loc, ok := z[name]; ok {
		return loc, loc != nil
	}
	loc, ok := loadZone(name)
	if len(z) < 64 {
		z[strings.Clone(name)] = loc
	}

	return loc, ok
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
// last day.
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

func periodMonths(p int64) int64 {
	y, m := p/100, p%100
	if y < 70 {
		y += 2000
	} else if y < 100 {
		y += 1900
	}

	return y*12 + m - 1
}

func timeBucketer(count int64, unit string) (func(time.Time) time.Time, bool) {
	fixed := map[string]time.Duration{"ms": time.Millisecond, "s": time.Second, "m": time.Minute, "h": time.Hour, "d": 24 * time.Hour, "w": 7 * 24 * time.Hour}
	if d, ok := fixed[unit]; ok {
		w := int64(time.Duration(count) * d)
		return func(t time.Time) time.Time {
			ns := t.UnixNano()
			q := ns / w
			if ns%w != 0 && ns < 0 {
				q--
			}
			return time.Unix(0, q*w).UTC()
		}, true
	}
	m, ok := map[string]int64{"mon": 1, "q": 3, "y": 12}[unit]
	if !ok {
		return nil, false
	}
	step := m * count

	return func(t time.Time) time.Time {
		idx := int64(t.Year())*12 + int64(t.Month()-1)
		b := idx / step * step
		if idx%step != 0 && idx < 0 {
			b -= step
		}
		return time.Date(int(b/12), time.Month(b%12+1), 1, 0, 0, 0, 0, time.UTC)
	}, true
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

func init() {
	constant := func(name string, f func(now time.Time) val) {
		defEnv(name, func(_ *ir.Call, env Env) (func() impl, error) {
			v := f(env.Now.UTC())
			return func() impl { return func(*ctx, []val) val { return v } }, nil
		})
	}
	constant("now", func(now time.Time) val { return timeVal(now) })
	constant("curdate", func(now time.Time) val { return timeVal(dayStart(now)) })
	constant("curtime", func(now time.Time) val { return durVal(now.Sub(dayStart(now))) })
	defEnv("unix_timestamp", func(call *ir.Call, env Env) (func() impl, error) {
		if len(call.Args) == 0 {
			v := floatVal(float64(env.Now.UnixNano()) / 1e9)
			return func() impl { return func(*ctx, []val) val { return v } }, nil
		}
		return func() impl {
			return timeFn(func(_ *ctx, t time.Time) val { return floatVal(float64(t.UnixNano()) / 1e9) })
		}, nil
	})
	fixed("date", timeFn(func(_ *ctx, t time.Time) val { return timeVal(dayStart(t)) }))
	fixed("time", clockFn(func(d time.Duration) val { return durVal(d) }))
	fixed("timestamp", strict(func(_ *ctx, a []val) val {
		t, ok := timeArg(a[0])
		if !ok {
			return null
		}
		if len(a) > 1 {
			d, ok := timeOfDay(a[1])
			if !ok {
				return null
			}
			t = t.Add(d)
		}
		return timeVal(t)
	}))
	def("datetime", func(*ir.Call) (func() impl, error) {
		return func() impl {
			z := zones{}
			return strict(func(c *ctx, a []val) val {
				t, ok := timeArg(a[0])
				if !ok {
					return null
				}
				if len(a) > 1 {
					tz, _ := c.str(a[1])
					loc, ok := z.load(tz)
					if !ok {
						return null
					}
					t = wallIn(t, time.UTC, loc)
				}
				return timeVal(t)
			})
		}, nil
	})
	fixed("date_add", strict(func(c *ctx, a []val) val {
		t, ok := timeArg(a[0])
		n, ok2 := numInt(a[1])
		unit, ok3 := c.str(a[2])
		if !ok || !ok2 || !ok3 {
			return null
		}
		r, ok := addCalendar(t, n, unit)
		if !ok {
			return null
		}
		return timeVal(r)
	}))
	addTime := func(sign time.Duration) impl {
		return strict(func(_ *ctx, a []val) val {
			d, ok := timeOfDay(a[1])
			if !ok {
				return null
			}
			if a[0].k == kDur {
				return durVal(a[0].dur() + sign*d)
			}
			t, ok := timeArg(a[0])
			if !ok {
				return null
			}
			return timeVal(t.Add(sign * d))
		})
	}
	fixed("addtime", addTime(1))
	fixed("subtime", addTime(-1))
	def("convert_tz", func(*ir.Call) (func() impl, error) {
		return func() impl {
			z := zones{}
			return strict(func(c *ctx, a []val) val {
				t, ok := timeArg(a[0])
				f, _ := c.str(a[1])
				to, _ := c.str(a[2])
				from, ok1 := z.load(f)
				dst, ok2 := z.load(to)
				if !ok || !ok1 || !ok2 {
					return null
				}
				return timeVal(wallIn(t, from, dst))
			})
		}, nil
	})
	format := strict(func(c *ctx, a []val) val {
		t, ok := timeArg(a[0])
		format, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		start := len(c.out)
		c.out = mysqlFormat(c.out, t, format)
		return c.result(start)
	})
	fixed("date_format", format)
	fixed("time_format", format)
	fixed("strftime", strict(func(c *ctx, a []val) val {
		t, ok := timeArg(a[0])
		if !ok {
			if f, isNum := numFloat(a[0]); isNum && a[0].k != kStr {
				t, ok = secondsTime(f), true
			}
		}
		format, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		start := len(c.out)
		c.out = posixFormat(c.out, t, format)
		return c.result(start)
	}))
	fixed("str_to_date", strict(func(c *ctx, a []val) val {
		s, ok := c.str(a[0])
		format, ok2 := c.str(a[1])
		if !ok || !ok2 {
			return null
		}
		t, ok := mysqlParse(s, format)
		if !ok {
			return null
		}
		return timeVal(t)
	}))
	fixed("get_format", strict(func(c *ctx, a []val) val {
		kind, _ := c.str(a[0])
		locale, _ := c.str(a[1])
		f, ok := getFormats[strings.ToLower(kind)][strings.ToUpper(locale)]
		if !ok {
			return null
		}
		return strVal(f)
	}))
	fixed("datediff", strict(func(_ *ctx, a []val) val {
		x, ok := timeArg(a[0])
		y, ok2 := timeArg(a[1])
		if !ok || !ok2 {
			return null
		}
		return intVal(toDays(x) - toDays(y))
	}))
	fixed("timediff", strict(func(_ *ctx, a []val) val {
		if a[0].k == kTime || a[1].k == kTime {
			x, ok := timeArg(a[0])
			y, ok2 := timeArg(a[1])
			if !ok || !ok2 {
				return null
			}
			return durVal(x.Sub(y))
		}
		x, ok := timeOfDay(a[0])
		y, ok2 := timeOfDay(a[1])
		if !ok || !ok2 {
			return null
		}
		return durVal(x - y)
	}))
	fixed("timestampadd", strict(func(c *ctx, a []val) val {
		unit, _ := c.str(a[0])
		n, ok := numInt(a[1])
		t, ok2 := timeArg(a[2])
		if !ok || !ok2 {
			return null
		}
		r, ok := addCalendar(t, n, strings.ToLower(unit))
		if !ok {
			return null
		}
		return timeVal(r)
	}))
	fixed("timestampdiff", strict(func(c *ctx, a []val) val {
		unit, _ := c.str(a[0])
		x, ok := timeArg(a[1])
		y, ok2 := timeArg(a[2])
		if !ok || !ok2 {
			return null
		}
		d, ok := diffUnits(x, y, strings.ToLower(unit))
		if !ok {
			return null
		}
		return intVal(d)
	}))
	part := func(name string, f func(t time.Time) int64) {
		fixed(name, timeFn(func(_ *ctx, t time.Time) val { return intVal(f(t)) }))
	}
	part("year", func(t time.Time) int64 { return int64(t.Year()) })
	part("quarter", func(t time.Time) int64 { return int64((t.Month()-1)/3 + 1) })
	part("month", func(t time.Time) int64 { return int64(t.Month()) })
	part("day", func(t time.Time) int64 { return int64(t.Day()) })
	part("dayofweek", func(t time.Time) int64 { return int64(t.Weekday()) + 1 })
	part("weekday", func(t time.Time) int64 { return int64((t.Weekday() + 6) % 7) })
	part("dayofyear", func(t time.Time) int64 { return int64(t.YearDay()) })
	part("to_days", toDays)
	part("to_seconds", func(t time.Time) int64 {
		return (toDays(t)-1)*86400 + int64(t.Sub(dayStart(t))/time.Second)
	})
	clock := func(name string, f func(d time.Duration) int64) {
		fixed(name, clockFn(func(d time.Duration) val { return intVal(f(d)) }))
	}
	clock("hour", func(d time.Duration) int64 { return int64(d / time.Hour) })
	clock("minute", func(d time.Duration) int64 { return int64(d % time.Hour / time.Minute) })
	clock("minute_of_day", func(d time.Duration) int64 { return int64(d / time.Minute) })
	clock("second", func(d time.Duration) int64 { return int64(d % time.Minute / time.Second) })
	clock("microsecond", func(d time.Duration) int64 { return int64(d % time.Second / time.Microsecond) })
	clock("time_to_sec", func(d time.Duration) int64 { return int64(d / time.Second) })
	fixed("dayname", timeFn(func(_ *ctx, t time.Time) val { return strVal(t.Weekday().String()) }))
	fixed("monthname", timeFn(func(_ *ctx, t time.Time) val { return strVal(t.Month().String()) }))
	fixed("last_day", timeFn(func(_ *ctx, t time.Time) val {
		return timeVal(time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC))
	}))
	week := func(yearweek bool) impl {
		return strict(func(_ *ctx, a []val) val {
			t, ok := timeArg(a[0])
			if !ok {
				return null
			}
			mode := int64(0)
			if len(a) > 1 {
				if mode, ok = numInt(a[1]); !ok || mode < 0 || mode > 7 {
					return null
				}
			}
			if yearweek {
				y, w := calcWeek(t, weekMode(int(mode))|2)
				return intVal(int64(y*100 + w))
			}
			_, w := calcWeek(t, weekMode(int(mode)))
			return intVal(int64(w))
		})
	}
	fixed("week", week(false))
	fixed("yearweek", week(true))
	def("extract", func(call *ir.Call) (func() impl, error) {
		p, ok := constString(call, 0)
		if !ok {
			return nil, errorf("extract needs a constant part")
		}
		f, ok := extractParts[strings.ToLower(p)]
		if !ok {
			return nil, errorf("unknown extract part %s", p)
		}
		g := timeFn(func(_ *ctx, t time.Time) val { return intVal(f(t)) })
		return func() impl { return func(c *ctx, a []val) val { return g(c, a[1:2]) } }, nil
	})
	fixed("from_days", strict(func(_ *ctx, a []val) val {
		n, ok := numInt(a[0])
		if !ok {
			return null
		}
		return timeVal(fromDays(n))
	}))
	fixed("from_unixtime", strict(func(c *ctx, a []val) val {
		f, ok := numFloat(a[0])
		if !ok {
			return null
		}
		t := secondsTime(f)
		if len(a) > 1 {
			format, _ := c.str(a[1])
			start := len(c.out)
			c.out = mysqlFormat(c.out, t, format)
			return c.result(start)
		}
		return timeVal(t)
	}))
	fixed("makedate", strict(func(_ *ctx, a []val) val {
		y, ok := numFloat(a[0])
		doy, ok2 := numFloat(a[1])
		if !ok || !ok2 || doy < 1 {
			return null
		}
		return timeVal(time.Date(int(math.Round(y)), 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(math.Round(doy))-1))
	}))
	fixed("maketime", strict(func(_ *ctx, a []val) val {
		h, ok := numFloat(a[0])
		m, ok2 := numFloat(a[1])
		s, ok3 := numFloat(a[2])
		if !ok || !ok2 || !ok3 || m < 0 || m >= 60 || s < 0 || s >= 60 {
			return null
		}
		return durVal(time.Duration(math.Round(h))*time.Hour + time.Duration(math.Round(m))*time.Minute + time.Duration(s*float64(time.Second)))
	}))
	fixed("period_add", strict(func(_ *ctx, a []val) val {
		p, ok := numInt(a[0])
		n, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return null
		}
		months := periodMonths(p) + n
		return intVal(months/12*100 + months%12 + 1)
	}))
	fixed("period_diff", strict(func(_ *ctx, a []val) val {
		p, ok := numInt(a[0])
		q, ok2 := numInt(a[1])
		if !ok || !ok2 {
			return null
		}
		return intVal(periodMonths(p) - periodMonths(q))
	}))
	fixed("sec_to_time", strict(func(_ *ctx, a []val) val {
		f, ok := numFloat(a[0])
		if !ok {
			return null
		}
		return durVal(time.Duration(f * float64(time.Second)))
	}))
	def("time_bucket", func(call *ir.Call) (func() impl, error) {
		count, ok := constInt(call, 1)
		unit, ok2 := constString(call, 2)
		if !ok || !ok2 || count <= 0 {
			return nil, errorf("time_bucket needs a positive constant count and unit")
		}
		f, ok := timeBucketer(count, unit)
		if !ok {
			return nil, errorf("unknown time bucket unit %q", unit)
		}
		g := timeFn(func(_ *ctx, t time.Time) val { return timeVal(f(t)) })
		return func() impl { return first(g) }, nil
	})
}
