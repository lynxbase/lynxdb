package ir

func p(name string, t Type) Param     { return Param{Name: name, Type: t} }
func pc(name string, t Type) Param    { return Param{Name: name, Type: t, Const: true} }
func pl(name string, arity int) Param { return Param{Name: name, Lambda: arity} }

func scalar(cat, name string, min int, params []Param, result ResultRule, doc string) *Func {
	return RegisterFunc(&Func{Name: name, Kind: ScalarFunc, Category: cat, Params: params, MinArgs: min, Result: result, Doc: doc})
}

func aware(f *Func) *Func    { f.Nulls = NullAware; return f }
func variadic(f *Func) *Func { f.Variadic = true; return f }
func volatile(f *Func) *Func { f.Volatile = true; return f }
func queryConst(f *Func) *Func {
	f.QueryConstant = true
	return f
}
func errs(f *Func, e string) *Func { f.Errors = e; return f }

func aggregate(name string, min int, params []Param, result ResultRule, merge MergeKind, doc string) *Func {
	return RegisterFunc(&Func{Name: name, Kind: AggregateFunc, Category: "aggregation", Params: params, MinArgs: min,
		Result: result, Nulls: NullSkipped, Merge: merge, Doc: doc})
}

func ordered(f *Func) *Func { f.OrderSensitive = true; return f }

func windowFn(name string, params []Param, result ResultRule, doc string) *Func {
	return RegisterFunc(&Func{Name: name, Kind: WindowFunc, Category: "window", Params: params, MinArgs: len(params),
		Result: result, Nulls: NullSkipped, OrderSensitive: true, Doc: doc})
}

func numOrAny(args []Type) (Type, error) {
	if len(args) > 0 && (args[0].Kind == KindInt || args[0].Kind == KindFloat) {
		return args[0], nil
	}
	return Any, nil
}

func fromUnixtimeResult(args []Type) (Type, error) {
	if len(args) == 2 {
		return String, nil
	}
	return Timestamp, nil
}

func arrayUnify(args []Type) (Type, error) {
	t := Null
	for _, a := range args {
		t = Unify(t, a)
	}
	if t.Kind == KindNull {
		t = Any
	}
	return ArrayOf(t), nil
}

func arrayConcat(args []Type) (Type, error) {
	t := Null
	for _, a := range args {
		if a.Kind == KindArray {
			a = a.ElemType()
		}
		t = Unify(t, a)
	}
	if t.Kind == KindNull {
		t = Any
	}
	return ArrayOf(t), nil
}

const (
	errNullOnInvalid = "Returns NULL for arguments it cannot interpret."
	errPlanTime      = "Invalid constant arguments are rejected at planning time."
)

var (
	numericParam = Float
	strArray     = ArrayOf(String)
)

func init() {
	registerMath()
	registerString()
	registerCondition()
	registerConversion()
	registerCrypto()
	registerDatetime()
	registerIP()
	registerJSON()
	registerCollection()
	registerSystem()
	registerAggregates()
	registerWindow()
	registerInternal()
}

func registerMath() {
	const c = "math"
	x := []Param{p("x", numericParam)}
	scalar(c, "abs", 1, x, numOrAny, "Absolute value.")
	scalar(c, "ceil", 1, x, Returns(Int), "Smallest integer not less than x.")
	scalar(c, "floor", 1, x, Returns(Int), "Largest integer not greater than x.")
	scalar(c, "round", 1, []Param{p("x", numericParam), p("d", Int)}, numOrAny, "x rounded half away from zero to d decimal places (default 0).")
	scalar(c, "rint", 1, x, Returns(Float), "x rounded to the nearest integer, ties to even.")
	scalar(c, "sqrt", 1, x, Returns(Float), "Square root; NULL for negative x.")
	scalar(c, "cbrt", 1, x, Returns(Float), "Cube root.")
	scalar(c, "pow", 2, []Param{p("x", numericParam), p("y", numericParam)}, Returns(Float), "x raised to the power y.")
	scalar(c, "exp", 1, x, Returns(Float), "e raised to the power x.")
	scalar(c, "expm1", 1, x, Returns(Float), "e raised to the power x, minus 1.")
	scalar(c, "ln", 1, x, Returns(Float), "Natural logarithm; NULL for x <= 0.")
	scalar(c, "log", 1, []Param{p("b_or_x", numericParam), p("x", numericParam)}, Returns(Float), "Natural logarithm of x, or with two arguments the base-b logarithm of x; NULL for non-positive input.")
	scalar(c, "log2", 1, x, Returns(Float), "Base-2 logarithm; NULL for x <= 0.")
	scalar(c, "log10", 1, x, Returns(Float), "Base-10 logarithm; NULL for x <= 0.")
	scalar(c, "mod", 2, []Param{p("n", numericParam), p("m", numericParam)}, NumericResult, "Remainder of n divided by m with the sign of n; NULL when m is 0.")
	scalar(c, "sign", 1, x, Returns(Int), "-1, 0 or 1 according to the sign of x.")
	for _, fn := range []string{"sin", "cos", "tan", "asin", "acos", "cot", "sinh", "cosh", "degrees", "radians"} {
		scalar(c, fn, 1, x, Returns(Float), "Trigonometric or angle-conversion function "+fn+" in radians; NULL outside its domain.")
	}
	scalar(c, "atan", 1, []Param{p("y_or_x", numericParam), p("x", numericParam)}, Returns(Float), "Arc tangent of x, or with two arguments of y/x using the signs of both to pick the quadrant.")
	scalar(c, "atan2", 2, []Param{p("y", numericParam), p("x", numericParam)}, Returns(Float), "Arc tangent of y/x using the signs of both to pick the quadrant.")
	scalar(c, "pi", 0, nil, Returns(Float), "The constant pi.")
	scalar(c, "e", 0, nil, Returns(Float), "The constant e.")
	volatile(scalar(c, "rand", 0, []Param{p("seed", Int)}, Returns(Float), "Uniform random float in [0, 1); with a seed the sequence is reproducible within one query."))
	errs(scalar(c, "conv", 3, []Param{p("x", Any), p("from_base", Int), p("to_base", Int)}, Returns(String), "x read as a number in from_base, written in to_base (2-36)."), errNullOnInvalid)
	scalar(c, "crc32", 1, []Param{p("s", String)}, Returns(Int), "CRC-32 (IEEE) checksum of the UTF-8 bytes of s.")
	variadic(scalar(c, "scalar_sum", 1, []Param{p("x", numericParam)}, NumericResult, "Sum of the arguments."))
	variadic(scalar(c, "scalar_avg", 1, []Param{p("x", numericParam)}, Returns(Float), "Arithmetic mean of the arguments."))
	variadic(scalar(c, "greatest", 1, []Param{p("x", Any)}, UnifyArgs(0), "Largest argument; NULL arguments are ignored."))
	variadic(scalar(c, "least", 1, []Param{p("x", Any)}, UnifyArgs(0), "Smallest argument; NULL arguments are ignored."))
	aware(MustFunc("greatest"))
	aware(MustFunc("least"))
}

func registerString() {
	const c = "string"
	s := []Param{p("s", String)}
	aware(variadic(scalar(c, "concat", 1, []Param{p("s", Any)}, Returns(String), "Concatenation of the arguments' string forms; NULL when any argument is NULL.")))
	MustFunc("concat").Nulls = NullStrict
	aware(variadic(scalar(c, "concat_ws", 2, []Param{p("sep", String), p("s", Any)}, Returns(String), "Concatenation of the non-NULL arguments separated by sep.")))
	scalar(c, "length", 1, s, Returns(Int), "Number of characters (Unicode code points) in s.")
	scalar(c, "lower", 1, s, Returns(String), "s in lower case.")
	scalar(c, "upper", 1, s, Returns(String), "s in upper case.")
	scalar(c, "trim", 1, s, Returns(String), "s without leading and trailing white space.")
	scalar(c, "ltrim", 1, s, Returns(String), "s without leading white space.")
	scalar(c, "rtrim", 1, s, Returns(String), "s without trailing white space.")
	scalar(c, "reverse", 1, s, Returns(String), "s with its characters in reverse order.")
	scalar(c, "left", 2, []Param{p("s", String), p("n", Int)}, Returns(String), "The first n characters of s.")
	scalar(c, "right", 2, []Param{p("s", String), p("n", Int)}, Returns(String), "The last n characters of s.")
	scalar(c, "substring", 2, []Param{p("s", String), p("start", Int), p("length", Int)}, Returns(String), "Substring of s from 1-based position start (negative counts from the end), of at most length characters.")
	scalar(c, "locate", 2, []Param{p("sub", String), p("s", String), p("start", Int)}, Returns(Int), "1-based position of the first occurrence of sub in s at or after start, or 0.")
	scalar(c, "ascii", 1, s, Returns(Int), "Code point of the first character of s, 0 for the empty string.")
	scalar(c, "strcmp", 2, []Param{p("a", String), p("b", String)}, Returns(Int), "-1, 0 or 1 comparing a and b by code points.")
	errs(scalar(c, "like", 2, []Param{p("s", String), pc("pattern", String), pc("case_sensitive", Bool)}, Returns(Bool), "TRUE when s matches the SQL LIKE pattern (% any run, _ one character, \\ escapes); case-insensitive unless case_sensitive is TRUE."), errPlanTime)
	errs(scalar(c, "regexp_match", 2, []Param{p("s", String), pc("pattern", String)}, Returns(Bool), "TRUE when the RE2 pattern matches anywhere in s."), errPlanTime)
	errs(scalar(c, "regexp_replace", 3, []Param{p("s", String), pc("pattern", String), p("replacement", String)}, Returns(String), "s with every match of the RE2 pattern replaced; $1 or \\1 in the replacement inserts a group."), errPlanTime)
	errs(scalar(c, "regexp_extract", 2, []Param{p("s", String), pc("pattern", String), pc("group", Int)}, Returns(String), "The text of capture group (default 0, the whole match) of the first match of pattern in s, or NULL."), errPlanTime)
	scalar(c, "contains", 2, []Param{p("s", String), p("sub", String)}, Returns(Bool), "TRUE when s contains sub (case-sensitive substring).")
}

func registerCondition() {
	const c = "condition"
	aware(scalar(c, "is_null", 1, []Param{p("x", Any)}, Returns(Bool), "TRUE when x is NULL or absent; never NULL."))
	aware(scalar(c, "is_empty", 1, []Param{p("x", Any)}, Returns(Bool), "TRUE when x is NULL, absent or the empty string."))
	aware(scalar(c, "is_blank", 1, []Param{p("x", Any)}, Returns(Bool), "TRUE when x is NULL, absent, or a string of only white space."))
	aware(variadic(scalar(c, "coalesce", 1, []Param{p("x", Any)}, UnifyArgs(0), "The first argument that is not NULL, or NULL.")))
}

func registerConversion() {
	const c = "conversion"
	aware(scalar(c, "tostring", 1, []Param{p("x", Any), pc("format", String)}, Returns(String), "String form of x; formats: hex, binary, commas, duration."))
	errs(scalar(c, "tonumber", 1, []Param{p("s", Any), pc("base", Int)}, Returns(Any), "s parsed as a number in base (default 10); an int when it has no fraction, else a float."), errNullOnInvalid)
	errs(scalar(c, "parse_json", 1, []Param{p("s", Any)}, Returns(Any), "The value JSON text s denotes, with objects and arrays as nested values."), errNullOnInvalid)
	for _, fn := range []struct{ name, doc string }{
		{"convert_auto", "x as a number when its text parses as one (leading sign, digits, fraction, exponent), else NULL."},
		{"convert_num", "x as a number after removing thousands separators and a trailing unit, else NULL."},
		{"rmcomma", "x as a number after removing every comma, else NULL."},
		{"rmunit", "The leading number of x with any trailing unit text removed, else NULL."},
		{"memk", "x as kilobytes: a number with an optional k, m or g suffix scaled to KB."},
		{"dur2sec", "x in [D+]HH:MM:SS form as seconds."},
		{"mstime", "x in [MM:]SS.SSS form as seconds."},
	} {
		errs(scalar(c, fn.name, 1, []Param{p("x", Any)}, Returns(Any), fn.doc), errNullOnInvalid)
	}
	errs(scalar(c, "ctime", 2, []Param{p("seconds", Any), p("format", String)}, Returns(String), "Epoch seconds formatted with strftime % specifiers."), errNullOnInvalid)
	errs(scalar(c, "mktime", 2, []Param{p("s", Any), p("format", String)}, Returns(Float), "s parsed with strftime % specifiers, as epoch seconds."), errNullOnInvalid)
	scalar("string", "wildcard_replace", 3, []Param{p("s", Any), pc("pattern", String), pc("replacement", String)}, Returns(Any),
		"Without '*' in pattern, every occurrence of pattern in s replaced; with '*', s replaced when it matches the whole pattern, each '*' of the replacement taking the text the corresponding '*' matched. \\* and \\\\ escape.")
}

func registerCrypto() {
	const c = "cryptographic"
	scalar(c, "md5", 1, []Param{p("s", String)}, Returns(String), "Hex MD5 digest of s.")
	scalar(c, "sha1", 1, []Param{p("s", String)}, Returns(String), "Hex SHA-1 digest of s.")
	errs(scalar(c, "sha2", 2, []Param{p("s", String), pc("bits", Int)}, Returns(String), "Hex SHA-2 digest of s with 224, 256, 384 or 512 bits."), errPlanTime)
}

func registerDatetime() {
	const c = "datetime"
	t := []Param{p("t", Any)}
	queryConst(scalar(c, "now", 0, nil, Returns(Timestamp), "The query's reference time."))
	volatile(scalar(c, "sysdate", 0, []Param{p("precision", Int)}, Returns(Timestamp), "The wall-clock time at evaluation."))
	queryConst(scalar(c, "curdate", 0, nil, Returns(Timestamp), "The start (00:00 UTC) of the reference time's day."))
	queryConst(scalar(c, "curtime", 0, nil, Returns(Duration), "The reference time's time of day as a duration since midnight UTC."))
	scalar(c, "date", 1, t, Returns(Timestamp), "The start (00:00 UTC) of t's day; t may be a timestamp or a date or timestamp string.")
	scalar(c, "time", 1, t, Returns(Duration), "t's time of day as a duration since midnight; t may be a timestamp, a duration or a time string.")
	scalar(c, "timestamp", 1, []Param{p("t", Any), p("add", Any)}, Returns(Timestamp), "t as a timestamp, plus the time of day add when given.")
	scalar(c, "datetime", 1, []Param{p("t", Any), p("to_tz", String)}, Returns(Timestamp), "t as a timestamp, converted to time zone to_tz when given.")
	scalar(c, "date_add", 3, []Param{p("t", Any), p("n", Int), pc("unit", String)}, Returns(Timestamp), "t plus n calendar units (microsecond, second, minute, hour, day, week, month, quarter, year).")
	scalar(c, "addtime", 2, []Param{p("t", Any), p("delta", Any)}, SameAs(0), "t plus the time of day or duration delta.")
	scalar(c, "subtime", 2, []Param{p("t", Any), p("delta", Any)}, SameAs(0), "t minus the time of day or duration delta.")
	scalar(c, "convert_tz", 3, []Param{p("t", Any), p("from_tz", String), p("to_tz", String)}, Returns(Timestamp), "t read as wall-clock time in from_tz, rewritten as wall-clock time in to_tz; NULL for unknown zones.")
	errs(scalar(c, "date_format", 2, []Param{p("t", Any), p("format", String)}, Returns(String), "t formatted with MySQL-style % specifiers."), errNullOnInvalid)
	errs(scalar(c, "time_format", 2, []Param{p("t", Any), p("format", String)}, Returns(String), "The time-of-day part of t formatted with MySQL-style % specifiers."), errNullOnInvalid)
	errs(scalar(c, "strftime", 2, []Param{p("t", Any), p("format", String)}, Returns(String), "t (a timestamp or epoch seconds) formatted with POSIX strftime % specifiers."), errNullOnInvalid)
	errs(scalar(c, "str_to_date", 2, []Param{p("s", String), p("format", String)}, Returns(Timestamp), "s parsed with MySQL-style % specifiers."), errNullOnInvalid)
	scalar(c, "get_format", 2, []Param{pc("type", String), pc("locale", String)}, Returns(String), "The MySQL-style format string for type (DATE, TIME, TIMESTAMP) and locale (USA, JIS, ISO, EUR, INTERNAL).")
	scalar(c, "datediff", 2, []Param{p("a", Any), p("b", Any)}, Returns(Int), "Whole days from b's date to a's date.")
	scalar(c, "timediff", 2, []Param{p("a", Any), p("b", Any)}, Returns(Duration), "a minus b as a duration.")
	scalar(c, "timestampadd", 3, []Param{pc("unit", String), p("n", Int), p("t", Any)}, Returns(Timestamp), "t plus n units.")
	scalar(c, "timestampdiff", 3, []Param{pc("unit", String), p("start", Any), p("end", Any)}, Returns(Int), "Whole units from start to end.")
	for _, fn := range []struct{ name, doc string }{
		{"year", "Year of t."}, {"quarter", "Quarter (1-4) of t."}, {"month", "Month (1-12) of t."},
		{"day", "Day of month (1-31) of t."}, {"dayofweek", "Day of week of t, 1 = Sunday to 7 = Saturday."},
		{"weekday", "Day of week of t, 0 = Monday to 6 = Sunday."}, {"dayofyear", "Day of year (1-366) of t."},
		{"hour", "Hour (0-23) of t."}, {"minute", "Minute (0-59) of t."}, {"minute_of_day", "Minutes since midnight of t."},
		{"second", "Second (0-59) of t."}, {"microsecond", "Microseconds (0-999999) of t."},
		{"to_days", "Days since year 0 of t's date."}, {"to_seconds", "Seconds since year 0 of t."},
		{"time_to_sec", "Seconds of t's time of day."},
	} {
		scalar(c, fn.name, 1, t, Returns(Int), fn.doc)
	}
	scalar(c, "dayname", 1, t, Returns(String), "English name of t's weekday.")
	scalar(c, "monthname", 1, t, Returns(String), "English name of t's month.")
	scalar(c, "last_day", 1, t, Returns(Timestamp), "The start of the last day of t's month.")
	scalar(c, "week", 1, []Param{p("t", Any), pc("mode", Int)}, Returns(Int), "Week number of t under MySQL week mode 0-7 (default 0).")
	scalar(c, "yearweek", 1, []Param{p("t", Any), pc("mode", Int)}, Returns(Int), "Year and week of t as YYYYWW under MySQL week mode 0-7 (default 0).")
	errs(scalar(c, "extract", 2, []Param{pc("part", String), p("t", Any)}, Returns(Int), "The named part of t (e.g. YEAR, MONTH, DAY_HOUR, MINUTE_SECOND)."), errPlanTime)
	scalar(c, "from_days", 1, []Param{p("n", Int)}, Returns(Timestamp), "The date n days after year 0.")
	scalar(c, "from_unixtime", 1, []Param{p("seconds", numericParam), p("format", String)}, fromUnixtimeResult, "The timestamp seconds after the Unix epoch; formatted with MySQL-style specifiers when format is given.")
	scalar(c, "makedate", 2, []Param{p("year", numericParam), p("dayofyear", numericParam)}, Returns(Timestamp), "The date for a year and day of year.")
	scalar(c, "maketime", 3, []Param{p("hour", numericParam), p("minute", numericParam), p("second", numericParam)}, Returns(Duration), "A time of day from its parts.")
	scalar(c, "period_add", 2, []Param{p("period", Int), p("n", Int)}, Returns(Int), "Period YYMM or YYYYMM plus n months, as YYYYMM.")
	scalar(c, "period_diff", 2, []Param{p("a", Int), p("b", Int)}, Returns(Int), "Months from period b to period a.")
	scalar(c, "sec_to_time", 1, []Param{p("seconds", numericParam)}, Returns(Duration), "seconds as a duration.")
	queryConst(scalar(c, "unix_timestamp", 0, []Param{p("t", Any)}, Returns(Float), "Seconds since the Unix epoch of t, or of the reference time."))
}

func registerIP() {
	scalar("ip", "cidrmatch", 2, []Param{p("ip", String), p("cidr", String)}, Returns(Bool), "TRUE when the IPv4 or IPv6 address ip is inside the CIDR block cidr; NULL when either does not parse.")
}

func registerJSON() {
	const c = "json"
	aware(scalar(c, "json", 1, []Param{p("s", Any)}, Returns(String), "s when it is valid JSON text, else NULL."))
	aware(scalar(c, "json_valid", 1, []Param{p("s", Any)}, Returns(Bool), "TRUE when s is valid JSON text; FALSE for NULL."))
	variadic(scalar(c, "json_object", 0, []Param{p("key_value", Any)}, Returns(String), "JSON object text from alternating keys and values."))
	MustFunc("json_object").Nulls = NullAware
	aware(variadic(scalar(c, "json_array", 0, []Param{p("element", Any)}, Returns(String), "JSON array text of the arguments.")))
	scalar(c, "json_array_length", 1, []Param{p("s", Any)}, Returns(Int), "Number of elements of the JSON array s; NULL when s is not a JSON array.")
	variadic(scalar(c, "json_extract", 2, []Param{p("s", Any), p("path", String)}, Returns(Any), "The value at a JSON path in s; with several paths a JSON array of the values."))
	variadic(scalar(c, "json_delete", 2, []Param{p("s", Any), p("path", String)}, Returns(String), "s without the members at the paths."))
	variadic(scalar(c, "json_set", 3, []Param{p("s", Any), p("path", String), p("value", Any)}, Returns(String), "s with the value at each path replaced or created."))
	variadic(scalar(c, "json_append", 3, []Param{p("s", Any), p("path", String), p("value", Any)}, Returns(String), "s with each value appended to the array at its path."))
	variadic(scalar(c, "json_extend", 3, []Param{p("s", Any), p("path", String), p("value", Any)}, Returns(String), "s with each array value's elements appended to the array at its path."))
	scalar(c, "json_keys", 1, []Param{p("s", Any)}, Returns(String), "JSON array text of the top-level keys of the JSON object s; NULL for non-objects.")
}

func registerCollection() {
	const c = "collection"
	aware(variadic(scalar(c, "array", 0, []Param{p("element", Any)}, arrayUnify, "Array of the arguments.")))
	scalar(c, "array_length", 1, []Param{p("a", Any)}, Returns(Int), "Number of elements of a; NULL for non-arrays.")
	aware(scalar(c, "forall", 2, []Param{p("a", Any), pl("predicate", 1)}, Returns(Bool), "TRUE when the predicate is TRUE for every element (TRUE for an empty array), NULL when none is FALSE but some is NULL."))
	aware(scalar(c, "exists", 2, []Param{p("a", Any), pl("predicate", 1)}, Returns(Bool), "TRUE when the predicate is TRUE for some element, NULL when none is TRUE but some is NULL."))
	scalar(c, "filter", 2, []Param{p("a", Any), pl("predicate", 1)}, SameAs(0), "Elements for which the predicate is TRUE.")
	scalar(c, "transform", 2, []Param{p("a", Any), pl("mapper", 2)}, Returns(ArrayOf(Any)), "Array of the lambda applied to each element (and its 0-based index for a two-parameter lambda).")
	scalar(c, "reduce", 3, []Param{p("a", Any), p("init", Any), pl("combine", 2), pl("finish", 1)}, Returns(Any), "Left fold of the elements starting at init, finished by the optional one-parameter lambda.")
	scalar(c, "mvjoin", 2, []Param{p("a", Any), p("sep", String)}, Returns(String), "String forms of the non-NULL elements joined by sep.")
	aware(variadic(scalar(c, "mvappend", 1, []Param{p("x", Any)}, arrayConcat, "Array of the arguments with array arguments flattened one level; NULL arguments are skipped.")))
	scalar(c, "split", 2, []Param{p("s", String), p("sep", String)}, Returns(strArray), "s split at every occurrence of sep; the empty separator splits into characters.")
	scalar(c, "mvdedup", 1, []Param{p("a", Any)}, SameAs(0), "a with later duplicates removed.")
	errs(scalar(c, "mvfind", 2, []Param{p("a", Any), pc("regex", String)}, Returns(Int), "0-based index of the first element matching regex, or NULL."), errPlanTime)
	scalar(c, "mvindex", 2, []Param{p("a", Any), p("start", Int), p("end", Int)}, Returns(Any), "The element at 0-based start, or with end the inclusive sub-array; negative indexes count from the end.")
	scalar(c, "mvzip", 2, []Param{p("a", Any), p("b", Any), p("sep", String)}, Returns(strArray), "Pairwise string concatenation of a and b joined by sep (default comma), as long as the shorter.")
}

func registerSystem() {
	aware(scalar("system", "typeof", 1, []Param{p("x", Any)}, Returns(String), "Name of the runtime type of x: null, bool, int, float, string, timestamp, duration, array, object."))
}

func registerAggregates() {
	x := []Param{p("x", Any)}
	aggregate("count", 0, x, Returns(Int), MergeExact, "Number of rows, or with an argument the number of rows where it is not NULL.")
	aggregate("sum", 1, x, numOrAny, MergeExact, "Sum of the numeric values.")
	aggregate("avg", 1, x, Returns(Float), MergeExact, "Arithmetic mean of the numeric values.")
	aggregate("min", 1, x, SameAs(0), MergeExact, "Smallest value; numbers order before strings.")
	aggregate("max", 1, x, SameAs(0), MergeExact, "Largest value; numbers order before strings.")
	aggregate("var_samp", 1, x, Returns(Float), MergeExact, "Sample variance of the numeric values; NULL for fewer than two.")
	aggregate("var_pop", 1, x, Returns(Float), MergeExact, "Population variance of the numeric values.")
	aggregate("stddev_samp", 1, x, Returns(Float), MergeExact, "Sample standard deviation of the numeric values; NULL for fewer than two.")
	aggregate("stddev_pop", 1, x, Returns(Float), MergeExact, "Population standard deviation of the numeric values.")
	aggregate("distinct_count", 1, x, Returns(Int), MergeExact, "Number of distinct non-NULL values.")
	aggregate("distinct_count_approx", 1, x, Returns(Int), MergeApprox, "HyperLogLog estimate of the number of distinct non-NULL values.")
	aggregate("percentile", 2, []Param{p("x", Any), pc("percent", Float)}, Returns(Float), MergeApprox, "t-digest estimate of the percent-th percentile (0-100) of the numeric values.")
	aggregate("earliest", 1, []Param{p("x", Any), p("time", Timestamp)}, SameAs(0), MergeExact, "The value of the row with the smallest time (the time column by default).")
	aggregate("latest", 1, []Param{p("x", Any), p("time", Timestamp)}, SameAs(0), MergeExact, "The value of the row with the largest time (the time column by default).")
	ordered(aggregate("first", 1, x, SameAs(0), MergeNone, "The first non-NULL value in input order."))
	ordered(aggregate("last", 1, x, SameAs(0), MergeNone, "The last non-NULL value in input order."))
	ordered(aggregate("take", 1, []Param{p("x", Any), pc("size", Int)}, ArrayOfArg(0), MergeNone, "Array of the first size (default 10) non-NULL values in input order."))
	ordered(aggregate("list", 1, x, ArrayOfArg(0), MergeNone, "Array of the non-NULL values in input order, at most 100."))
	aggregate("values", 1, x, ArrayOfArg(0), MergeExact, "Sorted array of the distinct non-NULL values, at most 100.")
}

func registerWindow() {
	windowFn("moving_avg", []Param{p("x", Any)}, Returns(Float), "Mean of the frame's numeric values once the frame holds its full row count, else NULL.")
	windowFn("weighted_moving_avg", []Param{p("x", Any)}, Returns(Float), "Linearly weighted mean of the frame's values, newest weighted highest, once the frame is full, else NULL.")
}

func registerInternal() {
	const c = "bucketing"
	errs(scalar(c, "time_bucket", 3, []Param{p("t", Timestamp), pc("n", Int), pc("unit", String)}, Returns(Timestamp), "Start of the n-unit bucket containing t; units ms, s, m, h, d, w align to the epoch, mon, q, y to calendar boundaries (UTC)."), errPlanTime)
	scalar(c, "num_bucket", 2, []Param{p("x", numericParam), pc("width", numericParam)}, numOrAny, "Lower bound of the width-sized bucket containing x: floor(x / width) * width.")
}
