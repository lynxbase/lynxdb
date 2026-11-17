package vm_test

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/ir"
	"github.com/lynxbase/lynxdb/pkg/ppl"
	"github.com/lynxbase/lynxdb/pkg/vm"
)

func TestIRRegistryHasImplementations(t *testing.T) {
	for _, f := range ir.Funcs() {
		if f.Kind == ir.ScalarFunc && !vm.IRFuncImplemented(f.Name) {
			t.Errorf("scalar function %s has no bytecode implementation", f.Name)
		}
	}
}

var testNow = time.Date(2024, 3, 15, 10, 30, 45, 0, time.UTC)

// evalPPL compiles "eval x = <expr>" and evaluates x against row.
func evalPPL(t *testing.T, expr string, row map[string]event.Value) event.Value {
	t.Helper()
	res, err := ppl.Compile("source=t | eval x = " + expr)
	if err != nil {
		t.Fatalf("%s: %v", expr, err)
	}
	comp, ok := res.Plan.Root.(*ir.Compute)
	if !ok {
		t.Fatalf("%s: root is %T", expr, res.Plan.Root)
	}
	prog, err := vm.CompileExpr(comp.Assigns[0].Expr, vm.CompileOptions{Now: testNow})
	if err != nil {
		t.Fatalf("%s: compile: %v", expr, err)
	}
	var m vm.VM
	v, err := m.Execute(prog, row)
	if err != nil {
		t.Fatalf("%s: execute: %v", expr, err)
	}
	return v
}

func TestIRExpressionSemantics(t *testing.T) {
	row := map[string]event.Value{
		"n":    event.IntValue(7),
		"f":    event.FloatValue(2.5),
		"s":    event.StringValue("Hello World"),
		"num":  event.StringValue("42"),
		"bad":  event.StringValue("x1"),
		"nul":  event.NullValue(),
		"ts":   event.TimestampValue(time.Date(2020, 8, 26, 13, 49, 0, 0, time.UTC)),
		"arr":  event.ArrayValue([]event.Value{event.IntValue(1), event.IntValue(2), event.IntValue(3)}),
		"obj":  event.ObjectValue(map[string]event.Value{"a": event.IntValue(1)}),
		"json": event.StringValue(`{"a":{"b":[10,20]},"c":"x"}`),
		"ip":   event.StringValue("192.168.1.7"),
	}
	cases := []struct {
		expr string
		want string
	}{
		{"n + 1", "8"},
		{"n / 2", "3"},
		{"n / 0", "<null>"},
		{"n % 3", "1"},
		{"f * 2", "5"},
		{"num + 1", "43"},
		{"num > 40", "true"},
		{"bad > 1", "<null>"},
		{"'a' + 'b'", "ab"},
		{"nul + 1", "<null>"},
		{"missing_field + 1", "<null>"},
		{"n > 5 and nul > 1", "<null>"},
		{"n < 5 and nul > 1", "false"},
		{"n > 5 or nul > 1", "true"},
		{"not (nul > 1)", "<null>"},
		{"true xor false", "true"},
		{"n in (1, 7)", "true"},
		{"n not in (1, 2)", "true"},
		{"n between 5 and 10", "true"},
		{"nul is null", "true"},
		{"missing_field is null", "true"},
		{"n is not null", "true"},
		{"isnull(nul)", "true"},
		{"isnotnull(s)", "true"},
		{"if(n > 5, 'big', 'small')", "big"},
		{"ifnull(nul, 'd')", "d"},
		{"nullif(n, 7)", "<null>"},
		{"coalesce(nul, missing_field, 3)", "3"},
		{"case(n = 1, 'one', n = 7, 'seven' else 'other')", "seven"},
		{"case(nul > 1, 'a' else 'b')", "b"},
		{"abs(-3)", "3"},
		{"ceil(2.1)", "3"},
		{"floor(-2.1)", "-3"},
		{"round(2.345, 2)", "2.35"},
		{"round(-2.5)", "-3"},
		{"sqrt(16)", "4"},
		{"sqrt(-1)", "<null>"},
		{"pow(2, 10)", "1024"},
		{"mod(10, 3)", "1"},
		{"log(100, 10000)", "2"},
		{"sign(-5)", "-1"},
		{"conv('ff', 16, 10)", "255"},
		{"crc32('abc')", "891568578"},
		{"concat('a', n, 'b')", "a7b"},
		{"concat('a', nul)", "<null>"},
		{"concat_ws('-', 'a', nul, 'b')", "a-b"},
		{"length(s)", "11"},
		{"lower(s)", "hello world"},
		{"upper('x')", "X"},
		{"substring(s, 1, 5)", "Hello"},
		{"substring(s, -5)", "World"},
		{"substr(s, 7)", "World"},
		{"locate('World', s)", "7"},
		{"locate('z', s)", "0"},
		{"position('o' IN s)", "5"},
		{"trim('  a  ')", "a"},
		{"reverse('abc')", "cba"},
		{"right(s, 5)", "World"},
		{"replace(s, 'o', '0')", "Hell0 W0rld"},
		{"regexp_replace(s, '(\\\\w+) (\\\\w+)', '$2 $1')", "World Hello"},
		{"like(s, 'hello%')", "true"},
		{"like(s, 'hello%', true)", "false"},
		{"ilike(s, '%WORLD')", "true"},
		{"s like 'H_llo%'", "true"},
		{"s contains 'world'", "true"},
		{"regexp_match(s, '^H.*d$')", "true"},
		{"s regexp 'xyz'", "false"},
		{"md5('abc')", "900150983cd24fb0d6963f7d28e17f72"},
		{"sha1('abc')", "a9993e364706816aba3e25717850c26c9cd0d89d"},
		{"sha2('abc', 256)", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"},
		{"cast('12' as int)", "12"},
		{"cast(3.9 as int)", "3"},
		{"cast(n as string)", "7"},
		{"cast('true' as boolean)", "true"},
		{"cast('x' as int)", "<null>"},
		{"tostring(255, 'hex')", "0xff"},
		{"tostring(1234567, 'commas')", "1,234,567"},
		{"tonumber('3.5')", "3.5"},
		{"tonumber('ff', 16)", "255"},
		{"typeof(n)", "int"},
		{"typeof(arr)", "array"},
		{"cidrmatch(ip, '192.168.1.0/24')", "true"},
		{"cidrmatch(ip, '10.0.0.0/8')", "false"},
		{"year(ts)", "2020"},
		{"month(ts)", "8"},
		{"dayofweek(ts)", "4"},
		{"weekday(ts)", "2"},
		{"dayofyear(ts)", "239"},
		{"hour(ts)", "13"},
		{"minute(ts)", "49"},
		{"quarter(ts)", "3"},
		{"dayname(ts)", "Wednesday"},
		{"monthname(ts)", "August"},
		{"date_format(ts, '%Y-%m-%d %H:%i')", "2020-08-26 13:49"},
		{"date_format(ts, '%W %D %M')", "Wednesday 26th August"},
		{"strftime(ts, '%Y/%m/%d %H:%M:%S')", "2020/08/26 13:49:00"},
		{"str_to_date('01,5,2013', '%d,%m,%Y')", "2013-05-01 00:00:00"},
		{"date_add(ts, INTERVAL 1 MONTH)", "2020-09-26 13:49:00"},
		{"date_sub(ts, INTERVAL 1 DAY)", "2020-08-25 13:49:00"},
		{"adddate(ts, 3)", "2020-08-29 13:49:00"},
		{"datediff(ts, date('2020-08-20'))", "6"},
		{"timestampdiff(HOUR, ts, timestamp('2020-08-27 13:49:00'))", "24"},
		{"timestampadd(MINUTE, 11, ts)", "2020-08-26 14:00:00"},
		{"extract(YEAR_MONTH FROM ts)", "202008"},
		{"last_day(ts)", "2020-08-31 00:00:00"},
		{"to_days(date('2007-10-07'))", "733321"},
		{"from_days(733321)", "2007-10-07 00:00:00"},
		{"week(date('2008-02-20'))", "7"},
		{"week(date('2008-02-20'), 1)", "8"},
		{"yearweek(date('1987-01-01'))", "198652"},
		{"from_unixtime(1220249547)", "2008-09-01 06:12:27"},
		{"unix_timestamp(timestamp('1970-01-02 00:00:00'))", "86400"},
		{"maketime(12, 15, 30)", "12:15:30"},
		{"sec_to_time(3661)", "01:01:01"},
		{"time_to_sec(time('01:00:10'))", "3610"},
		{"period_add(200801, 2)", "200803"},
		{"period_diff(200802, 200703)", "11"},
		{"makedate(2012, 32)", "2012-02-01 00:00:00"},
		{"get_format(DATE, 'USA')", "%m.%d.%Y"},
		{"convert_tz(timestamp('2008-05-15 12:00:00'), '+00:00', '+10:00')", "2008-05-15 22:00:00"},
		{"now()", "2024-03-15 10:30:45"},
		{"curdate()", "2024-03-15 00:00:00"},
		{"earliest('-1d', ts)", "false"},
		{"ts > '2020-01-01'", "true"},
		{"json_valid(json)", "true"},
		{"json_valid('{')", "false"},
		{"json_extract(json, '$.a.b[1]')", "20"},
		{"json_extract(json, '$.c')", "x"},
		{"json_extract(json, '$.a')", `{"b":[10,20]}`},
		{"json_array_length('[1,2,3]')", "3"},
		{"json_keys(json)", `["a","c"]`},
		{"json_object('k', 1, 'v', 'x')", `{"k":1,"v":"x"}`},
		{"json_array(1, 'a', nul)", `[1,"a",null]`},
		{"json_set(json, '$.c', 'y')", `{"a":{"b":[10,20]},"c":"y"}`},
		{"json_delete(json, '$.a')", `{"c":"x"}`},
		{"json_append('{\"a\":[1]}', '$.a', 2)", `{"a":[1,2]}`},
		{"array_length(arr)", "3"},
		{"array(1, 2)", `[1,2]`},
		{"filter(arr, x -> x > 1)", `[2,3]`},
		{"transform(arr, x -> x * 10)", `[10,20,30]`},
		{"transform(arr, (x, i) -> x + i)", `[1,3,5]`},
		{"transform(arr, (x, i) -> i)", `[0,1,2]`},
		{"transform(arr, (x, i) -> filter(arr, y -> y > i))", `[[1,2,3],[2,3],[3]]`},
		{"transform(arr, (x, i) -> reduce(arr, i, (acc, y) -> acc + y * x))", `[6,13,20]`},
		{"forall(arr, x -> x > 0)", "true"},
		{"exists(arr, x -> x > 2)", "true"},
		{"reduce(arr, 0, (acc, x) -> acc + x)", "6"},
		{"reduce(arr, 0, (acc, x) -> acc + x, acc -> acc * 10)", "60"},
		{"mvjoin(arr, ',')", "1,2,3"},
		{"mvappend(1, arr, 'z')", `[1,1,2,3,"z"]`},
		{"split('a,b', ',')", `["a","b"]`},
		{"mvdedup(array(1, 1, 2))", `[1,2]`},
		{"mvindex(arr, 1)", "2"},
		{"mvindex(arr, -1)", "3"},
		{"mvindex(arr, 0, 1)", `[1,2]`},
		{"mvfind(array('a', 'bc'), 'c')", "1"},
		{"mvzip(array('a', 'b'), array(1, 2))", `["a,1","b,2"]`},
		{"1 + concat('a', filter(arr, x -> x > 2))", "<null>"},
		{"sum(1, 2, 3)", "6"},
		{"avg(1, 2)", "1.5"},
		{"max(1, 5, 3)", "5"},
		{"min('b', 'a')", "a"},
		{"isempty('')", "true"},
		{"isblank('  ')", "true"},
	}
	for _, c := range cases {
		got := evalPPL(t, c.expr, row)
		s := got.String()
		if got.IsNull() {
			s = "<null>"
		}
		if got.Type() == event.FieldTypeTimestamp {
			s = got.AsTimestamp().UTC().Format("2006-01-02 15:04:05")
		}
		if got.Type() == event.FieldTypeDuration {
			d := got.AsDuration()
			s = time.Unix(0, 0).UTC().Add(d).Format("15:04:05")
		}
		if s != c.want {
			t.Errorf("%s = %s (%s), want %s", c.expr, s, got.Type(), c.want)
		}
	}
}

func TestIRSearchPredicates(t *testing.T) {
	row := map[string]event.Value{"_raw": event.StringValue("ERROR: connection refused by host db-01")}
	cases := []struct {
		q    string
		want bool
	}{
		{"error", true},
		{"conn*", true},
		{"conne?tion", true},
		{`"connection refused"`, true},
		{`"refused connection"`, false},
		{"db-01", true},
		{"warn", false},
		{"error NOT warn", true},
		{"(warn OR refused) host", true},
	}
	for _, c := range cases {
		res, err := ppl.Compile("search source=t " + c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		f, ok := res.Plan.Root.(*ir.Filter)
		if !ok {
			t.Fatalf("%s: root %T", c.q, res.Plan.Root)
		}
		prog, err := vm.CompileExpr(f.Cond, vm.CompileOptions{Now: testNow})
		if err != nil {
			t.Fatalf("%s: %v", c.q, err)
		}
		var m vm.VM
		v, err := m.Execute(prog, row)
		if err != nil {
			t.Fatal(err)
		}
		got := v.Type() == event.FieldTypeBool && v.AsBool()
		if got != c.want {
			t.Errorf("search %s on %q = %v, want %v", c.q, row["_raw"], v, c.want)
		}
	}
}
