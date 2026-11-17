package regression

import (
	"fmt"
	"testing"
)

// SSH Dataset: OpenSSH_2k.log (1999 non-blank lines → 2000 events)

func TestRegression_SSH(t *testing.T) {
	eng := sshEngine(t)

	// Ingestion
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("TotalCount_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
		t.Run("HEAD_10", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | head 10`)
			requireRowCount(t, rows, 10)
		})
		t.Run("HEAD_LargeN_CappedByData", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | head 5000`)
			requireRowCount(t, rows, 2000)
		})
	})

	// Search keywords and phrases
	t.Run("Search", func(t *testing.T) {
		t.Run("FailedPassword_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "Failed password" | stats count() as count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("CaseInsensitive_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "failed password" | stats count() as count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("BREAKIN_85", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "BREAK-IN ATTEMPT" | stats count() as count`)
			requireAggValue(t, rows, "count", 85)
		})
		t.Run("ImplicitAND_Root_370", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "Failed password" root | stats count() as count`)
			requireAggValue(t, rows, "count", 370)
		})
		t.Run("OR_SessionOpenedClosed_2", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "session opened" OR "session closed" | stats count() as count`)
			requireAggValue(t, rows, "count", 2)
		})
		t.Run("NOT_FailedPassword_NotRoot_150", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "Failed password" NOT root | stats count() as count`)
			requireAggValue(t, rows, "count", 150)
		})
		t.Run("Wildcard_InvalidUserFrom_252", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "(?i)Invalid user.*from") | stats count() as count`)
			requireAggValue(t, rows, "count", 252)
		})
		t.Run("Nonexistent_Returns0", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main NONEXISTENT_STRING_12345 | stats count() as count`)
			requireAggValue(t, rows, "count", 0)
		})
	})

	// WHERE
	t.Run("WHERE", func(t *testing.T) {
		t.Run("IsNotNull_TargetUser_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Failed password for (?<target_user>\w+)" | where isnotnull(target_user) | stats count() as count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("IsNull_TargetUser_1480", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Failed password for (?<target_user>\w+)" | where isnull(target_user) | stats count() as count`)
			requireAggValue(t, rows, "count", 1480)
		})
		t.Run("Match_IP_Positive", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "173\.234\.31\.186") | stats count() as count`)
			total := getInt(rows, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events matching IP, got %d", total)
			}
		})
		t.Run("RegexStartsWith_Dec10_09_676", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "^Dec 10 09:") | stats count() as count`)
			requireAggValue(t, rows, "count", 676)
		})
		t.Run("WhereTrue_1Eq1_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where 1 = 1 | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// REX
	t.Run("REX", func(t *testing.T) {
		t.Run("UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip_addr) | stats dc(ip_addr) as unique_ips`)
			requireAggValue(t, rows, "unique_ips", 30)
		})
		t.Run("TopUsername_Admin_21", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Invalid user (?<username>\w+) from" | where isnotnull(username) | stats count() as count by username | sort - count | head 3`)
			if len(rows) < 3 {
				t.Fatalf("expected at least 3 rows, got %d", len(rows))
			}
			top := fmt.Sprint(rows[0]["username"])
			topCount := toInt(rows[0]["count"])
			if top != "admin" {
				t.Errorf("expected top username=admin, got %s", top)
			}
			if topCount != 21 {
				t.Errorf("expected admin count=21, got %d", topCount)
			}
		})
		t.Run("ExtractPort_525", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "port (?<port>\d+)" | where isnotnull(port) | stats count() as count`)
			requireAggValue(t, rows, "count", 525)
		})
		t.Run("ChainedREX_PositiveTargetsAndIPs", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(target) and isnotnull(src_ip) | stats dc(target) as unique_targets, dc(src_ip) as unique_ips`)
			targets := getInt(rows, "unique_targets")
			ips := getInt(rows, "unique_ips")
			if targets <= 0 || ips <= 0 {
				t.Errorf("expected unique_targets > 0 and unique_ips > 0, got targets=%d ips=%d", targets, ips)
			}
		})
		t.Run("NoMatch_Returns0", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "NONEXISTENT_PATTERN_(?<captured>\w+)" | stats count(captured) as matched`)
			requireAggValue(t, rows, "matched", 0)
		})
	})

	// EVAL
	t.Run("EVAL", func(t *testing.T) {
		t.Run("StringAssignment", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | eval source_type = "ssh_log" | head 1 | fields source_type`)
			requireRowCount(t, rows, 1)
			st := fmt.Sprint(rows[0]["source_type"])
			if st != "ssh_log" {
				t.Errorf("expected source_type=ssh_log, got %s", st)
			}
		})
		t.Run("IF_AllPublicIPs", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | eval ip_class = if(regexp_match(ip_addr, "^10\."), "private", "public") | where isnotnull(ip_addr) | stats count() as count by ip_class`)
			for _, row := range rows {
				cls := fmt.Sprint(row["ip_class"])
				if cls == "private" {
					t.Error("unexpected private IP in SSH logs")
				}
			}
		})
		t.Run("CASE_FailedAuth_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | eval event_type = case(
          regexp_match(_raw, "Failed password"), "failed_auth",
          regexp_match(_raw, "Invalid user"), "invalid_user",
          regexp_match(_raw, "Accepted"), "success",
          regexp_match(_raw, "Connection closed"), "conn_closed",
          regexp_match(_raw, "Received disconnect"), "disconnect",
          regexp_match(_raw, "BREAK-IN"), "breakin_attempt"
          else "other"
      )
    | stats count() as count by event_type
    | sort - count`)
			types := rowsToMap(rows, "event_type", "count")
			if types["failed_auth"] != 520 {
				t.Errorf("expected failed_auth=520, got %d", types["failed_auth"])
			}
		})
		t.Run("Coalesce_NA_1480", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "Failed password for (?<target>\w+)"
    | eval user_or_unknown = coalesce(target, "N/A")
    | stats count() as count by user_or_unknown
    | where user_or_unknown = "N/A"`)
			requireAggValue(t, rows, "count", 1480)
		})
		t.Run("NullPropagation_1475", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "port (?<port>\d+)"
    | eval port_plus_one = cast(port as double) + 1
    | where isnull(port_plus_one)
    | stats count() as count`)
			requireAggValue(t, rows, "count", 1475)
		})
		t.Run("MultipleAssignments_WithPort_525", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | rex field=_raw "port (?<port>\d+)"
    | eval has_ip = if(isnotnull(ip), 1, 0),
           has_port = if(isnotnull(port), 1, 0),
           connection_info = has_ip + has_port
    | stats sum(has_ip) as with_ip, sum(has_port) as with_port`)
			withPort := getInt(rows, "with_port")
			if withPort != 525 {
				t.Errorf("expected with_port=525, got %d", withPort)
			}
		})
		t.Run("Length_PositiveAvgMaxMin", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | eval raw_len = length(_raw) | stats avg(raw_len) as avg_length, max(raw_len) as max_length, min(raw_len) as min_length`)
			avg := getFloat(rows, "avg_length")
			maxLen := getFloat(rows, "max_length")
			minLen := getFloat(rows, "min_length")
			if avg <= 0 || maxLen <= 0 || minLen <= 0 {
				t.Errorf("expected positive lengths: avg=%f max=%f min=%f", avg, maxLen, minLen)
			}
			if maxLen < avg || minLen > avg {
				t.Errorf("inconsistent: min=%f avg=%f max=%f", minLen, avg, maxLen)
			}
		})
	})

	// STATS
	t.Run("STATS", func(t *testing.T) {
		t.Run("Count_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
		t.Run("DC_UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | stats dc(ip) as unique_ips`)
			requireAggValue(t, rows, "unique_ips", 30)
		})
		t.Run("MaxFromSingleIP_867", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | stats count() as count by ip
    | stats max(count) as max_from_single_ip`)
			requireAggValue(t, rows, "max_from_single_ip", 867)
		})
		t.Run("TopUsernames_Admin21", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Invalid user (?<username>\w+)" | where isnotnull(username) | stats count() as count by username | sort - count | head 5`)
			if len(rows) != 5 {
				t.Errorf("expected 5 rows, got %d", len(rows))
			}
			if len(rows) > 0 {
				name := fmt.Sprint(rows[0]["username"])
				cnt := toInt(rows[0]["count"])
				if name != "admin" || cnt != 21 {
					t.Errorf("expected admin(21), got %s(%d)", name, cnt)
				}
			}
		})
		t.Run("EarliestLatest_NonEmpty", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | stats earliest(_time) as first_event, latest(_time) as last_event`)
			first := getStr(rows, "first_event")
			last := getStr(rows, "last_event")
			if first == "" || last == "" {
				t.Errorf("expected non-empty timestamps: first=%q last=%q", first, last)
			}
		})
		t.Run("NestedEval_RequestsPerIP", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | stats count() as requests, dc(ip) as unique_ips
    | eval requests_per_ip = round(requests / unique_ips, 2)`)
			rpi := getFloat(rows, "requests_per_ip")
			if rpi <= 0 {
				t.Errorf("expected requests_per_ip > 0, got %f", rpi)
			}
		})
	})

	// BIN
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span1h_SumsTo2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | bin _time span=1h as hour_bucket | stats count() as count by hour_bucket | sort hour_bucket`)
			if len(rows) < 5 {
				t.Errorf("expected at least 5 hour buckets, got %d", len(rows))
			}
			total := 0
			for _, row := range rows {
				total += toInt(row["count"])
			}
			if total != 2000 {
				t.Errorf("expected bucket totals to sum to 2000, got %d", total)
			}
		})
		t.Run("Span1m_Top5", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | bin _time span=1m as minute_bucket | stats count() as count by minute_bucket | sort - count | head 5`)
			requireRowCount(t, rows, 5)
		})
	})

	// SORT
	t.Run("SORT", func(t *testing.T) {
		t.Run("Ascending_Order", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort count | head 3`)
			if len(rows) != 3 {
				t.Fatalf("expected 3 rows, got %d", len(rows))
			}
			for i := 1; i < len(rows); i++ {
				prev := toInt(rows[i-1]["count"])
				curr := toInt(rows[i]["count"])
				if curr < prev {
					t.Errorf("not ascending: row[%d]=%d < row[%d]=%d", i, curr, i-1, prev)
				}
			}
		})
		t.Run("Descending_TopIP_867", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort - count | head 1`)
			if len(rows) == 0 {
				t.Fatal("expected at least 1 row")
			}
			ip := fmt.Sprint(rows[0]["ip"])
			count := toInt(rows[0]["count"])
			if ip != "183.62.140.253" {
				t.Errorf("expected top IP=183.62.140.253, got %s", ip)
			}
			if count != 867 {
				t.Errorf("expected count=867, got %d", count)
			}
		})
		t.Run("PreservesAllRows_30IPs", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort count | stats count() as count`)
			requireAggValue(t, rows, "count", 30)
		})
		t.Run("StringField_Alphabetical", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "Invalid user (?<username>\w+)" | where isnotnull(username) | stats count() as count by username | sort username | head 3`)
			if len(rows) != 3 {
				t.Fatalf("expected 3 rows, got %d", len(rows))
			}
			for i := 1; i < len(rows); i++ {
				prev := fmt.Sprint(rows[i-1]["username"])
				curr := fmt.Sprint(rows[i]["username"])
				if curr < prev {
					t.Errorf("not alphabetical: %s < %s", curr, prev)
				}
			}
		})
	})

	// RENAME and FIELDS
	t.Run("RenameFields", func(t *testing.T) {
		t.Run("Rename_DC_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | rename ip as source_ip
    | where isnotnull(source_ip)
    | stats dc(source_ip) as unique_sources`)
			requireAggValue(t, rows, "unique_sources", 30)
		})
		t.Run("Fields_Raw_3Rows", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | head 3 | fields _raw`)
			requireRowCount(t, rows, 3)
			for i, row := range rows {
				if _, ok := row["_raw"]; !ok {
					t.Errorf("row %d missing _raw field", i)
				}
			}
		})
	})

	// DEDUP
	t.Run("DEDUP", func(t *testing.T) {
		t.Run("UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | dedup ip
    | stats count() as count`)
			requireAggValue(t, rows, "count", 30)
		})
		t.Run("WithLimit_NoneOver3", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | dedup 3 ip
    | stats count() as count by ip
    | where count > 3
    | stats count() as count`)
			requireAggValue(t, rows, "count", 0)
		})
	})

	// EVENTSTATS
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("WithBY_TopIP_867", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | eventstats count() as ip_count by ip
    | where ip = "183.62.140.253"
    | head 1
    | fields ip, ip_count`)
			ipCount := getInt(rows, "ip_count")
			if ipCount != 867 {
				t.Errorf("expected ip_count=867, got %d", ipCount)
			}
		})
		t.Run("DoesNotReduce_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | eventstats count() as total | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// STREAMSTATS
	t.Run("STREAMSTATS", func(t *testing.T) {
		t.Run("RunningCount_10Rows", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | streamstats count() as row_num | where row_num <= 10 | fields row_num`)
			requireRowCount(t, rows, 10)
		})
		t.Run("CurrentTrue_LastRow2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | streamstats current=true count() as running_total
    | where running_total = 2000
    | stats count() as count`)
			requireAggValue(t, rows, "count", 1)
		})
		t.Run("WithBY_FirstOccurrence", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | streamstats count() as ip_running_count by ip
    | where ip = "183.62.140.253" and ip_running_count = 1
    | stats count() as count`)
			requireAggValue(t, rows, "count", 1)
		})
	})

	// Complex Pipelines
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("BruteForceDetection_Has183", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+) port (?<port>\d+)"
    | where isnotnull(src_ip)
    | stats count() as attempts, dc(target) as unique_targets by src_ip
    | where attempts > 50
    | sort - attempts`)
			if len(rows) < 1 {
				t.Fatal("expected at least 1 brute force IP")
			}
			found := false
			for _, row := range rows {
				if fmt.Sprint(row["src_ip"]) == "183.62.140.253" {
					found = true
				}
			}
			if !found {
				t.Error("expected 183.62.140.253 in brute force results")
			}
		})
		t.Run("EventClassification_PercentagesSumTo100", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | eval category = case(
          regexp_match(_raw, "Failed password"), "auth_failure",
          regexp_match(_raw, "Accepted"), "auth_success",
          regexp_match(_raw, "Invalid user"), "invalid_user",
          regexp_match(_raw, "BREAK-IN"), "breakin",
          regexp_match(_raw, "Connection closed"), "conn_closed",
          regexp_match(_raw, "Received disconnect"), "disconnect",
          regexp_match(_raw, "pam_unix"), "pam"
          else "other"
      )
    | stats count() as count by category
    | eventstats sum(count) as total
    | eval pct = round(count * 100.0 / total, 1)
    | sort - count`)
			totalPct := 0.0
			for _, row := range rows {
				totalPct += toFloat(row["pct"])
			}
			if totalPct < 99 || totalPct > 101 {
				t.Errorf("expected percentages to sum to ~100, got %f", totalPct)
			}
		})
		t.Run("TwoLevelAggregation_AtLeast2ThreatLevels", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | stats count() as count by ip
    | eval threat_level = case(
          count > 500, "critical",
          count > 100, "high",
          count > 50, "medium"
          else "low"
      )
    | stats count() as count by threat_level
    | sort - count`)
			if len(rows) < 2 {
				t.Errorf("expected at least 2 threat levels, got %d", len(rows))
			}
		})
	})

	// Wildcard search: search wildcards match whole tokens; patterns that
	// span tokens use regexp_match.
	t.Run("WildcardSearch", func(t *testing.T) {
		// Prefix
		t.Run("Prefix_Failed_610", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main Failed* | stats count() as count`)
			requireAggValue(t, rows, "count", 610)
		})
		// Suffix
		t.Run("Suffix_Preauth_618", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main *preauth | stats count() as count`)
			requireAggValue(t, rows, "count", 618)
		})
		// Contains
		t.Run("Contains_Password_521", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where _raw contains "password" | stats count() as count`)
			requireAggValue(t, rows, "count", 521)
		})
		// Multi-wildcard
		t.Run("Multi_FailedFromPort_524", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "(?i)Failed.*from.*port") | stats count() as count`)
			requireAggValue(t, rows, "count", 524)
		})
		// All
		t.Run("WildcardOnly_All_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main * | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
		// Boolean combos
		t.Run("WildcardAND_Root_370", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main Failed* root | stats count() as count`)
			requireAggValue(t, rows, "count", 370)
		})
		t.Run("WildcardOR_FailedOrInvalid_836", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main Failed* OR Invalid* | stats count() as count`)
			requireAggValue(t, rows, "count", 836)
		})
		t.Run("WildcardNOT_NotRoot_1257", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where not _raw contains "root" | stats count() as count`)
			requireAggValue(t, rows, "count", 1257)
		})
		// Field comparison
		t.Run("FieldPrefix_IP173_10", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | where like(ip, "173.234.%") | stats count() as count`)
			requireAggValue(t, rows, "count", 10)
		})
		t.Run("FieldExistence_Port_525", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "port (?<port>\d+)" | where isnotnull(port) | stats count() as count`)
			total := getInt(rows, "count")
			if total != 525 {
				t.Errorf("expected 525 events with port field, got %d", total)
			}
		})
	})
}

// Sort Elimination Correctness: verify that optimizer sort elimination does not
// change query results. These tests execute queries through the full engine
// (compiler -> optimizer -> executor) and check result correctness.

func TestRegression_SortElimination(t *testing.T) {
	eng := sshEngine(t)

	t.Run("SortTime_StatsCount", func(t *testing.T) {
		// sort _time | stats count -> sort eliminated (dead sort before stats), correct count
		rows := mustQuery(t, eng, `source=main | sort _time | stats count() as count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("SortTime_StatsAvgByHost", func(t *testing.T) {
		// sort _time | stats dc(ip) -> sort eliminated, correct aggregation
		rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | sort _time | stats dc(ip) as unique_ips`)
		requireAggValue(t, rows, "unique_ips", 30)
	})

	t.Run("SortTime_DedupField", func(t *testing.T) {
		// sort _time | dedup ip -> correct count
		rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | sort _time | dedup ip | stats count() as count`)
		requireAggValue(t, rows, "count", 30)
	})

	t.Run("SortTime_EvalStatsCount", func(t *testing.T) {
		// sort _time | eval x=1 | stats count -> sort eliminated (eval preserves, stats destroys)
		rows := mustQuery(t, eng, `source=main | sort _time | eval x = 1 | stats count() as count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("SortTime_Head5", func(t *testing.T) {
		// sort _time | head 5 -> sort kept (or fused to TopN), correct 5 rows
		rows := mustQuery(t, eng, `source=main | sort _time | head 5`)
		requireRowCount(t, rows, 5)
	})

	t.Run("SortTime_StreamstatsCount", func(t *testing.T) {
		// sort _time | streamstats count -> sort kept (streamstats depends on order)
		rows := mustQuery(t, eng, `source=main | sort _time | streamstats count() as row_num | where row_num <= 10 | stats count() as count`)
		requireAggValue(t, rows, "count", 10)
	})

	t.Run("DoubleSortEliminated_StatsCount", func(t *testing.T) {
		// sort a | sort -b | stats count -> both sorts eliminated, correct count
		rows := mustQuery(t, eng, `source=main | sort _time | sort -_time | stats count() as count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("FirstSortEliminated_SecondKept", func(t *testing.T) {
		// sort _time | stats count | sort -count -> first sort eliminated, second kept
		rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | sort _time | stats count() as count by ip | sort -count | head 1`)
		requireRowCount(t, rows, 1)
		// Top IP by count should be 183.62.140.253 with 867 events.
		topCount := toInt(rows[0]["count"])
		if topCount != 867 {
			t.Errorf("expected top count=867, got %d", topCount)
		}
	})

	t.Run("TerminalSort_CorrectOrder", func(t *testing.T) {
		// Terminal sort count | head 5 -> correct ascending order verified
		rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort count | head 5`)
		requireRowCount(t, rows, 5)
		for i := 1; i < len(rows); i++ {
			prev := toInt(rows[i-1]["count"])
			curr := toInt(rows[i]["count"])
			if curr < prev {
				t.Errorf("not ascending at position %d: %d < %d", i, curr, prev)
			}
		}
	})
}

// OpenStack Dataset: OpenStack_2k.log (1999 non-blank lines -> 2000 events)

func TestRegression_OpenStack(t *testing.T) {
	eng := openstackEngine(t)

	// Ingestion
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("TotalCount_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | stats count() as count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// Search keywords and phrases
	t.Run("Search", func(t *testing.T) {
		t.Run("VMStarted_22", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "VM Started" | stats count() as count`)
			requireAggValue(t, rows, "count", 22)
		})
		t.Run("WARNING_31", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main WARNING | stats count() as count`)
			requireAggValue(t, rows, "count", 31)
		})
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `search source=main "Lifecycle Event" | stats count() as count`)
			requireAggValue(t, rows, "count", 109)
		})
	})

	// WHERE
	t.Run("WHERE", func(t *testing.T) {
		t.Run("OR_VMStartedOrStopped_43", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "VM Started") OR regexp_match(_raw, "VM Stopped") | stats count() as count`)
			requireAggValue(t, rows, "count", 43)
		})
		t.Run("NumericGTE_Status400_41", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "status: (?<status>\d+)" | where isnotnull(status) | where cast(status as double) >= 400 | stats count() as count`)
			requireAggValue(t, rows, "count", 41)
		})
	})

	// REX
	t.Run("REX", func(t *testing.T) {
		t.Run("LogLevel_INFO1969_WARNING31", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \d+ (?<log_level>\w+)" | stats count() as count by log_level | sort log_level`)
			found := rowsToMap(rows, "log_level", "count")
			if found["INFO"] != 1969 {
				t.Errorf("expected INFO=1969, got %d", found["INFO"])
			}
			if found["WARNING"] != 31 {
				t.Errorf("expected WARNING=31, got %d", found["WARNING"])
			}
		})
		t.Run("HTTPStatus_200_933_404_41", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "status: (?<http_status>\d+)" | where isnotnull(http_status) | stats count() as count by http_status | sort - count`)
			statusCounts := rowsToMap(rows, "http_status", "count")
			expected := map[string]int{"200": 933, "404": 41, "204": 22, "202": 21}
			for status, count := range expected {
				if statusCounts[status] != count {
					t.Errorf("status %s: expected %d, got %d", status, count, statusCounts[status])
				}
			}
		})
		t.Run("HTTPMethod_GET931_POST64_DELETE22", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "(?<http_method>GET|POST|PUT|DELETE|PATCH) /" | where isnotnull(http_method) | stats count() as count by http_method | sort - count`)
			methods := rowsToMap(rows, "http_method", "count")
			if methods["GET"] != 931 {
				t.Errorf("GET: expected 931, got %d", methods["GET"])
			}
			if methods["POST"] != 64 {
				t.Errorf("POST: expected 64, got %d", methods["POST"])
			}
			if methods["DELETE"] != 22 {
				t.Errorf("DELETE: expected 22, got %d", methods["DELETE"])
			}
		})
		t.Run("InstanceUUID_DC22", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "\[instance: (?<instance_id>[a-f0-9-]+)\]" | where isnotnull(instance_id) | stats dc(instance_id) as unique_instances`)
			requireAggValue(t, rows, "unique_instances", 22)
		})
	})

	// EVAL
	t.Run("EVAL", func(t *testing.T) {
		t.Run("Arithmetic_SlowRequests_81", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval resp_ms = round(cast(resp_time as double) * 1000, 2)
    | where resp_ms > 300
    | stats count() as count`)
			requireAggValue(t, rows, "count", 81)
		})
		t.Run("Lower_INFO1969_WARNING31", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "\d+ (?<level>[A-Z]+) " | eval level_lower = lower(level) | stats count() as count by level_lower`)
			levels := rowsToMap(rows, "level_lower", "count")
			if levels["info"] != 1969 {
				t.Errorf("expected info=1969, got %d", levels["info"])
			}
			if levels["warning"] != 31 {
				t.Errorf("expected warning=31, got %d", levels["warning"])
			}
		})
		t.Run("Substring_Positive", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | eval log_source = substring(_raw, 1, 10) | stats dc(log_source) as unique_prefixes`)
			prefixes := getInt(rows, "unique_prefixes")
			if prefixes < 3 {
				t.Errorf("expected at least 3 unique prefixes, got %d", prefixes)
			}
		})
	})

	// STATS
	t.Run("STATS", func(t *testing.T) {
		t.Run("CountBY_Status200_933", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "status: (?<status>\d+)" | where isnotnull(status) | stats count() as count by status | sort - count`)
			if len(rows) < 4 {
				t.Errorf("expected at least 4 status codes, got %d", len(rows))
			}
			statusCounts := rowsToMap(rows, "status", "count")
			if statusCounts["200"] != 933 {
				t.Errorf("status 200: expected 933, got %d", statusCounts["200"])
			}
			if statusCounts["404"] != 41 {
				t.Errorf("status 404: expected 41, got %d", statusCounts["404"])
			}
		})
		t.Run("MinMax_ResponseTime", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "time: (?<resp_time>[0-9.]+)" | where isnotnull(resp_time) | eval rt = cast(resp_time as double) | stats min(rt) as min_time, max(rt) as max_time`)
			minT := getFloat(rows, "min_time")
			maxT := getFloat(rows, "max_time")
			if minT >= maxT {
				t.Errorf("expected min < max: min=%f max=%f", minT, maxT)
			}
			if minT < 0 {
				t.Errorf("expected min >= 0, got %f", minT)
			}
		})
		t.Run("Percentile_P95GEMedian", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval rt = cast(resp_time as double)
    | stats p95(rt) as p95, p50(rt) as median`)
			p95 := getFloat(rows, "p95")
			median := getFloat(rows, "median")
			if p95 < median {
				t.Errorf("expected p95 >= median: p95=%f median=%f", p95, median)
			}
			if p95 <= 0 || median <= 0 {
				t.Errorf("expected positive: p95=%f median=%f", p95, median)
			}
		})
		t.Run("Sum_PositiveTotalBytes", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | rex field=_raw "len: (?<resp_len>\d+)" | where isnotnull(resp_len) | eval resp_len_num = cast(resp_len as double) | stats sum(resp_len_num) as total_bytes, avg(resp_len_num) as avg_bytes`)
			total := getFloat(rows, "total_bytes")
			avg := getFloat(rows, "avg_bytes")
			if total <= 0 || avg <= 0 {
				t.Errorf("expected positive values: total=%f avg=%f", total, avg)
			}
		})
	})

	// BIN
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span5m_SumsTo2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | bin _time span=5m as time_bucket | stats count() as count by time_bucket | sort time_bucket`)
			total := 0
			for _, row := range rows {
				total += toInt(row["count"])
			}
			if total != 2000 {
				t.Errorf("expected 2000 total, got %d", total)
			}
		})
		t.Run("BucketCount_About15", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | bin _time span=1m as bucket | stats dc(bucket) as num_buckets`)
			buckets := getInt(rows, "num_buckets")
			if buckets < 10 || buckets > 20 {
				t.Errorf("expected ~15 buckets, got %d", buckets)
			}
		})
	})

	// EVENTSTATS
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("GlobalAggregation_1017", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(status)
    | eventstats count() as total_requests
    | head 1
    | fields status, total_requests`)
			totalReq := getInt(rows, "total_requests")
			if totalReq != 1017 {
				t.Errorf("expected total_requests=1017, got %d", totalReq)
			}
		})
		t.Run("Percentage_Status200_About92", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(status)
    | stats count() as count by status
    | eventstats sum(count) as total
    | eval pct = round(count * 100.0 / total, 2)
    | sort - pct`)
			if len(rows) == 0 {
				t.Fatal("expected at least 1 row")
			}
			topStatus := fmt.Sprint(rows[0]["status"])
			topPct := toFloat(rows[0]["pct"])
			if topStatus != "200" {
				t.Errorf("expected top status=200, got %s", topStatus)
			}
			if topPct < 90 || topPct > 93 {
				t.Errorf("expected pct ~91.7%%, got %f", topPct)
			}
		})
	})

	// Complex Pipelines
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("InstanceLifecycle_AtLeast3Types", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "\[instance: (?<instance_id>[a-f0-9-]+)\]"
    | where isnotnull(instance_id)
    | eval lifecycle_event = case(
          regexp_match(_raw, "VM Started"), "started",
          regexp_match(_raw, "VM Stopped"), "stopped",
          regexp_match(_raw, "VM Paused"), "paused",
          regexp_match(_raw, "VM Resumed"), "resumed",
          regexp_match(_raw, "spawned successfully"), "spawned",
          regexp_match(_raw, "Deleting instance"), "deleting",
          regexp_match(_raw, "Terminating"), "terminating"
          else "other"
      )
    | stats count() as count by lifecycle_event
    | sort - count`)
			if len(rows) < 3 {
				t.Errorf("expected at least 3 lifecycle event types, got %d", len(rows))
			}
		})
		t.Run("APILatencyAnalysis_AtLeast2Methods", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main
    | rex field=_raw "(?<method>GET|POST|DELETE) (?<url_path>/\S+) HTTP"
    | rex field=_raw "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)"
    | where isnotnull(method)
    | eval resp_ms = round(cast(resp_time as double) * 1000, 2)
    | stats count() as requests, avg(resp_ms) as avg_latency, max(resp_ms) as max_latency by method
    | sort method`)
			if len(rows) < 2 {
				t.Errorf("expected at least 2 methods, got %d", len(rows))
			}
		})
	})

	// Wildcard search
	t.Run("WildcardSearch", func(t *testing.T) {
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where _raw contains "Lifecycle Event" | stats count() as count`)
			requireAggValue(t, rows, "count", 109)
		})
		t.Run("VMEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "(?i)VM.*Event") | stats count() as count`)
			requireAggValue(t, rows, "count", 109)
		})
		t.Run("NovaInstance_923", func(t *testing.T) {
			// Raw data has 923 lines matching nova.*instance (case-insensitive).
			rows := mustQuery(t, eng, `source=main | where regexp_match(_raw, "(?i)nova.*instance") | stats count() as count`)
			requireAggValue(t, rows, "count", 923)
		})
	})
}
