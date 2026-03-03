package regression

import (
	"fmt"
	"testing"
)

// ═══════════════════════════════════════════════════════════════════════════════
// SSH Dataset: OpenSSH_2k.log (1999 non-blank lines → 2000 events)
// ═══════════════════════════════════════════════════════════════════════════════

func TestRegression_SSH(t *testing.T) {
	eng := sshEngine(t)

	// ─── Ingestion ───────────────────────────────────────────────────────
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("TotalCount_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
		t.Run("HEAD_10", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | HEAD 10`)
			requireRowCount(t, rows, 10)
		})
		t.Run("HEAD_LargeN_CappedByData", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | HEAD 5000`)
			requireRowCount(t, rows, 2000)
		})
	})

	// ─── Search Keywords ─────────────────────────────────────────────────
	t.Run("Search", func(t *testing.T) {
		t.Run("FailedPassword_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed password" | STATS count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("CaseInsensitive_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "failed password" | STATS count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("BREAKIN_85", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "BREAK-IN ATTEMPT" | STATS count`)
			requireAggValue(t, rows, "count", 85)
		})
		t.Run("ImplicitAND_Root_370", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed password" root | STATS count`)
			requireAggValue(t, rows, "count", 370)
		})
		t.Run("OR_SessionOpenedClosed_2", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "session opened" OR "session closed" | STATS count`)
			requireAggValue(t, rows, "count", 2)
		})
		t.Run("NOT_FailedPassword_NotRoot_150", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed password" NOT root | STATS count`)
			requireAggValue(t, rows, "count", 150)
		})
		t.Run("Wildcard_InvalidUserFrom_252", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Invalid user*from" | STATS count`)
			requireAggValue(t, rows, "count", 252)
		})
		t.Run("Nonexistent_Returns0", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "NONEXISTENT_STRING_12345" | STATS count`)
			requireAggValue(t, rows, "count", 0)
		})
	})

	// ─── WHERE ───────────────────────────────────────────────────────────
	t.Run("WHERE", func(t *testing.T) {
		t.Run("IsNotNull_TargetUser_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Failed password for (?<target_user>\w+)" | WHERE isnotnull(target_user) | STATS count`)
			requireAggValue(t, rows, "count", 520)
		})
		t.Run("IsNull_TargetUser_1480", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Failed password for (?<target_user>\w+)" | WHERE isnull(target_user) | STATS count`)
			requireAggValue(t, rows, "count", 1480)
		})
		t.Run("Match_IP_Positive", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | WHERE match(_raw, "173\.234\.31\.186") | STATS count`)
			total := getInt(rows, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events matching IP, got %d", total)
			}
		})
		t.Run("RegexStartsWith_Dec10_09_676", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | WHERE match(_raw, "^Dec 10 09:") | STATS count`)
			requireAggValue(t, rows, "count", 676)
		})
		t.Run("WhereTrue_1Eq1_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | WHERE 1=1 | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// ─── REX ─────────────────────────────────────────────────────────────
	t.Run("REX", func(t *testing.T) {
		t.Run("UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip_addr) | STATS dc(ip_addr) AS unique_ips`)
			requireAggValue(t, rows, "unique_ips", 30)
		})
		t.Run("TopUsername_Admin_21", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Invalid user (?<username>\w+) from" | WHERE isnotnull(username) | STATS count BY username | SORT - count | HEAD 3`)
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
			rows := mustQuery(t, eng, `FROM main | REX "port (?<port>\d+)" | WHERE isnotnull(port) | STATS count`)
			requireAggValue(t, rows, "count", 525)
		})
		t.Run("ChainedREX_PositiveTargetsAndIPs", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(target) AND isnotnull(src_ip) | STATS dc(target) AS unique_targets, dc(src_ip) AS unique_ips`)
			targets := getInt(rows, "unique_targets")
			ips := getInt(rows, "unique_ips")
			if targets <= 0 || ips <= 0 {
				t.Errorf("expected unique_targets > 0 and unique_ips > 0, got targets=%d ips=%d", targets, ips)
			}
		})
		t.Run("NoMatch_Returns0", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "NONEXISTENT_PATTERN_(?<captured>\w+)" | STATS count(captured) AS matched`)
			requireAggValue(t, rows, "matched", 0)
		})
	})

	// ─── EVAL ────────────────────────────────────────────────────────────
	t.Run("EVAL", func(t *testing.T) {
		t.Run("StringAssignment", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | EVAL source_type = "ssh_log" | HEAD 1 | TABLE source_type`)
			requireRowCount(t, rows, 1)
			st := fmt.Sprint(rows[0]["source_type"])
			if st != "ssh_log" {
				t.Errorf("expected source_type=ssh_log, got %s", st)
			}
		})
		t.Run("IF_AllPublicIPs", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | EVAL ip_class = IF(match(ip_addr, "^10\."), "private", "public") | WHERE isnotnull(ip_addr) | STATS count BY ip_class`)
			for _, row := range rows {
				cls := fmt.Sprint(row["ip_class"])
				if cls == "private" {
					t.Error("unexpected private IP in SSH logs")
				}
			}
		})
		t.Run("CASE_FailedAuth_520", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | EVAL event_type = CASE(
          match(_raw, "Failed password"), "failed_auth",
          match(_raw, "Invalid user"), "invalid_user",
          match(_raw, "Accepted"), "success",
          match(_raw, "Connection closed"), "conn_closed",
          match(_raw, "Received disconnect"), "disconnect",
          match(_raw, "BREAK-IN"), "breakin_attempt",
          1=1, "other"
      )
    | STATS count BY event_type
    | SORT - count`)
			types := rowsToMap(rows, "event_type", "count")
			if types["failed_auth"] != 520 {
				t.Errorf("expected failed_auth=520, got %d", types["failed_auth"])
			}
		})
		t.Run("Coalesce_NA_1480", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "Failed password for (?<target>\w+)"
    | EVAL user_or_unknown = coalesce(target, "N/A")
    | STATS count BY user_or_unknown
    | WHERE user_or_unknown = "N/A"`)
			requireAggValue(t, rows, "count", 1480)
		})
		t.Run("NullPropagation_1475", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "port (?<port>\d+)"
    | EVAL port_plus_one = tonumber(port) + 1
    | WHERE isnull(port_plus_one)
    | STATS count`)
			requireAggValue(t, rows, "count", 1475)
		})
		t.Run("MultipleAssignments_WithPort_525", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | REX "port (?<port>\d+)"
    | EVAL has_ip = IF(isnotnull(ip), 1, 0),
           has_port = IF(isnotnull(port), 1, 0),
           connection_info = has_ip + has_port
    | STATS sum(has_ip) AS with_ip, sum(has_port) AS with_port`)
			withPort := getInt(rows, "with_port")
			if withPort != 525 {
				t.Errorf("expected with_port=525, got %d", withPort)
			}
		})
		t.Run("Len_PositiveAvgMaxMin", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | EVAL raw_len = len(_raw) | STATS avg(raw_len) AS avg_length, max(raw_len) AS max_length, min(raw_len) AS min_length`)
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

	// ─── STATS ───────────────────────────────────────────────────────────
	t.Run("STATS", func(t *testing.T) {
		t.Run("Count_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
		t.Run("DC_UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | STATS dc(ip) AS unique_ips`)
			requireAggValue(t, rows, "unique_ips", 30)
		})
		t.Run("MaxFromSingleIP_867", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STATS count BY ip
    | STATS max(count) AS max_from_single_ip`)
			requireAggValue(t, rows, "max_from_single_ip", 867)
		})
		t.Run("TopUsernames_Admin21", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Invalid user (?<username>\w+)" | WHERE isnotnull(username) | STATS count BY username | SORT - count | HEAD 5`)
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
			rows := mustQuery(t, eng, `FROM main | STATS earliest(_time) AS first_event, latest(_time) AS last_event`)
			first := getStr(rows, "first_event")
			last := getStr(rows, "last_event")
			if first == "" || last == "" {
				t.Errorf("expected non-empty timestamps: first=%q last=%q", first, last)
			}
		})
		t.Run("NestedEval_RequestsPerIP", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STATS count AS requests, dc(ip) AS unique_ips
    | EVAL requests_per_ip = round(requests / unique_ips, 2)`)
			rpi := getFloat(rows, "requests_per_ip")
			if rpi <= 0 {
				t.Errorf("expected requests_per_ip > 0, got %f", rpi)
			}
		})
	})

	// ─── BIN ─────────────────────────────────────────────────────────────
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span1h_SumsTo2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | BIN _time span=1h AS hour_bucket | STATS count BY hour_bucket | SORT hour_bucket`)
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
			rows := mustQuery(t, eng, `FROM main | BIN _time span=1m AS minute_bucket | STATS count BY minute_bucket | SORT - count | HEAD 5`)
			requireRowCount(t, rows, 5)
		})
	})

	// ─── SORT ────────────────────────────────────────────────────────────
	t.Run("SORT", func(t *testing.T) {
		t.Run("Ascending_Order", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT count | HEAD 3`)
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
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT - count | HEAD 1`)
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
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT count | STATS count`)
			requireAggValue(t, rows, "count", 30)
		})
		t.Run("StringField_Alphabetical", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "Invalid user (?<username>\w+)" | WHERE isnotnull(username) | STATS count BY username | SORT username | HEAD 3`)
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

	// ─── RENAME/TABLE ────────────────────────────────────────────────────
	t.Run("RenameTable", func(t *testing.T) {
		t.Run("Rename_DC_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | RENAME ip AS source_ip
    | WHERE isnotnull(source_ip)
    | STATS dc(source_ip) AS unique_sources`)
			requireAggValue(t, rows, "unique_sources", 30)
		})
		t.Run("Table_Raw_3Rows", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | HEAD 3 | TABLE _raw`)
			requireRowCount(t, rows, 3)
			for i, row := range rows {
				if _, ok := row["_raw"]; !ok {
					t.Errorf("row %d missing _raw field", i)
				}
			}
		})
	})

	// ─── DEDUP ───────────────────────────────────────────────────────────
	t.Run("DEDUP", func(t *testing.T) {
		t.Run("UniqueIPs_30", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | DEDUP ip
    | STATS count`)
			requireAggValue(t, rows, "count", 30)
		})
		t.Run("WithLimit_NoneOver3", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | DEDUP 3 ip
    | STATS count BY ip
    | WHERE count > 3
    | STATS count`)
			requireAggValue(t, rows, "count", 0)
		})
	})

	// ─── EVENTSTATS ──────────────────────────────────────────────────────
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("WithBY_TopIP_867", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | EVENTSTATS count AS ip_count BY ip
    | WHERE ip = "183.62.140.253"
    | HEAD 1
    | TABLE ip, ip_count`)
			ipCount := getInt(rows, "ip_count")
			if ipCount != 867 {
				t.Errorf("expected ip_count=867, got %d", ipCount)
			}
		})
		t.Run("DoesNotReduce_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | EVENTSTATS count AS total | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// ─── STREAMSTATS ─────────────────────────────────────────────────────
	t.Run("STREAMSTATS", func(t *testing.T) {
		t.Run("RunningCount_10Rows", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | STREAMSTATS count AS row_num | WHERE row_num <= 10 | TABLE row_num`)
			requireRowCount(t, rows, 10)
		})
		t.Run("CurrentTrue_LastRow2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | STREAMSTATS count AS running_total current=true
    | WHERE running_total = 2000
    | STATS count`)
			requireAggValue(t, rows, "count", 1)
		})
		t.Run("WithBY_FirstOccurrence", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STREAMSTATS count AS ip_running_count BY ip
    | WHERE ip = "183.62.140.253" AND ip_running_count = 1
    | STATS count`)
			requireAggValue(t, rows, "count", 1)
		})
	})

	// ─── TRANSACTION ─────────────────────────────────────────────────────
	t.Run("TRANSACTION", func(t *testing.T) {
		t.Run("ByIP_30Transactions", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | TRANSACTION ip
    | STATS count`)
			requireAggValue(t, rows, "count", 30)
		})
		t.Run("Duration_PositiveMax", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | TRANSACTION ip
    | EVAL duration_sec = duration
    | STATS max(duration_sec) AS max_duration`)
			maxD := getFloat(rows, "max_duration")
			if maxD <= 0 {
				t.Errorf("expected max_duration > 0, got %f", maxD)
			}
		})
	})

	// ─── Complex Pipelines ───────────────────────────────────────────────
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("BruteForceDetection_Has183", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+) port (?<port>\d+)"
    | WHERE isnotnull(src_ip)
    | STATS count AS attempts, dc(target) AS unique_targets BY src_ip
    | WHERE attempts > 50
    | SORT - attempts`)
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
			rows := mustQuery(t, eng, `FROM main
    | EVAL category = CASE(
          match(_raw, "Failed password"), "auth_failure",
          match(_raw, "Accepted"), "auth_success",
          match(_raw, "Invalid user"), "invalid_user",
          match(_raw, "BREAK-IN"), "breakin",
          match(_raw, "Connection closed"), "conn_closed",
          match(_raw, "Received disconnect"), "disconnect",
          match(_raw, "pam_unix"), "pam",
          1=1, "other"
      )
    | STATS count BY category
    | EVENTSTATS sum(count) AS total
    | EVAL percentage = round(count * 100 / total, 1)
    | SORT - count`)
			totalPct := 0.0
			for _, row := range rows {
				totalPct += toFloat(row["percentage"])
			}
			if totalPct < 99 || totalPct > 101 {
				t.Errorf("expected percentages to sum to ~100, got %f", totalPct)
			}
		})
		t.Run("TwoLevelAggregation_AtLeast2ThreatLevels", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STATS count BY ip
    | EVAL threat_level = CASE(
          count > 500, "critical",
          count > 100, "high",
          count > 50, "medium",
          1=1, "low"
      )
    | STATS count BY threat_level
    | SORT - count`)
			if len(rows) < 2 {
				t.Errorf("expected at least 2 threat levels, got %d", len(rows))
			}
		})
	})

	// ─── Wildcard Search ─────────────────────────────────────────────────
	t.Run("WildcardSearch", func(t *testing.T) {
		// Prefix
		t.Run("Prefix_Failed_610", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search Failed* | STATS count`)
			requireAggValue(t, rows, "count", 610)
		})
		// Suffix
		t.Run("Suffix_Preauth_618", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search *preauth | STATS count`)
			requireAggValue(t, rows, "count", 618)
		})
		// Contains
		t.Run("Contains_Password_521", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "*password*" | STATS count`)
			requireAggValue(t, rows, "count", 521)
		})
		// Multi-wildcard
		t.Run("Multi_FailedFromPort_524", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed*from*port" | STATS count`)
			requireAggValue(t, rows, "count", 524)
		})
		// All
		t.Run("WildcardOnly_All_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search * | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
		// Boolean combos
		t.Run("WildcardAND_Root_370", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed*" root | STATS count`)
			requireAggValue(t, rows, "count", 370)
		})
		t.Run("WildcardOR_FailedOrInvalid_836", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Failed*" OR "Invalid*" | STATS count`)
			requireAggValue(t, rows, "count", 836)
		})
		t.Run("WildcardNOT_NotRoot_1257", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search NOT "*root*" | STATS count`)
			requireAggValue(t, rows, "count", 1257)
		})
		// Field comparison
		t.Run("FieldPrefix_IP173_10", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | search ip=173.234.* | STATS count`)
			requireAggValue(t, rows, "count", 10)
		})
		t.Run("FieldExistence_Port_525", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "port (?<port>\d+)" | search port=* | STATS count`)
			total := getInt(rows, "count")
			if total != 525 {
				t.Errorf("expected 525 events with port field, got %d", total)
			}
		})
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// Sort Elimination Correctness: verify that optimizer sort elimination does not
// change query results. These tests execute queries through the full engine
// (parser → optimizer → pipeline) and check result correctness.
// ═══════════════════════════════════════════════════════════════════════════════

func TestRegression_SortElimination(t *testing.T) {
	eng := sshEngine(t)

	t.Run("SortTime_StatsCount", func(t *testing.T) {
		// sort _time | stats count → sort eliminated (dead sort before stats), correct count
		rows := mustQuery(t, eng, `FROM main | SORT _time | STATS count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("SortTime_StatsAvgByHost", func(t *testing.T) {
		// sort _time | stats count by ip → sort eliminated, correct aggregation
		rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | SORT _time | STATS dc(ip) AS unique_ips`)
		requireAggValue(t, rows, "unique_ips", 30)
	})

	t.Run("SortTime_DedupField", func(t *testing.T) {
		// sort _time | dedup ip → sort eliminated (dedup is order-destroying), correct count
		rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | SORT _time | DEDUP ip | STATS count`)
		requireAggValue(t, rows, "count", 30)
	})

	t.Run("SortTime_EvalStatsCount", func(t *testing.T) {
		// sort _time | eval x=1 | stats count → sort eliminated (eval preserves, stats destroys)
		rows := mustQuery(t, eng, `FROM main | SORT _time | EVAL x=1 | STATS count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("SortTime_Head5", func(t *testing.T) {
		// sort _time | head 5 → sort kept (or fused to TopN), correct 5 rows
		rows := mustQuery(t, eng, `FROM main | SORT _time | HEAD 5`)
		requireRowCount(t, rows, 5)
	})

	t.Run("SortTime_StreamstatsCount", func(t *testing.T) {
		// sort _time | streamstats count → sort kept (streamstats depends on order)
		rows := mustQuery(t, eng, `FROM main | SORT _time | STREAMSTATS count AS row_num | WHERE row_num <= 10 | STATS count`)
		requireAggValue(t, rows, "count", 10)
	})

	t.Run("DoubleSortEliminated_StatsCount", func(t *testing.T) {
		// sort a | sort -b | stats count → both sorts eliminated, correct count
		rows := mustQuery(t, eng, `FROM main | SORT _time | SORT -_time | STATS count`)
		requireAggValue(t, rows, "count", 2000)
	})

	t.Run("FirstSortEliminated_SecondKept", func(t *testing.T) {
		// sort _time | stats count | sort -count → first sort eliminated, second kept
		rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | SORT _time | STATS count BY ip | SORT -count | HEAD 1`)
		requireRowCount(t, rows, 1)
		// Top IP by count should be 183.62.140.253 with 867 events.
		topCount := toInt(rows[0]["count"])
		if topCount != 867 {
			t.Errorf("expected top count=867, got %d", topCount)
		}
	})

	t.Run("TerminalSort_CorrectOrder", func(t *testing.T) {
		// Terminal sort count | head 5 → correct ascending order verified
		rows := mustQuery(t, eng, `FROM main | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT count | HEAD 5`)
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

// ═══════════════════════════════════════════════════════════════════════════════
// OpenStack Dataset: OpenStack_2k.log (1999 non-blank lines → 2000 events)
// ═══════════════════════════════════════════════════════════════════════════════

func TestRegression_OpenStack(t *testing.T) {
	eng := openstackEngine(t)

	// ─── Ingestion ───────────────────────────────────────────────────────
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("TotalCount_2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | STATS count`)
			requireAggValue(t, rows, "count", 2000)
		})
	})

	// ─── Search ──────────────────────────────────────────────────────────
	t.Run("Search", func(t *testing.T) {
		t.Run("VMStarted_22", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "VM Started" | STATS count`)
			requireAggValue(t, rows, "count", 22)
		})
		t.Run("WARNING_31", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search WARNING | STATS count`)
			requireAggValue(t, rows, "count", 31)
		})
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "Lifecycle Event" | STATS count`)
			requireAggValue(t, rows, "count", 109)
		})
	})

	// ─── WHERE ───────────────────────────────────────────────────────────
	t.Run("WHERE", func(t *testing.T) {
		t.Run("OR_VMStartedOrStopped_43", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | WHERE match(_raw, "VM Started") OR match(_raw, "VM Stopped") | STATS count`)
			requireAggValue(t, rows, "count", 43)
		})
		t.Run("NumericGTE_Status400_41", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "status: (?<status>\d+)" | WHERE isnotnull(status) | WHERE tonumber(status) >= 400 | STATS count`)
			requireAggValue(t, rows, "count", 41)
		})
	})

	// ─── REX ─────────────────────────────────────────────────────────────
	t.Run("REX", func(t *testing.T) {
		t.Run("LogLevel_INFO1969_WARNING31", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \d+ (?<log_level>\w+)" | STATS count BY log_level | SORT log_level`)
			found := rowsToMap(rows, "log_level", "count")
			if found["INFO"] != 1969 {
				t.Errorf("expected INFO=1969, got %d", found["INFO"])
			}
			if found["WARNING"] != 31 {
				t.Errorf("expected WARNING=31, got %d", found["WARNING"])
			}
		})
		t.Run("HTTPStatus_200_933_404_41", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "status: (?<http_status>\d+)" | WHERE isnotnull(http_status) | STATS count BY http_status | SORT - count`)
			statusCounts := rowsToMap(rows, "http_status", "count")
			expected := map[string]int{"200": 933, "404": 41, "204": 22, "202": 21}
			for status, count := range expected {
				if statusCounts[status] != count {
					t.Errorf("status %s: expected %d, got %d", status, count, statusCounts[status])
				}
			}
		})
		t.Run("HTTPMethod_GET931_POST64_DELETE22", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "\"(?<http_method>GET|POST|PUT|DELETE|PATCH)" | WHERE isnotnull(http_method) | STATS count BY http_method | SORT - count`)
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
			rows := mustQuery(t, eng, `FROM main | REX "\[instance: (?<instance_id>[a-f0-9-]+)\]" | WHERE isnotnull(instance_id) | STATS dc(instance_id) AS unique_instances`)
			requireAggValue(t, rows, "unique_instances", 22)
		})
	})

	// ─── EVAL ────────────────────────────────────────────────────────────
	t.Run("EVAL", func(t *testing.T) {
		t.Run("Arithmetic_SlowRequests_81", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL resp_ms = round(tonumber(resp_time) * 1000, 2)
    | WHERE resp_ms > 300
    | STATS count`)
			requireAggValue(t, rows, "count", 81)
		})
		t.Run("Lower_INFO1969_WARNING31", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "\d+ (?<level>[A-Z]+) " | EVAL level_lower = lower(level) | STATS count BY level_lower`)
			levels := rowsToMap(rows, "level_lower", "count")
			if levels["info"] != 1969 {
				t.Errorf("expected info=1969, got %d", levels["info"])
			}
			if levels["warning"] != 31 {
				t.Errorf("expected warning=31, got %d", levels["warning"])
			}
		})
		t.Run("Substr_Positive", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | EVAL log_source = substr(_raw, 1, 10) | STATS dc(log_source) AS unique_prefixes`)
			prefixes := getInt(rows, "unique_prefixes")
			if prefixes < 3 {
				t.Errorf("expected at least 3 unique prefixes, got %d", prefixes)
			}
		})
	})

	// ─── STATS ───────────────────────────────────────────────────────────
	t.Run("STATS", func(t *testing.T) {
		t.Run("CountBY_Status200_933", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | REX "status: (?<status>\d+)" | WHERE isnotnull(status) | STATS count BY status | SORT - count`)
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
			rows := mustQuery(t, eng, `FROM main | REX "time: (?<resp_time>[0-9.]+)" | WHERE isnotnull(resp_time) | EVAL rt = tonumber(resp_time) | STATS min(rt) AS min_time, max(rt) AS max_time`)
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
			rows := mustQuery(t, eng, `FROM main
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL rt = tonumber(resp_time)
    | STATS perc95(rt) AS p95, perc50(rt) AS median`)
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
			rows := mustQuery(t, eng, `FROM main | REX "len: (?<resp_len>\d+)" | WHERE isnotnull(resp_len) | EVAL resp_len_num = tonumber(resp_len) | STATS sum(resp_len_num) AS total_bytes, avg(resp_len_num) AS avg_bytes`)
			total := getFloat(rows, "total_bytes")
			avg := getFloat(rows, "avg_bytes")
			if total <= 0 || avg <= 0 {
				t.Errorf("expected positive values: total=%f avg=%f", total, avg)
			}
		})
	})

	// ─── BIN ─────────────────────────────────────────────────────────────
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span5m_SumsTo2000", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | BIN _time span=5m AS time_bucket | STATS count BY time_bucket | SORT time_bucket`)
			total := 0
			for _, row := range rows {
				total += toInt(row["count"])
			}
			if total != 2000 {
				t.Errorf("expected 2000 total, got %d", total)
			}
		})
		t.Run("BucketCount_About15", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | BIN _time span=1m AS bucket | STATS dc(bucket) AS num_buckets`)
			buckets := getInt(rows, "num_buckets")
			if buckets < 10 || buckets > 20 {
				t.Errorf("expected ~15 buckets, got %d", buckets)
			}
		})
	})

	// ─── EVENTSTATS ──────────────────────────────────────────────────────
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("GlobalAggregation_1017", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(status)
    | EVENTSTATS count AS total_requests
    | HEAD 1
    | TABLE status, total_requests`)
			totalReq := getInt(rows, "total_requests")
			if totalReq != 1017 {
				t.Errorf("expected total_requests=1017, got %d", totalReq)
			}
		})
		t.Run("Percentage_Status200_About92", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(status)
    | STATS count BY status
    | EVENTSTATS sum(count) AS total
    | EVAL pct = round(count * 100 / total, 2)
    | SORT - pct`)
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

	// ─── Complex Pipelines ───────────────────────────────────────────────
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("InstanceLifecycle_AtLeast3Types", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "\[instance: (?<instance_id>[a-f0-9-]+)\]"
    | WHERE isnotnull(instance_id)
    | EVAL lifecycle_event = CASE(
          match(_raw, "VM Started"), "started",
          match(_raw, "VM Stopped"), "stopped",
          match(_raw, "VM Paused"), "paused",
          match(_raw, "VM Resumed"), "resumed",
          match(_raw, "spawned successfully"), "spawned",
          match(_raw, "Deleting instance"), "deleting",
          match(_raw, "Terminating"), "terminating",
          1=1, "other"
      )
    | STATS count BY lifecycle_event
    | SORT - count`)
			if len(rows) < 3 {
				t.Errorf("expected at least 3 lifecycle event types, got %d", len(rows))
			}
		})
		t.Run("APILatencyAnalysis_AtLeast2Methods", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main
    | REX "\"(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+) HTTP"
    | REX "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(method)
    | EVAL resp_ms = round(tonumber(resp_time) * 1000, 2)
    | STATS count AS requests, avg(resp_ms) AS avg_latency, max(resp_ms) AS max_latency BY method
    | SORT method`)
			if len(rows) < 2 {
				t.Errorf("expected at least 2 methods, got %d", len(rows))
			}
		})
	})

	// ─── Wildcard Search ─────────────────────────────────────────────────
	t.Run("WildcardSearch", func(t *testing.T) {
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "*Lifecycle Event*" | STATS count`)
			requireAggValue(t, rows, "count", 109)
		})
		t.Run("VMEvent_109", func(t *testing.T) {
			rows := mustQuery(t, eng, `FROM main | search "VM*Event" | STATS count`)
			requireAggValue(t, rows, "count", 109)
		})
		t.Run("NovaInstance_923", func(t *testing.T) {
			// Raw data has 923 lines matching nova.*instance (case-insensitive).
			// The E2E test expected 646 through HTTP — that is a bug in the E2E
			// expected value or in the HTTP search path. The engine is correct.
			rows := mustQuery(t, eng, `FROM main | search "nova*instance*" | STATS count`)
			requireAggValue(t, rows, "count", 923)
		})
	})
}
