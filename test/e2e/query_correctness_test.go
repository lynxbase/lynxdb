//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
)

// TestE2E_QueryCorrectness runs PPL query correctness tests against SSH and
// OpenStack log datasets ingested via the typed client.
//
// A single harness (server) is shared across all subtests — data is ingested
// once, then queried many times.
func TestE2E_QueryCorrectness(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")
	h.IngestFile("idx_openstack", "testdata/logs/OpenStack_2k.log")

	// Data ingestion and basic count
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("SSH_TotalCount_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | stats count() as count`), "count", 2000)
		})
		t.Run("OpenStack_TotalCount_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack | stats count() as count`), "count", 2000)
		})
		t.Run("HEAD_10", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh | head 10`), 10)
		})
		t.Run("HEAD_1", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh | head 1`), 1)
		})
		t.Run("HEAD_LargeN_CappedByData", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh | head 5000`), 2000)
		})
	})

	// search command with keywords and phrases
	t.Run("SearchKeywords", func(t *testing.T) {
		t.Run("SimpleKeyword_FailedPassword_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "Failed password" | stats count() as count`), "count", 520)
		})
		t.Run("CaseInsensitive_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "failed password" | stats count() as count`), "count", 520)
		})
		t.Run("PhraseSearch_BREAKIN_85", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "BREAK-IN ATTEMPT" | stats count() as count`), "count", 85)
		})
		t.Run("ImplicitAND_FailedPassword_Root_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "Failed password" root | stats count() as count`), "count", 370)
		})
		t.Run("OR_SessionOpenedClosed_2", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "session opened" OR "session closed" | stats count() as count`), "count", 2)
		})
		t.Run("NOT_FailedPassword_NotRoot_150", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "Failed password" NOT root | stats count() as count`), "count", 150)
		})
		t.Run("Wildcard_InvalidUserFrom_252", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "(?i)Invalid user.*from") | stats count() as count`), "count", 252)
		})
		t.Run("OpenStack_VMStarted_22", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_openstack "VM Started" | stats count() as count`), "count", 22)
		})
		t.Run("OpenStack_WARNING_31", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_openstack WARNING | stats count() as count`), "count", 31)
		})
		t.Run("OpenStack_Lifecycle_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_openstack "Lifecycle Event" | stats count() as count`), "count", 109)
		})
		t.Run("ComplexBoolean_PositiveResult", func(t *testing.T) {
			r := h.MustQuery(`search source=idx_ssh ("Failed password" OR "Invalid user") "173.234.31.186" | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 results for complex boolean, got %d", total)
			}
		})
		t.Run("Nonexistent_Returns0", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh NONEXISTENT_STRING_12345 | stats count() as count`), "count", 0)
		})
	})

	// where command
	t.Run("WHERE", func(t *testing.T) {
		t.Run("StringComparison_PID24200", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "sshd\[(?<pid>\d+)\]" | where pid = "24200" | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events with PID 24200, got %d", total)
			}
		})
		t.Run("IsNotNull_TargetUser_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "Failed password for (?<target_user>\w+)" | where isnotnull(target_user) | stats count() as count`), "count", 520)
		})
		t.Run("IsNull_TargetUser_1480", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "Failed password for (?<target_user>\w+)" | where isnull(target_user) | stats count() as count`), "count", 1480)
		})
		t.Run("Match_IP", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "173\.234\.31\.186") | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events matching IP, got %d", total)
			}
		})
		t.Run("AND_PortAndRoot", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "port (?<port>\d+)" | where isnotnull(port) and regexp_match(_raw, "root") | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0, got %d", total)
			}
		})
		t.Run("OR_VMStartedOrStopped_43", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack | where regexp_match(_raw, "VM Started") or regexp_match(_raw, "VM Stopped") | stats count() as count`), "count", 43)
		})
		t.Run("NumericGTE_Status400_41", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack | rex field=_raw "status: (?<status>\d+)" | where isnotnull(status) | where cast(status as int) >= 400 | stats count() as count`), "count", 41)
		})
		t.Run("RegexStartsWith_Dec10_09_676", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "^Dec 10 09:") | stats count() as count`), "count", 676)
		})
		t.Run("WhereTrue_1Eq1_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where 1 = 1 | stats count() as count`), "count", 2000)
		})
	})

	// rex field extraction
	t.Run("REX", func(t *testing.T) {
		t.Run("ExtractIP_UniqueIPs_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip_addr) | stats dc(ip_addr) as unique_ips`), "unique_ips", 30)
		})
		t.Run("ExtractPID_Positive", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "sshd\[(?<pid>\d+)\]" | where isnotnull(pid) | stats dc(pid) as unique_pids`)
			pids := GetInt(r, "unique_pids")
			if pids <= 0 {
				t.Errorf("expected unique_pids > 0, got %d", pids)
			}
		})
		t.Run("ExtractUsername_TopAdmin_21", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "Invalid user (?<username>\w+) from" | where isnotnull(username) | stats count() as count by username | sort -count | head 3`)
			rows := EventRows(r)
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
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "port (?<port>\d+)" | where isnotnull(port) | stats count() as count`), "count", 525)
		})
		t.Run("ChainedREX_PositiveTargetsAndIPs", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(target) and isnotnull(src_ip) | stats dc(target) as unique_targets, dc(src_ip) as unique_ips`)
			targets := GetInt(r, "unique_targets")
			ips := GetInt(r, "unique_ips")
			if targets <= 0 || ips <= 0 {
				t.Errorf("expected unique_targets > 0 and unique_ips > 0, got targets=%d ips=%d", targets, ips)
			}
		})
		t.Run("ExtractLogLevel_INFO1969_WARNING31", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \d+ (?<log_level>\w+)" | stats count() as count by log_level | sort log_level`)
			rows := EventRows(r)
			found := map[string]int{}
			for _, row := range rows {
				level := fmt.Sprint(row["log_level"])
				count := toInt(row["count"])
				found[level] = count
			}
			if found["INFO"] != 1969 {
				t.Errorf("expected INFO=1969, got %d", found["INFO"])
			}
			if found["WARNING"] != 31 {
				t.Errorf("expected WARNING=31, got %d", found["WARNING"])
			}
		})
		t.Run("ExtractHTTPStatus_200_933_404_41", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "status: (?<http_status>\d+)" | where isnotnull(http_status) | stats count() as count by http_status | sort -count`)
			rows := EventRows(r)
			statusCounts := map[string]int{}
			for _, row := range rows {
				s := fmt.Sprint(row["http_status"])
				statusCounts[s] = toInt(row["count"])
			}
			expected := map[string]int{"200": 933, "404": 41, "204": 22, "202": 21}
			for status, count := range expected {
				if statusCounts[status] != count {
					t.Errorf("status %s: expected %d, got %d", status, count, statusCounts[status])
				}
			}
		})
		t.Run("ExtractHTTPMethod_GET931_POST64_DELETE22", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "(?<http_method>GET|POST|PUT|DELETE|PATCH) /" | where isnotnull(http_method) | stats count() as count by http_method | sort -count`)
			rows := EventRows(r)
			methods := map[string]int{}
			for _, row := range rows {
				m := fmt.Sprint(row["http_method"])
				methods[m] = toInt(row["count"])
			}
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
		t.Run("ExtractInstanceUUID_22", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack | rex field=_raw "\[instance: (?<instance_id>[a-f0-9-]+)\]" | where isnotnull(instance_id) | stats dc(instance_id) as unique_instances`), "unique_instances", 22)
		})
		t.Run("ExtractResponseTime_PositiveAvg", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "time: (?<resp_time>[0-9.]+)" | where isnotnull(resp_time) | eval resp_ms = cast(resp_time as double) * 1000 | stats count() as count, avg(resp_ms) as avg_ms`)
			total := GetInt(r, "count")
			avgMs := GetFloat(r, "avg_ms")
			if total <= 0 {
				t.Errorf("expected total > 0, got %d", total)
			}
			if avgMs <= 0 {
				t.Errorf("expected avg_ms > 0, got %f", avgMs)
			}
		})
		t.Run("NoMatch_Returns0", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "NONEXISTENT_PATTERN_(?<captured>\w+)" | stats count(captured) as matched`), "matched", 0)
		})
		t.Run("FieldParam_NovaSubsystems", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "nova\.(?<nova_subsystem>[\w.]+)" | where isnotnull(nova_subsystem) | stats dc(nova_subsystem) as unique_subsystems`)
			subs := GetInt(r, "unique_subsystems")
			if subs < 3 {
				t.Errorf("expected at least 3 unique subsystems, got %d", subs)
			}
		})
	})

	// eval expressions
	t.Run("EVAL", func(t *testing.T) {
		t.Run("StringAssignment", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | eval source_type = "ssh_log" | head 1 | fields source_type`)
			rows := EventRows(r)
			if len(rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(rows))
			}
			st := fmt.Sprint(rows[0]["source_type"])
			if st != "ssh_log" {
				t.Errorf("expected source_type=ssh_log, got %s", st)
			}
		})
		t.Run("IF_AllPublicIPs", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | eval ip_class = if(regexp_match(ip_addr, "^10\."), "private", "public") | where isnotnull(ip_addr) | stats count() as count by ip_class`)
			rows := EventRows(r)
			for _, row := range rows {
				cls := fmt.Sprint(row["ip_class"])
				if cls == "private" {
					t.Error("unexpected private IP in SSH logs")
				}
			}
		})
		t.Run("CASE_FailedAuth520", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
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
    | sort -count`)
			rows := EventRows(r)
			types := map[string]int{}
			for _, row := range rows {
				et := fmt.Sprint(row["event_type"])
				types[et] = toInt(row["count"])
			}
			if types["failed_auth"] != 520 {
				t.Errorf("expected failed_auth=520, got %d", types["failed_auth"])
			}
		})
		t.Run("Arithmetic_SlowRequests_81", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval resp_ms = round(cast(resp_time as double) * 1000, 2)
    | where resp_ms > 300
    | stats count() as count`), "count", 81)
		})
		t.Run("Length_PositiveAvgMaxMin", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | eval raw_len = length(_raw) | stats avg(raw_len) as avg_length, max(raw_len) as max_length, min(raw_len) as min_length`)
			avg := GetFloat(r, "avg_length")
			maxLen := GetFloat(r, "max_length")
			minLen := GetFloat(r, "min_length")
			if avg <= 0 || maxLen <= 0 || minLen <= 0 {
				t.Errorf("expected positive lengths: avg=%f max=%f min=%f", avg, maxLen, minLen)
			}
			if maxLen < avg || minLen > avg {
				t.Errorf("inconsistent: min=%f avg=%f max=%f", minLen, avg, maxLen)
			}
		})
		t.Run("Coalesce_NA_1480", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "Failed password for (?<target>\w+)"
    | eval user_or_unknown = coalesce(target, "N/A")
    | stats count() as count by user_or_unknown
    | where user_or_unknown = "N/A"`), "count", 1480)
		})
		t.Run("Lower_INFO1969_WARNING31", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "\d+ (?<level>[A-Z]+) " | eval level_lower = lower(level) | stats count() as count by level_lower`)
			rows := EventRows(r)
			levels := map[string]int{}
			for _, row := range rows {
				l := fmt.Sprint(row["level_lower"])
				levels[l] = toInt(row["count"])
			}
			if levels["info"] != 1969 {
				t.Errorf("expected info=1969, got %d", levels["info"])
			}
			if levels["warning"] != 31 {
				t.Errorf("expected warning=31, got %d", levels["warning"])
			}
		})
		t.Run("MultipleAssignments_WithPort525", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | rex field=_raw "port (?<port>\d+)"
    | eval has_ip = if(isnotnull(ip), 1, 0),
           has_port = if(isnotnull(port), 1, 0),
           connection_info = has_ip + has_port
    | stats sum(has_ip) as with_ip, sum(has_port) as with_port`)
			withPort := GetInt(r, "with_port")
			if withPort != 525 {
				t.Errorf("expected with_port=525, got %d", withPort)
			}
		})
		t.Run("NullPropagation_1475", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "port (?<port>\d+)"
    | eval port_plus_one = cast(port as int) + 1
    | where isnull(port_plus_one)
    | stats count() as count`), "count", 1475)
		})
		t.Run("ToString_404_41", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(status)
    | eval status_str = cast(cast(status as int) as string)
    | where status_str = "404"
    | stats count() as count`), "count", 41)
		})
		t.Run("Substring_Positive", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | eval log_source = substring(_raw, 1, 10) | stats dc(log_source) as unique_prefixes`)
			prefixes := GetInt(r, "unique_prefixes")
			if prefixes < 3 {
				t.Errorf("expected at least 3 unique prefixes, got %d", prefixes)
			}
		})
		t.Run("Replace_PositiveSubnets", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | eval ip_masked = replace(ip, "\.\d+$", ".xxx")
    | stats dc(ip_masked) as unique_subnets`)
			subnets := GetInt(r, "unique_subnets")
			if subnets <= 0 {
				t.Errorf("expected unique_subnets > 0, got %d", subnets)
			}
		})
		t.Run("SplitArrayLength_5Parts", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "\[req-(?<req_id>[a-f0-9-]+)"
    | where isnotnull(req_id)
    | eval req_parts = split(req_id, "-")
    | eval part_count = array_length(req_parts)
    | head 1
    | fields req_id, part_count`)
			rows := EventRows(r)
			if len(rows) > 0 {
				pc := toInt(rows[0]["part_count"])
				if pc != 5 {
					t.Errorf("expected part_count=5, got %d", pc)
				}
			}
		})
		t.Run("NestedFunctions_AvgLastOctet", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | eval ip_last_octet = cast(replace(ip, ".*\.", "") as int)
    | stats avg(ip_last_octet) as avg_last_octet`)
			avg := GetFloat(r, "avg_last_octet")
			if avg <= 0 || avg > 255 {
				t.Errorf("expected avg_last_octet in (0, 255], got %f", avg)
			}
		})
	})

	// stats aggregation
	t.Run("STATS", func(t *testing.T) {
		t.Run("Count_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | stats count() as count`), "count", 2000)
		})
		t.Run("CountBY_Status200_933", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "status: (?<status>\d+)" | where isnotnull(status) | stats count() as count by status | sort -count`)
			rows := EventRows(r)
			if len(rows) < 4 {
				t.Errorf("expected at least 4 rows, got %d", len(rows))
			}
			statusCounts := map[string]int{}
			for _, row := range rows {
				s := fmt.Sprint(row["status"])
				statusCounts[s] = toInt(row["count"])
			}
			if statusCounts["200"] != 933 {
				t.Errorf("status 200: expected 933, got %d", statusCounts["200"])
			}
			if statusCounts["404"] != 41 {
				t.Errorf("status 404: expected 41, got %d", statusCounts["404"])
			}
		})
		t.Run("DC_UniqueIPs_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | stats dc(ip) as unique_ips`), "unique_ips", 30)
		})
		t.Run("Values_ContainsMethods", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "(?<method>GET|POST|PUT|DELETE) /" | where isnotnull(method) | stats values(method) as methods`)
			methods := GetStr(r, "methods")
			for _, m := range []string{"GET", "POST", "DELETE"} {
				if !strings.Contains(methods, m) {
					t.Errorf("expected methods to contain %s, got: %s", m, methods)
				}
			}
		})
		t.Run("Sum_PositiveTotalBytes", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "len: (?<resp_len>\d+)" | where isnotnull(resp_len) | eval resp_len_num = cast(resp_len as double) | stats sum(resp_len_num) as total_bytes, avg(resp_len_num) as avg_bytes`)
			total := GetFloat(r, "total_bytes")
			avg := GetFloat(r, "avg_bytes")
			if total <= 0 || avg <= 0 {
				t.Errorf("expected positive values: total=%f avg=%f", total, avg)
			}
		})
		t.Run("MinMax_ResponseTime", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "time: (?<resp_time>[0-9.]+)" | where isnotnull(resp_time) | eval rt = cast(resp_time as double) | stats min(rt) as min_time, max(rt) as max_time`)
			minT := GetFloat(r, "min_time")
			maxT := GetFloat(r, "max_time")
			if minT >= maxT {
				t.Errorf("expected min < max: min=%f max=%f", minT, maxT)
			}
			if minT < 0 {
				t.Errorf("expected min >= 0, got %f", minT)
			}
		})
		t.Run("MultipleBY_MethodStatus", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "(?<method>GET|POST|DELETE) /"
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(method) and isnotnull(status)
    | stats count() as count by method, status
    | sort method, status`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 method x status combos, got %d", EventCount(r))
			}
		})
		t.Run("NestedEval_RequestsPerIP", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | stats count() as requests, dc(ip) as unique_ips
    | eval requests_per_ip = round(requests / unique_ips, 2)`)
			rpi := GetFloat(r, "requests_per_ip")
			if rpi <= 0 {
				t.Errorf("expected requests_per_ip > 0, got %f", rpi)
			}
		})
		t.Run("Percentile_P95GEMedian", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval rt = cast(resp_time as double)
    | stats p95(rt) as p95, p50(rt) as median`)
			p95 := GetFloat(r, "p95")
			median := GetFloat(r, "median")
			if p95 < median {
				t.Errorf("expected p95 >= median: p95=%f median=%f", p95, median)
			}
			if p95 <= 0 || median <= 0 {
				t.Errorf("expected positive: p95=%f median=%f", p95, median)
			}
		})
		t.Run("EarliestLatest_NonEmpty", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | stats earliest(_time) as first_event, latest(_time) as last_event`)
			first := GetStr(r, "first_event")
			last := GetStr(r, "last_event")
			if first == "" || last == "" {
				t.Errorf("expected non-empty timestamps: first=%q last=%q", first, last)
			}
		})
		t.Run("MaxFromSingleIP_867", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | stats count() as count by ip
    | stats max(count) as max_from_single_ip, avg(count) as avg_per_ip`), "max_from_single_ip", 867)
		})
		t.Run("TopUsernames_Admin21", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "Invalid user (?<username>\w+)" | where isnotnull(username) | stats count() as count by username | sort -count | head 5`)
			rows := EventRows(r)
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
	})

	// bin time bucketing
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span1h_SumsTo2000", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | bin _time span=1h as hour_bucket | stats count() as count by hour_bucket | sort hour_bucket`)
			rows := EventRows(r)
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
		t.Run("Span5m_SumsTo2000", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | bin _time span=5m as time_bucket | stats count() as count by time_bucket | sort time_bucket`)
			rows := EventRows(r)
			total := 0
			for _, row := range rows {
				total += toInt(row["count"])
			}
			if total != 2000 {
				t.Errorf("expected 2000 total, got %d", total)
			}
		})
		t.Run("Span1m_Top5", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh | bin _time span=1m as minute_bucket | stats count() as count by minute_bucket | sort -count | head 5`), 5)
		})
		t.Run("BucketCount_About15", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | bin _time span=1m as bucket | stats dc(bucket) as num_buckets`)
			buckets := GetInt(r, "num_buckets")
			if buckets < 10 || buckets > 20 {
				t.Errorf("expected ~15 buckets, got %d", buckets)
			}
		})
		t.Run("BINWithStats_AvgResponse", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval rt = cast(resp_time as double)
    | bin _time span=5m as bucket
    | stats avg(rt) as avg_response, count() as requests by bucket
    | sort bucket`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 time buckets, got %d", EventCount(r))
			}
		})
	})

	// sort
	t.Run("SORT", func(t *testing.T) {
		t.Run("Ascending_Order", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort count | head 3`)
			rows := EventRows(r)
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
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort -count | head 1`)
			rows := EventRows(r)
			if len(rows) == 0 {
				t.Fatal("expected at least 1 row, got 0")
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
		t.Run("MultipleFields", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "(?<method>GET|POST|DELETE) /"
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(method) and isnotnull(status)
    | stats count() as count by method, status
    | sort method, -count`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 rows, got %d", EventCount(r))
			}
		})
		t.Run("PreservesAllRows_30IPs", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | stats count() as count by ip | sort count | stats count() as count`), "count", 30)
		})
		t.Run("StringField_Alphabetical", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "Invalid user (?<username>\w+)" | where isnotnull(username) | stats count() as count by username | sort username | head 3`)
			rows := EventRows(r)
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

	// rename and fields
	t.Run("RenameFields", func(t *testing.T) {
		t.Run("Rename_IP_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | rename ip as source_ip
    | where isnotnull(source_ip)
    | stats dc(source_ip) as unique_sources`), "unique_sources", 30)
		})
		t.Run("Fields_Raw_3Rows", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | head 3 | fields _raw`)
			rows := EventRows(r)
			if len(rows) != 3 {
				t.Errorf("expected 3 rows, got %d", len(rows))
			}
			for i, row := range rows {
				if _, ok := row["_raw"]; !ok {
					t.Errorf("row %d missing _raw field", i)
				}
			}
		})
		t.Run("RenameThenStats_200_933", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "status: (?<status>\d+)"
    | rename status as http_code
    | where isnotnull(http_code)
    | stats count() as count by http_code`)
			rows := EventRows(r)
			codes := map[string]int{}
			for _, row := range rows {
				code := fmt.Sprint(row["http_code"])
				codes[code] = toInt(row["count"])
			}
			if codes["200"] != 933 {
				t.Errorf("expected 200=933, got %d", codes["200"])
			}
		})
		t.Run("FieldsMultiple", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "sshd\[(?<pid>\d+)\]"
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | head 5
    | fields _time, pid, ip`), 5)
		})
	})

	// dedup
	t.Run("DEDUP", func(t *testing.T) {
		t.Run("DedupField_UniqueIPs_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | dedup ip
    | stats count() as count`), "count", 30)
		})
		t.Run("KeepsFirst_5Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | dedup ip
    | sort ip
    | head 5`), 5)
		})
		t.Run("MultipleFields_AtLeast3", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "(?<method>GET|POST|DELETE) /"
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(method) and isnotnull(status)
    | dedup method, status
    | stats count() as count`)
			combos := GetInt(r, "count")
			if combos < 3 {
				t.Errorf("expected at least 3 unique combos, got %d", combos)
			}
		})
		t.Run("WithLimit_NoneOver3", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | dedup 3 ip
    | stats count() as count by ip
    | where count > 3
    | stats count() as count`), "count", 0)
		})
	})

	// eventstats
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("GlobalAggregation_1017", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(status)
    | eventstats count() as total_requests
    | head 1
    | fields status, total_requests`)
			totalReq := GetInt(r, "total_requests")
			if totalReq != 1017 {
				t.Errorf("expected total_requests=1017, got %d", totalReq)
			}
		})
		t.Run("WithBY_TopIP_867", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | eventstats count() as ip_count by ip
    | where ip = "183.62.140.253"
    | head 1
    | fields ip, ip_count`)
			ipCount := GetInt(r, "ip_count")
			if ipCount != 867 {
				t.Errorf("expected ip_count=867, got %d", ipCount)
			}
		})
		t.Run("Percentage_Status200_About92", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(status)
    | stats count() as count by status
    | eventstats sum(count) as total
    | eval pct = round(count * 100.0 / total, 2)
    | sort -pct`)
			rows := EventRows(r)
			if len(rows) == 0 {
				t.Fatal("expected at least 1 row, got 0")
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
		t.Run("DoesNotReduce_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | eventstats count() as total | stats count() as count`), "count", 2000)
		})
	})

	// streamstats
	t.Run("STREAMSTATS", func(t *testing.T) {
		t.Run("RunningCount_10Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_ssh | streamstats count() as row_num | where row_num <= 10 | fields row_num`), 10)
		})
		t.Run("Window_10Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`source=idx_openstack
    | rex field=_raw "time: (?<resp_time>[0-9.]+)"
    | where isnotnull(resp_time)
    | eval rt = cast(resp_time as double)
    | streamstats window=5 avg(rt) as rolling_avg
    | head 10
    | fields rt, rolling_avg`), 10)
		})
		t.Run("CurrentTrue_LastRow2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | streamstats current=true count() as running_total
    | where running_total = 2000
    | stats count() as count`), "count", 1)
		})
		t.Run("WithBY_FirstOccurrence", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | streamstats count() as ip_running_count by ip
    | where ip = "183.62.140.253" and ip_running_count = 1
    | stats count() as count`), "count", 1)
		})
	})

	// Multi-command pipelines
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("BruteForceDetection_Has183", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+) port (?<port>\d+)"
    | where isnotnull(src_ip)
    | stats count() as attempts, dc(target) as unique_targets by src_ip
    | where attempts > 50
    | sort -attempts`)
			rows := EventRows(r)
			if len(rows) < 1 {
				t.Errorf("expected at least 1 brute force IP, got %d", len(rows))
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
		t.Run("APILatencyAnalysis_AtLeast2Methods", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+) HTTP"
    | rex field=_raw "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)"
    | where isnotnull(method)
    | eval resp_ms = round(cast(resp_time as double) * 1000, 2)
    | stats count() as requests, avg(resp_ms) as avg_latency, max(resp_ms) as max_latency by method
    | sort method`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 methods, got %d", EventCount(r))
			}
		})
		t.Run("EventClassification_PercentagesSumTo100", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
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
    | eval percentage = round(count * 100.0 / total, 1)
    | sort -count`)
			rows := EventRows(r)
			totalPct := 0.0
			for _, row := range rows {
				totalPct += toFloat(row["percentage"])
			}
			if totalPct < 99 || totalPct > 101 {
				t.Errorf("expected percentages to sum to ~100, got %f", totalPct)
			}
		})
		t.Run("InstanceLifecycle_AtLeast3Types", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
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
    | sort -count`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 lifecycle event types, got %d", EventCount(r))
			}
		})
		t.Run("TwoLevelAggregation_AtLeast2ThreatLevels", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
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
    | sort -count`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 threat levels, got %d", EventCount(r))
			}
		})
		t.Run("TimeBucketedRate_AtLeast2Windows", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | bin _time span=10m as time_window
    | stats count() as events, dc(ip) as unique_ips by time_window
    | eval events_per_ip = round(events / unique_ips, 2)
    | sort time_window`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 time windows, got %d", EventCount(r))
			}
		})
		t.Run("RequestAnalysisChain_AtLeast2Rows", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack
    | rex field=_raw "(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+)"
    | rex field=_raw "status: (?<status>\d+)"
    | where isnotnull(method) and isnotnull(url_path)
    | eval endpoint = case(
          regexp_match(url_path, "servers/detail"), "servers_detail",
          regexp_match(url_path, "os-server-external-events"), "external_events",
          regexp_match(url_path, "metadata"), "metadata"
          else "other"
      )
    | stats count() as count by endpoint, method
    | sort -count`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 rows, got %d", EventCount(r))
			}
		})
		t.Run("SubnetAnalysis_AtLeast3Subnets", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh
    | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | where isnotnull(ip)
    | rex field=ip "(?<subnet>\d+\.\d+\.\d+)\."
    | stats count() as requests, dc(ip) as unique_hosts by subnet
    | sort -requests
    | head 5`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 subnets, got %d", EventCount(r))
			}
		})
	})

	// Wildcard search: search wildcards match whole tokens; patterns that
	// span tokens use regexp_match.
	t.Run("WildcardSearch", func(t *testing.T) {
		// Prefix wildcards
		t.Run("Prefix_Failed_610", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh Failed* | stats count() as count`), "count", 610)
		})
		t.Run("Prefix_ReceivedDisconnect_468", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh Received disconnect* | stats count() as count`), "count", 468)
		})
		// Suffix wildcards
		t.Run("Suffix_Preauth_618", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh *preauth | stats count() as count`), "count", 618)
		})
		t.Run("Suffix_AuthFailure_496", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh authentication *failure | stats count() as count`), "count", 496)
		})
		// Contains wildcards
		t.Run("Contains_Password_521", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh *password* | stats count() as count`), "count", 521)
		})
		t.Run("Contains_Preauth_618", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh *preauth* | stats count() as count`), "count", 618)
		})
		t.Run("Contains_IP_10", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh "173.234.31.186" | stats count() as count`), "count", 10)
		})
		// Patterns spanning several tokens
		t.Run("Multi_FailedFromPort_524", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "Failed.*from.*port") | stats count() as count`), "count", 524)
		})
		t.Run("Multi_FailedInvalidFrom_139", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "Failed.*invalid.*from") | stats count() as count`), "count", 139)
		})
		t.Run("Multi_PasswordRootPortSsh2_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | where regexp_match(_raw, "password.*root.*port.*ssh2") | stats count() as count`), "count", 370)
		})
		// Wildcard only: match all
		t.Run("WildcardOnly_All_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh * | stats count() as count`), "count", 2000)
		})
		// Wildcards with boolean operators
		t.Run("WildcardAND_Root_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh Failed* root | stats count() as count`), "count", 370)
		})
		t.Run("WildcardOR_FailedOrInvalid_836", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh Failed* OR Invalid* | stats count() as count`), "count", 836)
		})
		t.Run("WildcardNOT_NotRoot_1257", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_ssh NOT *root* | stats count() as count`), "count", 1257)
		})
		// Field value wildcards
		t.Run("FieldPrefix_IP173_10", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | where like(ip, "173.234.%") | stats count() as count`), "count", 10)
		})
		t.Run("FieldSuffix_IP253_Positive", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "(?<ip>\d+\.\d+\.\d+\.\d+)" | where isnotnull(ip) | where like(ip, "%253") | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events with IP ending in 253, got %d", total)
			}
		})
		t.Run("FieldExistence_Port_525", func(t *testing.T) {
			r := h.MustQuery(`source=idx_ssh | rex field=_raw "port (?<port>\d+)" | where isnotnull(port) | stats count() as count`)
			total := GetInt(r, "count")
			if total != 525 {
				t.Errorf("expected 525 events with port field, got %d", total)
			}
		})
		t.Run("LikeAlternatives_GET_POST_GT990", func(t *testing.T) {
			r := h.MustQuery(`source=idx_openstack | rex field=_raw "(?<method>GET|POST|PUT|DELETE|PATCH) /" | where isnotnull(method) | where like(method, "G%") or like(method, "P%") | stats count() as count`)
			total := GetInt(r, "count")
			if total <= 990 {
				t.Errorf("expected > 990 events matching GET/POST/PUT/PATCH, got %d", total)
			}
		})
		// OpenStack
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`search source=idx_openstack "Lifecycle Event" | stats count() as count`), "count", 109)
		})
		t.Run("VMEvent_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`source=idx_openstack | where regexp_match(_raw, "VM.*Event") | stats count() as count`), "count", 109)
		})
		t.Run("NovaInstance_923", func(t *testing.T) {
			// 923 lines of the raw data match nova.*instance case-insensitively.
			requireAggValue(t, h.MustQuery(`source=idx_openstack | where regexp_match(_raw, "(?i)nova.*instance") | stats count() as count`), "count", 923)
		})
	})
}
