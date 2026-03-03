//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
)

// TestE2E_QueryCorrectness runs SPL2 query correctness tests against SSH and
// OpenStack log datasets ingested via the typed client. This is the bulk
// migration of the original e2e Categories 1-14 and 16.
//
// A single harness (server) is shared across all subtests — data is ingested
// once, then queried many times.
func TestE2E_QueryCorrectness(t *testing.T) {
	h := NewHarness(t)
	h.IngestFile("idx_ssh", "testdata/logs/OpenSSH_2k.log")
	h.IngestFile("idx_openstack", "testdata/logs/OpenStack_2k.log")

	// ─── Category 1: Data Ingestion & Basic Count ───────────────────────
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("SSH_TotalCount_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | STATS count`), "count", 2000)
		})
		t.Run("OpenStack_TotalCount_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | STATS count`), "count", 2000)
		})
		t.Run("HEAD_10", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh | HEAD 10`), 10)
		})
		t.Run("HEAD_1", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh | HEAD 1`), 1)
		})
		t.Run("HEAD_LargeN_CappedByData", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh | HEAD 5000`), 2000)
		})
	})

	// ─── Category 2: search Command with Keywords ───────────────────────
	t.Run("SearchKeywords", func(t *testing.T) {
		t.Run("SimpleKeyword_FailedPassword_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed password" | STATS count`), "count", 520)
		})
		t.Run("CaseInsensitive_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "failed password" | STATS count`), "count", 520)
		})
		t.Run("PhraseSearch_BREAKIN_85", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "BREAK-IN ATTEMPT" | STATS count`), "count", 85)
		})
		t.Run("ImplicitAND_FailedPassword_Root_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed password" root | STATS count`), "count", 370)
		})
		t.Run("OR_SessionOpenedClosed_2", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "session opened" OR "session closed" | STATS count`), "count", 2)
		})
		t.Run("NOT_FailedPassword_NotRoot_150", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed password" NOT root | STATS count`), "count", 150)
		})
		t.Run("Wildcard_InvalidUserFrom_252", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Invalid user*from" | STATS count`), "count", 252)
		})
		t.Run("OpenStack_VMStarted_22", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search "VM Started" | STATS count`), "count", 22)
		})
		t.Run("OpenStack_WARNING_31", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search WARNING | STATS count`), "count", 31)
		})
		t.Run("OpenStack_Lifecycle_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search "Lifecycle Event" | STATS count`), "count", 109)
		})
		t.Run("ComplexBoolean_PositiveResult", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | search ("Failed password" OR "Invalid user") "173.234.31.186" | STATS count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 results for complex boolean, got %d", total)
			}
		})
		t.Run("Nonexistent_Returns0", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "NONEXISTENT_STRING_12345" | STATS count`), "count", 0)
		})
	})

	// ─── Category 3: WHERE Command ──────────────────────────────────────
	t.Run("WHERE", func(t *testing.T) {
		t.Run("StringComparison_PID24200", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "sshd\[(?<pid>\d+)\]" | WHERE pid="24200" | STATS count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events with PID 24200, got %d", total)
			}
		})
		t.Run("IsNotNull_TargetUser_520", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "Failed password for (?<target_user>\w+)" | WHERE isnotnull(target_user) | STATS count`), "count", 520)
		})
		t.Run("IsNull_TargetUser_1480", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "Failed password for (?<target_user>\w+)" | WHERE isnull(target_user) | STATS count`), "count", 1480)
		})
		t.Run("Match_IP", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | WHERE match(_raw, "173\.234\.31\.186") | STATS count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events matching IP, got %d", total)
			}
		})
		t.Run("AND_PortAndRoot", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "port (?<port>\d+)" | WHERE isnotnull(port) AND match(_raw, "root") | STATS count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0, got %d", total)
			}
		})
		t.Run("OR_VMStartedOrStopped_43", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | WHERE match(_raw, "VM Started") OR match(_raw, "VM Stopped") | STATS count`), "count", 43)
		})
		t.Run("NumericGTE_Status400_41", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | REX "status: (?<status>\d+)" | WHERE isnotnull(status) | WHERE tonumber(status) >= 400 | STATS count`), "count", 41)
		})
		t.Run("RegexStartsWith_Dec10_09_676", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | WHERE match(_raw, "^Dec 10 09:") | STATS count`), "count", 676)
		})
		t.Run("WhereTrue_1Eq1_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | WHERE 1=1 | STATS count`), "count", 2000)
		})
	})

	// ─── Category 4: REX (Regular Expression Extraction) ────────────────
	t.Run("REX", func(t *testing.T) {
		t.Run("ExtractIP_UniqueIPs_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip_addr) | STATS dc(ip_addr) AS unique_ips`), "unique_ips", 30)
		})
		t.Run("ExtractPID_Positive", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "sshd\[(?<pid>\d+)\]" | WHERE isnotnull(pid) | STATS dc(pid) AS unique_pids`)
			pids := GetInt(r, "unique_pids")
			if pids <= 0 {
				t.Errorf("expected unique_pids > 0, got %d", pids)
			}
		})
		t.Run("ExtractUsername_TopAdmin_21", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "Invalid user (?<username>\w+) from" | WHERE isnotnull(username) | STATS count BY username | SORT - count | HEAD 3`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "port (?<port>\d+)" | WHERE isnotnull(port) | STATS count`), "count", 525)
		})
		t.Run("ChainedREX_PositiveTargetsAndIPs", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(target) AND isnotnull(src_ip) | STATS dc(target) AS unique_targets, dc(src_ip) AS unique_ips`)
			targets := GetInt(r, "unique_targets")
			ips := GetInt(r, "unique_ips")
			if targets <= 0 || ips <= 0 {
				t.Errorf("expected unique_targets > 0 and unique_ips > 0, got targets=%d ips=%d", targets, ips)
			}
		})
		t.Run("ExtractLogLevel_INFO1969_WARNING31", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ \d+ (?<log_level>\w+)" | STATS count BY log_level | SORT log_level`)
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
			r := h.MustQuery(`FROM idx_openstack | REX "status: (?<http_status>\d+)" | WHERE isnotnull(http_status) | STATS count BY http_status | SORT - count`)
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
			r := h.MustQuery(`FROM idx_openstack | REX "\"(?<http_method>GET|POST|PUT|DELETE|PATCH)" | WHERE isnotnull(http_method) | STATS count BY http_method | SORT - count`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | REX "\[instance: (?<instance_id>[a-f0-9-]+)\]" | WHERE isnotnull(instance_id) | STATS dc(instance_id) AS unique_instances`), "unique_instances", 22)
		})
		t.Run("ExtractResponseTime_PositiveAvg", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "time: (?<resp_time>[0-9.]+)" | WHERE isnotnull(resp_time) | EVAL resp_ms = tonumber(resp_time) * 1000 | STATS count, avg(resp_ms) AS avg_ms`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "NONEXISTENT_PATTERN_(?<captured>\w+)" | STATS count(captured) AS matched`), "matched", 0)
		})
		t.Run("FieldParam_NovaSubsystems", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX field=_raw "nova\.(?<nova_subsystem>[\w.]+)" | WHERE isnotnull(nova_subsystem) | STATS dc(nova_subsystem) AS unique_subsystems`)
			subs := GetInt(r, "unique_subsystems")
			if subs < 3 {
				t.Errorf("expected at least 3 unique subsystems, got %d", subs)
			}
		})
	})

	// ─── Category 5: EVAL (Expression Evaluation) ───────────────────────
	t.Run("EVAL", func(t *testing.T) {
		t.Run("StringAssignment", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | EVAL source_type = "ssh_log" | HEAD 1 | TABLE source_type`)
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
			r := h.MustQuery(`FROM idx_ssh | REX "(?<ip_addr>\d+\.\d+\.\d+\.\d+)" | EVAL ip_class = IF(match(ip_addr, "^10\."), "private", "public") | WHERE isnotnull(ip_addr) | STATS count BY ip_class`)
			rows := EventRows(r)
			for _, row := range rows {
				cls := fmt.Sprint(row["ip_class"])
				if cls == "private" {
					t.Error("unexpected private IP in SSH logs")
				}
			}
		})
		t.Run("CASE_FailedAuth520", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
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
			requireAggValue(t, h.MustQuery(`FROM idx_openstack
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL resp_ms = round(tonumber(resp_time) * 1000, 2)
    | WHERE resp_ms > 300
    | STATS count`), "count", 81)
		})
		t.Run("Len_PositiveAvgMaxMin", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | EVAL raw_len = len(_raw) | STATS avg(raw_len) AS avg_length, max(raw_len) AS max_length, min(raw_len) AS min_length`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "Failed password for (?<target>\w+)"
    | EVAL user_or_unknown = coalesce(target, "N/A")
    | STATS count BY user_or_unknown
    | WHERE user_or_unknown = "N/A"`), "count", 1480)
		})
		t.Run("Lower_INFO1969_WARNING31", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "\d+ (?<level>[A-Z]+) " | EVAL level_lower = lower(level) | STATS count BY level_lower`)
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
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | REX "port (?<port>\d+)"
    | EVAL has_ip = IF(isnotnull(ip), 1, 0),
           has_port = IF(isnotnull(port), 1, 0),
           connection_info = has_ip + has_port
    | STATS sum(has_ip) AS with_ip, sum(has_port) AS with_port`)
			withPort := GetInt(r, "with_port")
			if withPort != 525 {
				t.Errorf("expected with_port=525, got %d", withPort)
			}
		})
		t.Run("NullPropagation_1475", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "port (?<port>\d+)"
    | EVAL port_plus_one = tonumber(port) + 1
    | WHERE isnull(port_plus_one)
    | STATS count`), "count", 1475)
		})
		t.Run("ToString_404_41", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(status)
    | EVAL status_str = tostring(tonumber(status))
    | WHERE status_str = "404"
    | STATS count`), "count", 41)
		})
		t.Run("Substr_Positive", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | EVAL log_source = substr(_raw, 1, 10) | STATS dc(log_source) AS unique_prefixes`)
			prefixes := GetInt(r, "unique_prefixes")
			if prefixes < 3 {
				t.Errorf("expected at least 3 unique prefixes, got %d", prefixes)
			}
		})
		t.Run("Replace_PositiveSubnets", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | EVAL ip_masked = replace(ip, "\.\d+$", ".xxx")
    | STATS dc(ip_masked) AS unique_subnets`)
			subnets := GetInt(r, "unique_subnets")
			if subnets <= 0 {
				t.Errorf("expected unique_subnets > 0, got %d", subnets)
			}
		})
		t.Run("SplitMvcount_5Parts", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "\[req-(?<req_id>[a-f0-9-]+)"
    | WHERE isnotnull(req_id)
    | EVAL req_parts = split(req_id, "-")
    | EVAL part_count = mvcount(req_parts)
    | HEAD 1
    | TABLE req_id, part_count`)
			rows := EventRows(r)
			if len(rows) > 0 {
				pc := toInt(rows[0]["part_count"])
				if pc != 5 {
					t.Errorf("expected part_count=5, got %d", pc)
				}
			}
		})
		t.Run("NestedFunctions_AvgLastOctet", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | EVAL ip_last_octet = tonumber(replace(ip, ".*\.", ""))
    | STATS avg(ip_last_octet) AS avg_last_octet`)
			avg := GetFloat(r, "avg_last_octet")
			if avg <= 0 || avg > 255 {
				t.Errorf("expected avg_last_octet in (0, 255], got %f", avg)
			}
		})
	})

	// ─── Category 6: STATS (Aggregation) ────────────────────────────────
	t.Run("STATS", func(t *testing.T) {
		t.Run("Count_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | STATS count`), "count", 2000)
		})
		t.Run("CountBY_Status200_933", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "status: (?<status>\d+)" | WHERE isnotnull(status) | STATS count BY status | SORT - count`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | STATS dc(ip) AS unique_ips`), "unique_ips", 30)
		})
		t.Run("Values_ContainsMethods", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "\"(?<method>GET|POST|PUT|DELETE)" | WHERE isnotnull(method) | STATS values(method) AS methods`)
			methods := GetStr(r, "methods")
			for _, m := range []string{"GET", "POST", "DELETE"} {
				if !strings.Contains(methods, m) {
					t.Errorf("expected methods to contain %s, got: %s", m, methods)
				}
			}
		})
		t.Run("Sum_PositiveTotalBytes", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "len: (?<resp_len>\d+)" | WHERE isnotnull(resp_len) | EVAL resp_len_num = tonumber(resp_len) | STATS sum(resp_len_num) AS total_bytes, avg(resp_len_num) AS avg_bytes`)
			total := GetFloat(r, "total_bytes")
			avg := GetFloat(r, "avg_bytes")
			if total <= 0 || avg <= 0 {
				t.Errorf("expected positive values: total=%f avg=%f", total, avg)
			}
		})
		t.Run("MinMax_ResponseTime", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "time: (?<resp_time>[0-9.]+)" | WHERE isnotnull(resp_time) | EVAL rt = tonumber(resp_time) | STATS min(rt) AS min_time, max(rt) AS max_time`)
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
			r := h.MustQuery(`FROM idx_openstack
    | REX "\"(?<method>GET|POST|DELETE)"
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(method) AND isnotnull(status)
    | STATS count BY method, status
    | SORT method, status`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 method x status combos, got %d", EventCount(r))
			}
		})
		t.Run("NestedEval_RequestsPerIP", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STATS count AS requests, dc(ip) AS unique_ips
    | EVAL requests_per_ip = round(requests / unique_ips, 2)`)
			rpi := GetFloat(r, "requests_per_ip")
			if rpi <= 0 {
				t.Errorf("expected requests_per_ip > 0, got %f", rpi)
			}
		})
		t.Run("Percentile_P95GEMedian", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL rt = tonumber(resp_time)
    | STATS perc95(rt) AS p95, perc50(rt) AS median`)
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
			r := h.MustQuery(`FROM idx_ssh | STATS earliest(_time) AS first_event, latest(_time) AS last_event`)
			first := GetStr(r, "first_event")
			last := GetStr(r, "last_event")
			if first == "" || last == "" {
				t.Errorf("expected non-empty timestamps: first=%q last=%q", first, last)
			}
		})
		t.Run("MaxFromSingleIP_867", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STATS count BY ip
    | STATS max(count) AS max_from_single_ip, avg(count) AS avg_per_ip`), "max_from_single_ip", 867)
		})
		t.Run("TopUsernames_Admin21", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "Invalid user (?<username>\w+)" | WHERE isnotnull(username) | STATS count BY username | SORT - count | HEAD 5`)
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

	// ─── Category 7: BIN (Time Bucketing) ───────────────────────────────
	t.Run("BIN", func(t *testing.T) {
		t.Run("Span1h_SumsTo2000", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | BIN _time span=1h AS hour_bucket | STATS count BY hour_bucket | SORT hour_bucket`)
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
			r := h.MustQuery(`FROM idx_openstack | BIN _time span=5m AS time_bucket | STATS count BY time_bucket | SORT time_bucket`)
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
			requireEventCount(t, h.MustQuery(`FROM idx_ssh | BIN _time span=1m AS minute_bucket | STATS count BY minute_bucket | SORT - count | HEAD 5`), 5)
		})
		t.Run("BucketCount_About15", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | BIN _time span=1m AS bucket | STATS dc(bucket) AS num_buckets`)
			buckets := GetInt(r, "num_buckets")
			if buckets < 10 || buckets > 20 {
				t.Errorf("expected ~15 buckets, got %d", buckets)
			}
		})
		t.Run("BINWithStats_AvgResponse", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL rt = tonumber(resp_time)
    | BIN _time span=5m AS bucket
    | STATS avg(rt) AS avg_response, count AS requests BY bucket
    | SORT bucket`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 time buckets, got %d", EventCount(r))
			}
		})
	})

	// ─── Category 8: SORT ───────────────────────────────────────────────
	t.Run("SORT", func(t *testing.T) {
		t.Run("Ascending_Order", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT count | HEAD 3`)
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
			r := h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT - count | HEAD 1`)
			rows := EventRows(r)
			if len(rows) == 0 {
				t.Fatal("expected at least 1 row, got 0 (REX extraction may be broken)")
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
			r := h.MustQuery(`FROM idx_openstack
    | REX "\"(?<method>GET|POST|DELETE)"
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(method) AND isnotnull(status)
    | STATS count BY method, status
    | SORT method, - count`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 rows, got %d", EventCount(r))
			}
		})
		t.Run("PreservesAllRows_30IPs", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | STATS count BY ip | SORT count | STATS count`), "count", 30)
		})
		t.Run("StringField_Alphabetical", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "Invalid user (?<username>\w+)" | WHERE isnotnull(username) | STATS count BY username | SORT username | HEAD 3`)
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

	// ─── Category 9: RENAME and TABLE ───────────────────────────────────
	t.Run("RenameTable", func(t *testing.T) {
		t.Run("Rename_IP_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | RENAME ip AS source_ip
    | WHERE isnotnull(source_ip)
    | STATS dc(source_ip) AS unique_sources`), "unique_sources", 30)
		})
		t.Run("Table_Raw_3Rows", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | HEAD 3 | TABLE _raw`)
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
			r := h.MustQuery(`FROM idx_openstack
    | REX "status: (?<status>\d+)"
    | RENAME status AS http_code
    | WHERE isnotnull(http_code)
    | STATS count BY http_code`)
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
		t.Run("TableMultipleFields", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh
    | REX "sshd\[(?<pid>\d+)\]"
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | HEAD 5
    | TABLE _time, pid, ip`), 5)
		})
	})

	// ─── Category 10: DEDUP ─────────────────────────────────────────────
	t.Run("DEDUP", func(t *testing.T) {
		t.Run("DedupField_UniqueIPs_30", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | DEDUP ip
    | STATS count`), "count", 30)
		})
		t.Run("KeepsFirst_5Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | DEDUP ip
    | SORT ip
    | HEAD 5`), 5)
		})
		t.Run("MultipleFields_AtLeast3", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "\"(?<method>GET|POST|DELETE)"
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(method) AND isnotnull(status)
    | DEDUP method, status
    | STATS count`)
			combos := GetInt(r, "count")
			if combos < 3 {
				t.Errorf("expected at least 3 unique combos, got %d", combos)
			}
		})
		t.Run("WithLimit_NoneOver3", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | DEDUP 3 ip
    | STATS count BY ip
    | WHERE count > 3
    | STATS count`), "count", 0)
		})
	})

	// ─── Category 11: EVENTSTATS ────────────────────────────────────────
	t.Run("EVENTSTATS", func(t *testing.T) {
		t.Run("GlobalAggregation_1017", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(status)
    | EVENTSTATS count AS total_requests
    | HEAD 1
    | TABLE status, total_requests`)
			totalReq := GetInt(r, "total_requests")
			if totalReq != 1017 {
				t.Errorf("expected total_requests=1017, got %d", totalReq)
			}
		})
		t.Run("WithBY_TopIP_867", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | EVENTSTATS count AS ip_count BY ip
    | WHERE ip = "183.62.140.253"
    | HEAD 1
    | TABLE ip, ip_count`)
			ipCount := GetInt(r, "ip_count")
			if ipCount != 867 {
				t.Errorf("expected ip_count=867, got %d", ipCount)
			}
		})
		t.Run("Percentage_Status200_About92", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(status)
    | STATS count BY status
    | EVENTSTATS sum(count) AS total
    | EVAL pct = round(count * 100 / total, 2)
    | SORT - pct`)
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
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | EVENTSTATS count AS total | STATS count`), "count", 2000)
		})
	})

	// ─── Category 12: STREAMSTATS ───────────────────────────────────────
	t.Run("STREAMSTATS", func(t *testing.T) {
		t.Run("RunningCount_10Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_ssh | STREAMSTATS count AS row_num | WHERE row_num <= 10 | TABLE row_num`), 10)
		})
		t.Run("Window_10Rows", func(t *testing.T) {
			requireEventCount(t, h.MustQuery(`FROM idx_openstack
    | REX "time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(resp_time)
    | EVAL rt = tonumber(resp_time)
    | STREAMSTATS window=5 avg(rt) AS rolling_avg
    | HEAD 10
    | TABLE rt, rolling_avg`), 10)
		})
		t.Run("CurrentTrue_LastRow2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | STREAMSTATS count AS running_total current=true
    | WHERE running_total = 2000
    | STATS count`), "count", 1)
		})
		t.Run("WithBY_FirstOccurrence", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | STREAMSTATS count AS ip_running_count BY ip
    | WHERE ip = "183.62.140.253" AND ip_running_count = 1
    | STATS count`), "count", 1)
		})
	})

	// ─── Category 13: TRANSACTION ───────────────────────────────────────
	t.Run("TRANSACTION", func(t *testing.T) {
		t.Run("ByIP_30Transactions", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | TRANSACTION ip
    | STATS count`), "count", 30)
		})
		t.Run("WithMaxspan_PositiveSessions", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | TRANSACTION ip maxspan=5m
    | STATS count`)
			sessions := GetInt(r, "count")
			if sessions <= 0 {
				t.Errorf("expected sessions > 0, got %d", sessions)
			}
		})
		t.Run("Duration_PositiveMax", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | TRANSACTION ip
    | EVAL duration_sec = duration
    | STATS max(duration_sec) AS max_duration`)
			maxD := GetFloat(r, "max_duration")
			if maxD <= 0 {
				t.Errorf("expected max_duration > 0, got %f", maxD)
			}
		})
	})

	// ─── Category 14: Complex Multi-Stage Pipelines ─────────────────────
	t.Run("ComplexPipelines", func(t *testing.T) {
		t.Run("BruteForceDetection_Has183", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "Failed password for (?:invalid user )?(?<target>\w+) from (?<src_ip>\d+\.\d+\.\d+\.\d+) port (?<port>\d+)"
    | WHERE isnotnull(src_ip)
    | STATS count AS attempts, dc(target) AS unique_targets BY src_ip
    | WHERE attempts > 50
    | SORT - attempts`)
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
			r := h.MustQuery(`FROM idx_openstack
    | REX "\"(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+) HTTP"
    | REX "status: (?<status>\d+) len: (?<resp_len>\d+) time: (?<resp_time>[0-9.]+)"
    | WHERE isnotnull(method)
    | EVAL resp_ms = round(tonumber(resp_time) * 1000, 2)
    | STATS count AS requests, avg(resp_ms) AS avg_latency, max(resp_ms) AS max_latency BY method
    | SORT method`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 methods, got %d", EventCount(r))
			}
		})
		t.Run("EventClassification_PercentagesSumTo100", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
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
			r := h.MustQuery(`FROM idx_openstack
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
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 lifecycle event types, got %d", EventCount(r))
			}
		})
		t.Run("TwoLevelAggregation_AtLeast2ThreatLevels", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
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
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 threat levels, got %d", EventCount(r))
			}
		})
		t.Run("TimeBucketedRate_AtLeast2Windows", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | BIN _time span=10m AS time_window
    | STATS count AS events, dc(ip) AS unique_ips BY time_window
    | EVAL events_per_ip = round(events / unique_ips, 2)
    | SORT time_window`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 time windows, got %d", EventCount(r))
			}
		})
		t.Run("RequestAnalysisChain_AtLeast2Rows", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack
    | REX "\"(?<method>GET|POST|DELETE) (?<url_path>/[^\s]+)"
    | REX "status: (?<status>\d+)"
    | WHERE isnotnull(method) AND isnotnull(url_path)
    | EVAL endpoint = CASE(
          match(url_path, "servers/detail"), "servers_detail",
          match(url_path, "os-server-external-events"), "external_events",
          match(url_path, "metadata"), "metadata",
          1=1, "other"
      )
    | STATS count BY endpoint, method
    | SORT - count`)
			if EventCount(r) < 2 {
				t.Errorf("expected at least 2 rows, got %d", EventCount(r))
			}
		})
		t.Run("SubnetAnalysis_AtLeast3Subnets", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh
    | REX "(?<ip>\d+\.\d+\.\d+\.\d+)"
    | WHERE isnotnull(ip)
    | REX field=ip "(?<subnet>\d+\.\d+\.\d+)\."
    | STATS count AS requests, dc(ip) AS unique_hosts BY subnet
    | SORT - requests
    | HEAD 5`)
			if EventCount(r) < 3 {
				t.Errorf("expected at least 3 subnets, got %d", EventCount(r))
			}
		})
	})

	// ─── Category 16: Wildcard Search ───────────────────────────────────
	t.Run("WildcardSearch", func(t *testing.T) {
		// A) Prefix wildcards
		t.Run("Prefix_Failed_610", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search Failed* | STATS count`), "count", 610)
		})
		t.Run("Prefix_ReceivedDisconnect_468", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Received disconnect*" | STATS count`), "count", 468)
		})
		// B) Suffix wildcards
		t.Run("Suffix_Preauth_618", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search *preauth | STATS count`), "count", 618)
		})
		t.Run("Suffix_AuthFailure_496", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "*authentication failure" | STATS count`), "count", 496)
		})
		// C) Contains wildcards
		t.Run("Contains_Password_521", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "*password*" | STATS count`), "count", 521)
		})
		t.Run("Contains_Preauth_618", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search *preauth* | STATS count`), "count", 618)
		})
		t.Run("Contains_IP_10", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search *173.234.31.186* | STATS count`), "count", 10)
		})
		// D) Multi-wildcard
		t.Run("Multi_FailedFromPort_524", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed*from*port" | STATS count`), "count", 524)
		})
		t.Run("Multi_FailedInvalidFrom_139", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed*invalid*from" | STATS count`), "count", 139)
		})
		t.Run("Multi_PasswordRootPortSsh2_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "*password*root*port*ssh2" | STATS count`), "count", 370)
		})
		// E) Wildcard-only
		t.Run("WildcardOnly_All_2000", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search * | STATS count`), "count", 2000)
		})
		// F) Wildcard with boolean operators
		t.Run("WildcardAND_Root_370", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed*" root | STATS count`), "count", 370)
		})
		t.Run("WildcardOR_FailedOrInvalid_836", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search "Failed*" OR "Invalid*" | STATS count`), "count", 836)
		})
		t.Run("WildcardNOT_NotRoot_1257", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | search NOT "*root*" | STATS count`), "count", 1257)
		})
		// G) Field comparison wildcards
		t.Run("FieldPrefix_IP173_10", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | search ip=173.234.* | STATS count`), "count", 10)
		})
		t.Run("FieldSuffix_IP253_Positive", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "(?<ip>\d+\.\d+\.\d+\.\d+)" | WHERE isnotnull(ip) | search ip=*253 | STATS count`)
			total := GetInt(r, "count")
			if total <= 0 {
				t.Errorf("expected > 0 events with IP ending in 253, got %d", total)
			}
		})
		t.Run("FieldExistence_Port_525", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_ssh | REX "port (?<port>\d+)" | search port=* | STATS count`)
			total := GetInt(r, "count")
			if total != 525 {
				t.Errorf("expected 525 events with port field, got %d", total)
			}
		})
		// H) IN with wildcards
		t.Run("INWithWildcards_GET_POST_GT990", func(t *testing.T) {
			r := h.MustQuery(`FROM idx_openstack | REX "\"(?<method>GET|POST|PUT|DELETE|PATCH)" | WHERE isnotnull(method) | search method IN (G*, P*) | STATS count`)
			total := GetInt(r, "count")
			if total <= 990 {
				t.Errorf("expected > 990 events matching GET/POST/PUT/PATCH, got %d", total)
			}
		})
		// I) OpenStack wildcards
		t.Run("LifecycleEvent_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search "*Lifecycle Event*" | STATS count`), "count", 109)
		})
		t.Run("VMEvent_109", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search "VM*Event" | STATS count`), "count", 109)
		})
		t.Run("NovaInstance_646", func(t *testing.T) {
			requireAggValue(t, h.MustQuery(`FROM idx_openstack | search "nova*instance*" | STATS count`), "count", 646)
		})
	})
}
