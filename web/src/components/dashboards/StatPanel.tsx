import type { QueryResult, AggregateResult, EventsResult, DashboardPanel } from "../../api/client";
import { formatCount } from "../../utils/format";
import styles from "./StatPanel.module.css";

interface StatPanelProps {
  data: QueryResult;
  panel: DashboardPanel;
}

export function StatPanel({ data, panel }: StatPanelProps) {
  let value: number | null = null;
  let label = panel.title;

  if (data.type === "events") {
    const events = (data as EventsResult).events;
    value = events.length;
    label = label || "Events";
  } else {
    const agg = data as AggregateResult;
    if (agg.rows.length > 0) {
      // Find first non-time column
      const valIdx = agg.columns.findIndex(
        (c) => c !== "_time" && c !== "time" && c !== "timestamp",
      );
      const idx = valIdx >= 0 ? valIdx : agg.columns.length > 1 ? 1 : 0;
      value = Number(agg.rows[0][idx]) || 0;
      label = label || agg.columns[idx] || "Value";
    }
  }

  const displayValue = value != null ? formatCount(value) : "--";

  return (
    <div class={styles.stat}>
      <div class={styles.value}>{displayValue}</div>
      <div class={styles.label}>{label}</div>
    </div>
  );
}
