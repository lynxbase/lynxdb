/**
 * Time range formatting utilities for the TimeRangePicker.
 * Handles parsing relative expressions and producing human-readable labels.
 */

export interface Preset {
  label: string;
  value: string;
}

/** Shared preset definitions used by TimeRangePicker and label formatting. */
export const PRESETS: Preset[] = [
  { label: "Last 5 minutes", value: "-5m" },
  { label: "Last 15 minutes", value: "-15m" },
  { label: "Last 30 minutes", value: "-30m" },
  { label: "Last 1 hour", value: "-1h" },
  { label: "Last 3 hours", value: "-3h" },
  { label: "Last 6 hours", value: "-6h" },
  { label: "Last 12 hours", value: "-12h" },
  { label: "Last 24 hours", value: "-24h" },
  { label: "Last 2 days", value: "-2d" },
  { label: "Last 7 days", value: "-7d" },
  { label: "Last 30 days", value: "-30d" },
  { label: "Last 90 days", value: "-90d" },
];

/**
 * Parse a human-friendly relative expression into server-compatible from/to values.
 *
 * Examples:
 *   "2h ago to 30m ago"  -> { from: "-2h", to: "-30m" }
 *   "1d ago to now"      -> { from: "-1d", to: "now" }
 *   "4h ago to 1h ago"   -> { from: "-4h", to: "-1h" }
 *
 * Returns null if the expression does not match the expected format.
 */
export function parseRelativeExpression(
  expr: string,
): { from: string; to: string } | null {
  const trimmed = expr.trim();
  const re = /^(\d+[smhdw])\s+ago\s+to\s+(\d+[smhdw]\s+ago|now)$/i;
  const match = trimmed.match(re);
  if (!match) return null;

  const fromPart = match[1] ?? ""; // e.g. "2h"
  const toPart = match[2] ?? ""; // e.g. "30m ago" or "now"

  const fromVal = `-${fromPart}`;
  let toVal: string;

  if (toPart.toLowerCase() === "now") {
    toVal = "now";
  } else {
    // Strip " ago" suffix to get the duration part
    const toMatch = toPart.match(/^(\d+[smhdw])\s+ago$/i);
    if (!toMatch) return null;
    toVal = `-${toMatch[1] ?? ""}`;
  }

  return { from: fromVal, to: toVal };
}

/**
 * Parse a Grafana-style "now-3h" expression into the internal format.
 *
 * Examples:
 *   "now-3h"  -> "-3h"
 *   "now-30m" -> "-30m"
 *   "now"     -> undefined (meaning current time)
 *
 * Returns null if the expression does not match.
 */
export function parseNowExpression(expr: string): string | undefined | null {
  const trimmed = expr.trim().toLowerCase();
  if (trimmed === "now") return undefined;

  const match = trimmed.match(/^now\s*-\s*(\d+[smhdw])$/);
  if (match) return `-${match[1]}`;

  // Try bare relative format: "-3h"
  if (/^-\d+[smhdw]$/.test(trimmed)) return trimmed;

  return null; // Not a valid now-expression
}

/**
 * Convert an internal relative value to Grafana-style display format.
 *
 * "-3h"      -> "now-3h"
 * undefined  -> "now"
 * ISO string -> as-is
 */
export function toNowExpr(value: string | undefined): string {
  if (value === undefined || value === "now") return "now";
  if (value.startsWith("-")) return `now${value}`;
  return value;
}

/**
 * Convert a relative server value like "-2h" to a human label like "2h ago".
 */
function relativeToLabel(val: string): string {
  if (val === "now") return "now";
  if (val.startsWith("-")) return `${val.slice(1)} ago`;
  return val;
}

/**
 * Format a short date/time string for display in the trigger button.
 * Example: "Mar 15 14:00"
 */
function formatShortDateTime(isoStr: string): string {
  const d = new Date(isoStr);
  if (isNaN(d.getTime())) return isoStr;
  const month = d.toLocaleDateString("en-US", { month: "short" });
  const day = d.getDate();
  const time = d.toLocaleTimeString("en-US", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
  });
  return `${month} ${day} ${time}`;
}

/**
 * Produce a human-readable label for the current time range selection.
 *
 * - Preset match (e.g. "-1h" with no to): "Last 1 hour"
 * - Relative range: "2h ago to 30m ago"
 * - Absolute ISO dates: "Mar 15 14:00 -- Mar 15 16:00"
 */
export function getTimeRangeLabel(
  from: string,
  to: string | undefined,
): string {
  // Check if it matches a preset (from is a preset value, to is undefined)
  if (to === undefined || to === "now") {
    const preset = PRESETS.find((p) => p.value === from);
    if (preset) return preset.label;
  }

  // Relative values (start with "-" or "now")
  const isRelativeFrom = from.startsWith("-");
  const isRelativeTo =
    to === undefined ||
    to === "now" ||
    (to !== undefined && to.startsWith("-"));

  if (isRelativeFrom && isRelativeTo) {
    const fromLabel = relativeToLabel(from);
    const toLabel = to === undefined ? "now" : relativeToLabel(to);
    return `${fromLabel} to ${toLabel}`;
  }

  // Absolute ISO dates
  const fromFormatted = formatShortDateTime(from);
  const toFormatted = to ? formatShortDateTime(to) : "now";
  return `${fromFormatted} -- ${toFormatted}`;
}
