import type { AggregateResult } from "../../api/client";

const PALETTE = [
  "#4F46E5",
  "#3b82f6",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#6b7280",
];

interface Slice {
  label: string;
  value: number;
  color: string;
  pct: string;
}

/** Detect category vs value columns by inspecting row data types. */
function classifyColumns(data: AggregateResult): { categoryIdx: number; valueIdxs: number[] } {
  let categoryIdx = 0;
  const valueIdxs: number[] = [];
  for (let ci = 0; ci < data.columns.length; ci++) {
    const isNumeric = data.rows.length > 0 && data.rows.every(
      (r) => r[ci] == null || typeof r[ci] === "number" || (typeof r[ci] === "string" && !isNaN(Number(r[ci])) && String(r[ci]).trim() !== ""),
    );
    if (isNumeric) {
      valueIdxs.push(ci);
    } else {
      categoryIdx = ci;
    }
  }
  if (valueIdxs.length === 0) {
    return { categoryIdx: 0, valueIdxs: data.columns.slice(1).map((_, i) => i + 1) };
  }
  return { categoryIdx, valueIdxs };
}

export function PiePanel({ data }: { data: AggregateResult }) {
  if (!data || data.rows.length === 0) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          color: "var(--text-secondary)",
          fontSize: "0.8rem",
        }}
      >
        No data
      </div>
    );
  }

  const { categoryIdx, valueIdxs } = classifyColumns(data);
  const valIdx = valueIdxs[0] ?? 1;

  const total = data.rows.reduce((s, r) => s + (Number(r[valIdx]) || 0), 0);
  if (total === 0) {
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          color: "var(--text-secondary)",
          fontSize: "0.8rem",
        }}
      >
        No data
      </div>
    );
  }

  const slices: Slice[] = data.rows.map((row, i) => ({
    label: String(row[categoryIdx] ?? ""),
    value: Number(row[valIdx]) || 0,
    color: PALETTE[i % PALETTE.length],
    pct: (((Number(row[valIdx]) || 0) / total) * 100).toFixed(1),
  }));

  // Build SVG paths
  let cumAngle = -Math.PI / 2;
  const paths: { d: string; color: string }[] = [];

  for (const slice of slices) {
    const angle = (slice.value / total) * 2 * Math.PI;

    if (slices.length === 1) {
      // Full circle
      paths.push({ d: "M 50 10 A 40 40 0 1 1 49.99 10 Z", color: slice.color });
    } else {
      const x1 = 50 + 40 * Math.cos(cumAngle);
      const y1 = 50 + 40 * Math.sin(cumAngle);
      cumAngle += angle;
      const x2 = 50 + 40 * Math.cos(cumAngle);
      const y2 = 50 + 40 * Math.sin(cumAngle);
      const largeArc = angle > Math.PI ? 1 : 0;
      const d = `M 50 50 L ${x1} ${y1} A 40 40 0 ${largeArc} 1 ${x2} ${y2} Z`;
      paths.push({ d, color: slice.color });
    }
  }

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        height: "100%",
        gap: "12px",
        padding: "4px",
      }}
    >
      <svg viewBox="0 0 100 100" style={{ width: "55%", maxHeight: "100%" }}>
        {paths.map((p, i) => (
          <path key={i} d={p.d} fill={p.color} />
        ))}
      </svg>
      <div style={{ fontSize: "11px", overflow: "auto", flex: 1 }}>
        {slices.map((s, i) => (
          <div
            key={i}
            style={{
              display: "flex",
              alignItems: "center",
              gap: "4px",
              marginBottom: "2px",
            }}
          >
            <span
              style={{
                width: "8px",
                height: "8px",
                borderRadius: "50%",
                background: s.color,
                flexShrink: 0,
                display: "inline-block",
              }}
            />
            <span
              style={{
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
              }}
            >
              {s.label} ({s.pct}%)
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
