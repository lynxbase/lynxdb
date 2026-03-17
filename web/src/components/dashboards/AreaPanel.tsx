import { useRef, useEffect } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { AggregateResult } from "../../api/client";

const COLORS = [
  "#4F46E5",
  "#3b82f6",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#8b5cf6",
  "#ec4899",
  "#06b6d4",
];

export function AreaPanel({ data }: { data: AggregateResult }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !data || data.rows.length === 0) return;

    const xIsNumeric = data.rows.every(
      (r) => typeof r[0] === "number" || !isNaN(Number(r[0])),
    );
    const labels = data.rows.map((r) => String(r[0] ?? ""));
    const xValues = xIsNumeric
      ? data.rows.map((r) => Number(r[0]))
      : data.rows.map((_, i) => i);

    const seriesCols = data.columns.slice(1);
    const seriesData: number[][] = seriesCols.map((_, ci) =>
      data.rows.map((r) => Number(r[ci + 1]) || 0),
    );

    const opts: uPlot.Options = {
      width: el.clientWidth,
      height: el.clientHeight - 4,
      scales: { x: { time: false } },
      series: [
        {},
        ...seriesCols.map((name, i) => ({
          label: name,
          stroke: COLORS[i % COLORS.length],
          fill: COLORS[i % COLORS.length] + "33",
          width: 2,
        })),
      ],
      axes: [
        {
          show: true,
          font: "10px sans-serif",
          size: 20,
          gap: 2,
          ...(xIsNumeric
            ? {}
            : {
                values: (_u: uPlot, splits: number[]) =>
                  splits.map((s) => labels[Math.round(s)] ?? ""),
              }),
        },
        { show: true, font: "10px sans-serif", size: 40, gap: 4 },
      ],
      legend: { show: seriesCols.length > 1 },
      cursor: { show: true, points: { show: false } },
    };

    chartRef.current?.destroy();
    chartRef.current = new uPlot(
      opts,
      [xValues, ...seriesData] as uPlot.AlignedData,
      el,
    );

    return () => {
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [data]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        chartRef.current?.setSize({
          width: entry.contentRect.width,
          height: entry.contentRect.height - 4,
        });
      }
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  return <div ref={containerRef} style={{ width: "100%", height: "100%" }} />;
}
