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

export function TimechartPanel({ data }: { data: AggregateResult }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !data || data.rows.length === 0) return;

    // Find time column
    const timeIdx = data.columns.findIndex(
      (c) => c === "_time" || c === "time" || c === "timestamp",
    );
    const tIdx = timeIdx >= 0 ? timeIdx : 0;

    const seriesNames = data.columns.filter((_, i) => i !== tIdx);
    const times = data.rows.map(
      (r) => new Date(r[tIdx] as string).getTime() / 1000,
    );
    const seriesData: number[][] = seriesNames.map((name) => {
      const colIdx = data.columns.indexOf(name);
      return data.rows.map((r) => Number(r[colIdx]) || 0);
    });

    const opts: uPlot.Options = {
      width: el.clientWidth,
      height: el.clientHeight - 4,
      scales: { x: { time: true } },
      series: [
        {},
        ...seriesNames.map((name, i) => ({
          label: name,
          stroke: COLORS[i % COLORS.length],
          width: 2,
        })),
      ],
      axes: [
        { show: true, font: "10px sans-serif", size: 20, gap: 2 },
        { show: true, font: "10px sans-serif", size: 40, gap: 4 },
      ],
      legend: { show: seriesNames.length > 1 },
      cursor: { show: true, points: { show: false } },
    };

    chartRef.current?.destroy();
    chartRef.current = new uPlot(
      opts,
      [times, ...seriesData] as uPlot.AlignedData,
      el,
    );

    return () => {
      chartRef.current?.destroy();
      chartRef.current = null;
    };
  }, [data]);

  // Responsive sizing
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
