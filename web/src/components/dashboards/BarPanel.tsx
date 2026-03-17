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

/** Custom bars path builder for uPlot. */
function barsPaths(widthFactor: number): uPlot.Series.PathBuilder {
  return (u: uPlot, seriesIdx: number) => {
    const xData = u.data[0];
    const yData = u.data[seriesIdx];
    const xScale = u.scales.x;
    const yScale = u.scales.y;

    if (!xData || !yData || xData.length < 1) return null;

    const dataSpacing = xData.length > 1 ? xData[1] - xData[0] : 1;
    const xMin = xScale.min ?? xData[0];
    const xMax = xScale.max ?? xData[xData.length - 1];
    const plotWidth = u.bbox.width / devicePixelRatio;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(
      4,
      (dataSpacing / xRange) * plotWidth * widthFactor,
    );

    const fillPath = new Path2D();

    const yMin = yScale.min ?? 0;
    const yMax = yScale.max ?? 1;
    const plotHeight = u.bbox.height / devicePixelRatio;
    const plotLeft = u.bbox.left / devicePixelRatio;
    const plotTop = u.bbox.top / devicePixelRatio;

    for (let i = 0; i < xData.length; i++) {
      const xVal = xData[i];
      const yVal = yData[i];
      if (yVal == null || yVal === 0) continue;

      const cx = plotLeft + ((xVal - xMin) / xRange) * plotWidth;
      const barH =
        ((Number(yVal) - yMin) / ((yMax as number) - yMin)) * plotHeight;
      const x = cx - barWidthPx / 2;
      const y = plotTop + plotHeight - barH;

      fillPath.rect(x, y, barWidthPx, barH);
    }

    return {
      fill: fillPath,
      stroke: fillPath,
      clip: undefined as unknown as Path2D,
      flags: 3,
    };
  };
}

export function BarPanel({ data }: { data: AggregateResult }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !data || data.rows.length === 0) return;

    // First column = categories, rest = values
    const labels = data.rows.map((r) => String(r[0] ?? ""));
    const xValues = data.rows.map((_, i) => i);
    const valueCols = data.columns.slice(1);

    const seriesData: number[][] = valueCols.map((_, ci) =>
      data.rows.map((r) => Number(r[ci + 1]) || 0),
    );

    const widthFactor = data.rows.length > 1 ? 0.7 : 0.5;

    const opts: uPlot.Options = {
      width: el.clientWidth,
      height: el.clientHeight - 4,
      scales: {
        x: { time: false },
        y: {
          range: (_u: uPlot, _min: number, max: number) => [0, max * 1.1 || 1],
        },
      },
      series: [
        {},
        ...valueCols.map((name, i) => ({
          label: name,
          fill: COLORS[i % COLORS.length] + "cc",
          stroke: COLORS[i % COLORS.length],
          width: 0,
          paths: barsPaths(widthFactor),
        })),
      ],
      axes: [
        {
          show: true,
          font: "10px sans-serif",
          size: 30,
          gap: 2,
          values: (_u: uPlot, splits: number[]) =>
            splits.map((s) => labels[Math.round(s)] ?? ""),
        },
        { show: true, font: "10px sans-serif", size: 40, gap: 4 },
      ],
      legend: { show: valueCols.length > 1 },
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
