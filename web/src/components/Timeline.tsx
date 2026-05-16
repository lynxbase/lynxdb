import { useRef, useEffect, useCallback, useState } from "react";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { HistogramBucket, HistogramBucketGrouped } from "../api/client";
import { cssVar, chartAxisFont } from "../utils/chartColors";

/** Stacking order from bottom to top */
const LEVEL_ORDER = ["debug", "info", "warn", "error"];

/** Colors per log level */
const LEVEL_COLORS: Record<string, string> = {
  error: "#f2495c",
  warn: "#ff9830",
  info: "#5794f2",
  debug: "#8e8e8e",
  other: "#6e6e6e",
};

interface TimelineProps {
  from: string;
  to?: string;
  buckets: HistogramBucket[];
  groupedBuckets?: HistogramBucketGrouped[];
  visible: boolean;
  onBrush?: (from: number, to: number) => void;
  onReset?: () => void;
  showReset?: boolean;
}

/**
 * Convert histogram buckets into the [timestamps[], counts[]] tuple that uPlot
 * expects. uPlot x-axis uses epoch seconds (not milliseconds).
 */
function toUPlotData(buckets: HistogramBucket[]): [number[], number[]] {
  const times: number[] = [];
  const counts: number[] = [];
  for (const b of buckets) {
    times.push(new Date(b.time).getTime() / 1000);
    counts.push(b.count);
  }
  return [times, counts];
}

/**
 * Convert grouped histogram buckets into stacked uPlot data.
 * Returns timestamps + one cumulative array per level (bottom to top).
 */
function toStackedUPlotData(buckets: HistogramBucketGrouped[]): {
  data: number[][];
  levels: string[];
  maxCumulative: number;
} {
  const times: number[] = [];

  // Discover all levels present across all buckets; normalize to lowercase
  const levelSet = new Set<string>();
  for (const b of buckets) {
    times.push(new Date(b.time).getTime() / 1000);
    for (const key of Object.keys(b.counts)) {
      levelSet.add(key.toLowerCase());
    }
  }

  // Build ordered level list: known levels in LEVEL_ORDER first, then "other" and any extras
  const levels: string[] = [];
  for (const l of LEVEL_ORDER) {
    if (levelSet.has(l)) {
      levels.push(l);
      levelSet.delete(l);
    }
  }
  // Add "other" next, then any remaining unknown levels
  if (levelSet.has("other")) {
    levels.push("other");
    levelSet.delete("other");
  }
  for (const l of Array.from(levelSet).sort()) {
    levels.push(l);
  }

  // Build cumulative stacked arrays; levels[s] is already lowercase —
  // try lowercase key first, then the original uppercase variant.
  const rawArrays: number[][] = levels.map(() =>
    new Array(buckets.length).fill(0),
  );
  for (let i = 0; i < buckets.length; i++) {
    const bucket = buckets[i];
    if (!bucket) continue;
    for (let s = 0; s < levels.length; s++) {
      const level = levels[s];
      if (!level) continue;
      const rawArr = rawArrays[s];
      if (!rawArr) continue;
      const count =
        bucket.counts[level] ??
        bucket.counts[level.toUpperCase()] ??
        0;
      rawArr[i] = count;
    }
  }

  // Stack: each level[s][i] = sum of all levels 0..s at position i
  const stackedArrays: number[][] = levels.map(() =>
    new Array(buckets.length).fill(0),
  );
  let maxCumulative = 0;
  for (let i = 0; i < buckets.length; i++) {
    let cumulative = 0;
    for (let s = 0; s < levels.length; s++) {
      const rawArr = rawArrays[s];
      const stackedArr = stackedArrays[s];
      if (!rawArr || !stackedArr) continue;
      cumulative += rawArr[i] ?? 0;
      stackedArr[i] = cumulative;
    }
    if (cumulative > maxCumulative) {
      maxCumulative = cumulative;
    }
  }

  return { data: [times, ...stackedArrays], levels, maxCumulative };
}

/**
 * Format an epoch-seconds timestamp for the tooltip.
 * Shows "MMM DD HH:mm".
 */
function formatTooltipTime(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const mon = d.toLocaleString("en-US", { month: "short" });
  const dd = String(d.getDate()).padStart(2, "0");
  return `${mon} ${dd} ${hh}:${mm}`;
}

function levelColor(level: string): string {
  return LEVEL_COLORS[level] ?? LEVEL_COLORS["other"] ?? "#6e6e6e";
}

export function Timeline({
  buckets,
  groupedBuckets,
  visible,
  onBrush,
  onReset,
  showReset,
}: TimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [tooltipVisible, setTooltipVisible] = useState(false);
  const [tooltipContent, setTooltipContent] = useState<string[]>([]);
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  // Determine mode: grouped vs ungrouped
  const isGrouped = groupedBuckets != null && groupedBuckets.length > 0;
  const hasBuckets = isGrouped || buckets.length > 0;

  // Tooltip handler for ungrouped mode
  const handleCursorMoveUngrouped = useCallback((u: uPlot) => {
    const idx = u.cursor.idx;
    const xData = u.data[0];
    const yData = u.data[1];
    if (idx == null || idx < 0 || !xData || idx >= xData.length) {
      setTooltipVisible(false);
      return;
    }
    const ts = xData[idx] ?? 0;
    const count = yData?.[idx];
    setTooltipContent([formatTooltipTime(ts), String(count ?? 0)]);
    setTooltipPos({
      x: (u.cursor.left ?? 0) + 10,
      y: (u.cursor.top ?? 0) - 10,
    });
    setTooltipVisible(true);
  }, []);

  // Create / recreate chart when buckets change
  useEffect(() => {
    const el = containerRef.current;
    if (!el || !hasBuckets) {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
      return;
    }

    const borderColor = cssVar("--chart-grid") || cssVar("--border") || "#2c3235";
    const textMuted = cssVar("--chart-axis") || cssVar("--text-muted") || "#8e8e8e";

    if (isGrouped) {
      // -- Stacked grouped mode --
      const stacked = toStackedUPlotData(groupedBuckets!);
      const { data, levels, maxCumulative } = stacked;

      const barWidthFactor = groupedBuckets!.length > 1 ? 0.85 : 0.5;

      const series: uPlot.Series[] = [{}]; // x-axis placeholder
      for (let s = 0; s < levels.length; s++) {
        const lvl = levels[s] ?? "";
        const color = levelColor(lvl);
        series.push({
          label: lvl,
          fill: color + "cc", // 80% opacity
          stroke: color,
          width: 0,
          paths: stackedBarsPaths(barWidthFactor, s),
        });
      }

      // Tooltip for grouped mode
      const handleCursorGrouped = (u: uPlot) => {
        const idx = u.cursor.idx;
        const xData = u.data[0];
        if (idx == null || idx < 0 || !xData || idx >= xData.length) {
          setTooltipVisible(false);
          return;
        }
        const ts = xData[idx] ?? 0;
        const lines = [formatTooltipTime(ts)];
        // Show per-level breakdown (top to bottom = reverse of stacking)
        for (let s = levels.length - 1; s >= 0; s--) {
          const cumArr = u.data[s + 1];
          const prevArr = s > 0 ? u.data[s] : undefined;
          const cumVal = cumArr?.[idx] ?? 0;
          const prevVal = s > 0 ? (prevArr?.[idx] ?? 0) : 0;
          const raw = cumVal - prevVal;
          if (raw > 0) {
            lines.push(`${levels[s] ?? ""}: ${raw}`);
          }
        }
        setTooltipContent(lines);
        setTooltipPos({
          x: (u.cursor.left ?? 0) + 10,
          y: (u.cursor.top ?? 0) - 10,
        });
        setTooltipVisible(true);
      };

      const opts: uPlot.Options = {
        width: el.clientWidth,
        height: 80,
        cursor: {
          x: true,
          y: false,
          points: { show: false },
          drag: { x: true, y: false, setScale: false },
        },
        select: {
          show: true,
          left: 0,
          top: 0,
          width: 0,
          height: 80,
        },
        hooks: {
          setCursor: [handleCursorGrouped],
          setSelect: [
            (u: uPlot) => {
              const sel = u.select;
              if (sel.width > 10) {
                const fromTs = u.posToVal(sel.left, "x");
                const toTs = u.posToVal(sel.left + sel.width, "x");
                if (onBrush && fromTs < toTs) {
                  onBrush(fromTs, toTs);
                }
              }
              u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
            },
          ],
        },
        legend: { show: false },
        axes: [
          {
            show: true,
            stroke: textMuted,
            grid: { show: true, stroke: borderColor, width: 1 },
            ticks: { show: false },
            font: chartAxisFont(),
            size: 20,
            gap: 2,
          },
          {
            show: false,
            grid: { show: false },
          },
        ],
        scales: {
          x: { time: true },
          y: { range: () => [0, (maxCumulative || 1) * 1.1] },
        },
        series,
      };

      if (chartRef.current) {
        chartRef.current.destroy();
      }
      chartRef.current = new uPlot(opts, data as uPlot.AlignedData, el);
    } else {
      // -- Ungrouped mode (backward compatible) --
      const data = toUPlotData(buckets);
      const accentColor = cssVar("--accent") || cssVar("--primary") || "#3274d9";
      const barWidthFactor = buckets.length > 1 ? 0.85 : 0.5;

      const opts: uPlot.Options = {
        width: el.clientWidth,
        height: 80,
        cursor: {
          x: true,
          y: false,
          points: { show: false },
          drag: { x: true, y: false, setScale: false },
        },
        select: {
          show: true,
          left: 0,
          top: 0,
          width: 0,
          height: 80,
        },
        hooks: {
          setCursor: [handleCursorMoveUngrouped],
          setSelect: [
            (u: uPlot) => {
              const sel = u.select;
              if (sel.width > 10) {
                const fromTs = u.posToVal(sel.left, "x");
                const toTs = u.posToVal(sel.left + sel.width, "x");
                if (onBrush && fromTs < toTs) {
                  onBrush(fromTs, toTs);
                }
              }
              u.setSelect({ left: 0, top: 0, width: 0, height: 0 }, false);
            },
          ],
        },
        legend: { show: false },
        axes: [
          {
            show: true,
            stroke: textMuted,
            grid: { show: true, stroke: borderColor, width: 1 },
            ticks: { show: false },
            font: chartAxisFont(),
            size: 20,
            gap: 2,
          },
          {
            show: false,
            grid: { show: false },
          },
        ],
        scales: {
          x: { time: true },
          y: {
            range: (_u: uPlot, _min: number, max: number) => [
              0,
              max * 1.1 || 1,
            ],
          },
        },
        series: [
          {},
          {
            label: "Events",
            fill: accentColor + "66",
            stroke: accentColor,
            width: 1,
            paths: barsPaths(barWidthFactor),
          },
        ],
      };

      if (chartRef.current) {
        chartRef.current.destroy();
      }
      chartRef.current = new uPlot(opts, data, el);
    }

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [buckets, groupedBuckets, onBrush]);

  // Handle resize
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (chartRef.current) {
          chartRef.current.setSize({
            width: entry.contentRect.width,
            height: 80,
          });
        }
      }
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  // Hide tooltip when cursor leaves
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    function onLeave() {
      setTooltipVisible(false);
    }
    el.addEventListener("mouseleave", onLeave);
    return () => el.removeEventListener("mouseleave", onLeave);
  }, []);

  if (!visible) return null;

  const legendLevels = isGrouped
    ? toStackedUPlotData(groupedBuckets!).levels
    : [];

  return (
    <div className="shrink-0 border-b border-border">
      <div className="relative w-full h-20 shrink-0 bg-background overflow-hidden [&_.u-wrap]:bg-transparent [&_.u-over]:cursor-crosshair [&_.u-select]:bg-primary [&_.u-select]:opacity-12" ref={containerRef}>
        {!hasBuckets && (
          <div className="flex items-center justify-center h-full text-muted-foreground text-xs">No histogram data</div>
        )}
        <div
          ref={tooltipRef}
          className={`absolute z-10 pointer-events-none px-2 py-1 rounded-sm bg-card border border-border text-foreground font-mono text-[0.6875rem] whitespace-nowrap leading-snug transition-opacity duration-100 ${tooltipVisible ? "opacity-100" : "opacity-0"}`}
          style={{
            left: `${tooltipPos.x}px`,
            top: `${tooltipPos.y}px`,
          }}
        >
          {tooltipContent.map((line, i) => (
            <div
              key={i}
              className={i === 0 ? "text-muted-foreground" : "text-primary font-semibold"}
            >
              {line}
            </div>
          ))}
        </div>
        {showReset && (
          <button
            type="button"
            className="absolute top-1 right-2 z-[5] px-2 py-0.5 text-[0.625rem] font-mono bg-secondary text-muted-foreground border border-border rounded-sm cursor-pointer hover:bg-muted hover:text-foreground transition-colors"
            onClick={onReset}
            aria-label="Reset time range"
            title="Reset time range"
          >
            Reset
          </button>
        )}
      </div>
      {isGrouped && legendLevels.length > 0 && (
        <div className="flex gap-3 py-0.5 pl-10 text-[0.6875rem] bg-background">
          {legendLevels.map((level) => (
            <span key={level} className="inline-flex items-center gap-1 text-muted-foreground">
              <span
                className="inline-block size-2 rounded-full shrink-0"
                style={{ background: levelColor(level) }}
              />
              {level}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * Custom bars path builder for uPlot (ungrouped mode).
 * Draws filled rectangles for each data point.
 */
function barsPaths(widthFactor: number): uPlot.Series.PathBuilder {
  return (u: uPlot, seriesIdx: number, _idx0: number, _idx1: number) => {
    const xData = u.data[0];
    const yData = u.data[seriesIdx];
    const xScale = u.scales["x"];
    const yScale = u.scales["y"];

    if (!xData || !yData || xData.length < 2 || !xScale || !yScale) {
      return null;
    }

    // Both values are guaranteed defined: xData.length >= 2
    const x0 = xData[0] ?? 0;
    const x1 = xData[1] ?? 0;
    const xLast = xData[xData.length - 1] ?? 0;
    const dataSpacing = xData.length > 1 ? x1 - x0 : 60;
    const xMin = xScale.min ?? x0;
    const xMax = xScale.max ?? xLast;
    const plotWidth = u.bbox.width;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(
      1,
      (dataSpacing / xRange) * plotWidth * widthFactor,
    );

    const fillPath = new Path2D();
    const strokePath = new Path2D();

    const yMin = yScale.min ?? 0;
    const yMax = yScale.max ?? 1;
    const plotHeight = u.bbox.height;
    const plotLeft = u.bbox.left;
    const plotTop = u.bbox.top;

    for (let i = 0; i < xData.length; i++) {
      const xVal = xData[i];
      const yVal = yData[i];
      if (xVal == null || yVal == null || yVal === 0) continue;

      const cx = plotLeft + ((xVal - xMin) / xRange) * plotWidth;
      const barH = ((yVal - yMin) / (yMax - yMin)) * plotHeight;
      const x = cx - barWidthPx / 2;
      const y = plotTop + plotHeight - barH;

      fillPath.rect(x, y, barWidthPx, barH);
      strokePath.rect(x, y, barWidthPx, barH);
    }

    return {
      fill: fillPath,
      stroke: strokePath,
      clip: undefined as unknown as Path2D,
      flags: 3,
    };
  };
}

/**
 * Stacked bars path builder for uPlot.
 * Draws bars from previous series cumulative value to current series cumulative value.
 * seriesLevel is 0-based index into the levels array (not uPlot series index).
 */
function stackedBarsPaths(
  widthFactor: number,
  seriesLevel: number,
): uPlot.Series.PathBuilder {
  return (u: uPlot, seriesIdx: number, _idx0: number, _idx1: number) => {
    const xData = u.data[0];
    const yData = u.data[seriesIdx];
    const xScale = u.scales["x"];
    const yScale = u.scales["y"];

    if (!xData || !yData || xData.length < 2 || !xScale || !yScale) {
      return null;
    }

    // Previous stacked series data (the bottom of this bar segment)
    const prevData = seriesLevel > 0 ? u.data[seriesIdx - 1] : null;

    const x0 = xData[0] ?? 0;
    const x1 = xData[1] ?? 0;
    const xLast = xData[xData.length - 1] ?? 0;
    const dataSpacing = xData.length > 1 ? x1 - x0 : 60;
    const xMin = xScale.min ?? x0;
    const xMax = xScale.max ?? xLast;
    const plotWidth = u.bbox.width;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(
      1,
      (dataSpacing / xRange) * plotWidth * widthFactor,
    );

    const fillPath = new Path2D();

    const yMin = yScale.min ?? 0;
    const yMax = yScale.max ?? 1;
    const yRange = yMax - yMin || 1;
    const plotHeight = u.bbox.height;
    const plotLeft = u.bbox.left;
    const plotTop = u.bbox.top;

    for (let i = 0; i < xData.length; i++) {
      const xVal = xData[i];
      if (xVal == null) continue;
      const topVal = yData[i] ?? 0;
      const bottomVal = prevData ? (prevData[i] ?? 0) : 0;
      if (topVal <= bottomVal) continue;

      const cx = plotLeft + ((xVal - xMin) / xRange) * plotWidth;
      const topY =
        plotTop + plotHeight - ((topVal - yMin) / yRange) * plotHeight;
      const bottomY =
        plotTop + plotHeight - ((bottomVal - yMin) / yRange) * plotHeight;
      const x = cx - barWidthPx / 2;
      const barH = bottomY - topY;

      if (barH > 0) {
        fillPath.rect(x, topY, barWidthPx, barH);
      }
    }

    return {
      fill: fillPath,
      stroke: fillPath,
      clip: undefined as unknown as Path2D,
      flags: 3,
    };
  };
}
