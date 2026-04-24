import { useRef, useEffect, useCallback, useState } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { HistogramBucket, HistogramBucketGrouped } from "../api/client";
import { cssVar, chartAxisFont } from "../utils/chartColors";
import styles from "./Timeline.module.css";

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
  const rawArrays: number[][] = levels.map(() => new Array(buckets.length).fill(0));
  for (let i = 0; i < buckets.length; i++) {
    for (let s = 0; s < levels.length; s++) {
      const count =
        buckets[i].counts[levels[s]] ??
        buckets[i].counts[levels[s].toUpperCase()] ??
        0;
      rawArrays[s][i] = count;
    }
  }

  // Stack: each level[s][i] = sum of all levels 0..s at position i
  const stackedArrays: number[][] = levels.map(() => new Array(buckets.length).fill(0));
  let maxCumulative = 0;
  for (let i = 0; i < buckets.length; i++) {
    let cumulative = 0;
    for (let s = 0; s < levels.length; s++) {
      cumulative += rawArrays[s][i];
      stackedArrays[s][i] = cumulative;
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
  return LEVEL_COLORS[level] || LEVEL_COLORS.other;
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
  const handleCursorMoveUngrouped = useCallback(
    (u: uPlot) => {
      const idx = u.cursor.idx;
      if (idx == null || idx < 0 || !u.data[0] || idx >= u.data[0].length) {
        setTooltipVisible(false);
        return;
      }
      const ts = u.data[0][idx];
      const count = u.data[1][idx];
      setTooltipContent([
        formatTooltipTime(ts),
        String(count ?? 0),
      ]);
      setTooltipPos({ x: (u.cursor.left ?? 0) + 10, y: (u.cursor.top ?? 0) - 10 });
      setTooltipVisible(true);
    },
    [],
  );

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

    const borderColor = cssVar("--border") || "#e5e7eb";
    const textMuted = cssVar("--text-muted") || "#9ca3af";

    if (isGrouped) {
      // -- Stacked grouped mode --
      const stacked = toStackedUPlotData(groupedBuckets!);
      const { data, levels, maxCumulative } = stacked;

      const barWidthFactor = groupedBuckets!.length > 1 ? 0.85 : 0.5;

      const series: uPlot.Series[] = [{}]; // x-axis placeholder
      for (let s = 0; s < levels.length; s++) {
        const color = levelColor(levels[s]);
        series.push({
          label: levels[s],
          fill: color + "cc", // 80% opacity
          stroke: color,
          width: 0,
          paths: stackedBarsPaths(barWidthFactor, s),
        });
      }

      // Tooltip for grouped mode
      const handleCursorGrouped = (u: uPlot) => {
        const idx = u.cursor.idx;
        if (idx == null || idx < 0 || !u.data[0] || idx >= u.data[0].length) {
          setTooltipVisible(false);
          return;
        }
        const ts = u.data[0][idx];
        const lines = [formatTooltipTime(ts)];
        // Show per-level breakdown (top to bottom = reverse of stacking)
        for (let s = levels.length - 1; s >= 0; s--) {
          const cumVal = u.data[s + 1][idx] ?? 0;
          const prevVal = s > 0 ? (u.data[s][idx] ?? 0) : 0;
          const raw = cumVal - prevVal;
          if (raw > 0) {
            lines.push(`${levels[s]}: ${raw}`);
          }
        }
        setTooltipContent(lines);
        setTooltipPos({ x: (u.cursor.left ?? 0) + 10, y: (u.cursor.top ?? 0) - 10 });
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
      const accentColor = cssVar("--accent") || "#3274d9";
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
          y: { range: (_u: uPlot, _min: number, max: number) => [0, max * 1.1 || 1] },
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
    <div class={styles.wrapper}>
      <div class={styles.container} ref={containerRef}>
        {!hasBuckets && (
          <div class={styles.empty}>No histogram data</div>
        )}
        <div
          ref={tooltipRef}
          class={`${styles.tooltip} ${tooltipVisible ? styles.tooltipVisible : ""}`}
          style={{
            left: `${tooltipPos.x}px`,
            top: `${tooltipPos.y}px`,
          }}
        >
          {tooltipContent.map((line, i) => (
            <div key={i} class={i === 0 ? styles.tooltipTime : styles.tooltipCount}>
              {line}
            </div>
          ))}
        </div>
        {showReset && (
          <button
            type="button"
            class={styles.resetBtn}
            onClick={onReset}
            aria-label="Reset time range"
            title="Reset time range"
          >
            Reset
          </button>
        )}
      </div>
      {isGrouped && legendLevels.length > 0 && (
        <div class={styles.legend}>
          {legendLevels.map((level) => (
            <span key={level} class={styles.legendItem}>
              <span
                class={styles.legendDot}
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
    const xScale = u.scales.x;
    const yScale = u.scales.y;

    if (!xData || !yData || xData.length < 2) {
      return null;
    }

    const dataSpacing = xData.length > 1 ? xData[1] - xData[0] : 60;
    const xMin = xScale.min ?? xData[0];
    const xMax = xScale.max ?? xData[xData.length - 1];
    const plotWidth = u.bbox.width;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(1, (dataSpacing / xRange) * plotWidth * widthFactor);

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
      if (yVal == null || yVal === 0) continue;

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
function stackedBarsPaths(widthFactor: number, seriesLevel: number): uPlot.Series.PathBuilder {
  return (u: uPlot, seriesIdx: number, _idx0: number, _idx1: number) => {
    const xData = u.data[0];
    const yData = u.data[seriesIdx];
    const xScale = u.scales.x;
    const yScale = u.scales.y;

    if (!xData || !yData || xData.length < 2) {
      return null;
    }

    // Previous stacked series data (the bottom of this bar segment)
    const prevData = seriesLevel > 0 ? u.data[seriesIdx - 1] : null;

    const dataSpacing = xData.length > 1 ? xData[1] - xData[0] : 60;
    const xMin = xScale.min ?? xData[0];
    const xMax = xScale.max ?? xData[xData.length - 1];
    const plotWidth = u.bbox.width;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(1, (dataSpacing / xRange) * plotWidth * widthFactor);

    const fillPath = new Path2D();

    const yMin = yScale.min ?? 0;
    const yMax = yScale.max ?? 1;
    const yRange = yMax - yMin || 1;
    const plotHeight = u.bbox.height;
    const plotLeft = u.bbox.left;
    const plotTop = u.bbox.top;

    for (let i = 0; i < xData.length; i++) {
      const xVal = xData[i];
      const topVal = yData[i] ?? 0;
      const bottomVal = prevData ? (prevData[i] ?? 0) : 0;
      if (topVal <= bottomVal) continue;

      const cx = plotLeft + ((xVal - xMin) / xRange) * plotWidth;
      const topY = plotTop + plotHeight - ((topVal - yMin) / yRange) * plotHeight;
      const bottomY = plotTop + plotHeight - ((bottomVal - yMin) / yRange) * plotHeight;
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
