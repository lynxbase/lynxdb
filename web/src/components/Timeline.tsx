import { useRef, useEffect, useCallback, useState } from "preact/hooks";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import type { HistogramBucket } from "../api/client";
import styles from "./Timeline.module.css";

interface TimelineProps {
  from: string;
  to?: string;
  buckets: HistogramBucket[];
  visible: boolean;
  onBrush?: (from: number, to: number) => void;
}

/** Read a CSS custom property value from :root */
function cssVar(name: string): string {
  return getComputedStyle(document.documentElement)
    .getPropertyValue(name)
    .trim();
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
 * Format an epoch-seconds timestamp for the tooltip.
 * Shows "HH:mm" for intra-day ranges, "MMM DD HH:mm" otherwise.
 */
function formatTooltipTime(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const mon = d.toLocaleString("en-US", { month: "short" });
  const dd = String(d.getDate()).padStart(2, "0");
  return `${mon} ${dd} ${hh}:${mm}`;
}

export function Timeline({ buckets, visible, onBrush }: TimelineProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<uPlot | null>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const [tooltipVisible, setTooltipVisible] = useState(false);
  const [tooltipContent, setTooltipContent] = useState({ time: "", count: "" });
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });

  const handleCursorMove = useCallback(
    (u: uPlot) => {
      const idx = u.cursor.idx;
      if (idx == null || idx < 0 || !u.data[0] || idx >= u.data[0].length) {
        setTooltipVisible(false);
        return;
      }

      const ts = u.data[0][idx];
      const count = u.data[1][idx];
      const left = u.cursor.left ?? 0;
      const top = u.cursor.top ?? 0;

      setTooltipContent({
        time: formatTooltipTime(ts),
        count: String(count ?? 0),
      });
      setTooltipPos({ x: left + 10, y: top - 10 });
      setTooltipVisible(true);
    },
    [],
  );

  // Create / recreate chart when buckets change
  useEffect(() => {
    const el = containerRef.current;
    if (!el || buckets.length === 0) {
      // Destroy existing chart if data is empty
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
      return;
    }

    const data = toUPlotData(buckets);

    // Resolve colors from CSS vars
    const accentColor = cssVar("--accent") || "#4F46E5";
    const borderColor = cssVar("--border") || "#e5e7eb";
    const textMuted = cssVar("--text-muted") || "#9ca3af";

    // Compute bar width from data spacing
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
        setCursor: [handleCursorMove],
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
            // Clear selection visual
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
          font: "10px sans-serif",
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
        y: { range: (_u: uPlot, min: number, max: number) => [0, max * 1.1 || 1] },
      },
      series: [
        {},
        {
          label: "Events",
          fill: accentColor + "66", // 40% opacity
          stroke: accentColor,
          width: 1,
          paths: barsPaths(barWidthFactor),
        },
      ],
    };

    // If chart already exists, just update data + size
    if (chartRef.current) {
      chartRef.current.destroy();
    }

    chartRef.current = new uPlot(opts, data, el);

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [buckets, onBrush]);

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

  return (
    <div class={styles.container} ref={containerRef}>
      {buckets.length === 0 && (
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
        <span class={styles.tooltipTime}>{tooltipContent.time}</span>
        <span class={styles.tooltipCount}>{tooltipContent.count}</span>
      </div>
    </div>
  );
}

/**
 * Custom bars path builder for uPlot.
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

    // Calculate bar width in CSS pixels from data spacing
    const dataSpacing = xData.length > 1 ? xData[1] - xData[0] : 60;
    const xMin = xScale.min ?? xData[0];
    const xMax = xScale.max ?? xData[xData.length - 1];
    const plotWidth = u.bbox.width / devicePixelRatio;
    const xRange = xMax - xMin || 1;
    const barWidthPx = Math.max(1, (dataSpacing / xRange) * plotWidth * widthFactor);

    const fillPath = new Path2D();
    const strokePath = new Path2D();

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
      flags: 3, // BAND_CLIP_FILL | BAND_CLIP_STROKE
    };
  };
}
