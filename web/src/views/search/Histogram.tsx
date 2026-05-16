import { Timeline } from "../../components/Timeline";
import { useSearchStore } from "../../stores/search";

interface HistogramProps {
  onBrush: (fromTs: number, toTs: number) => void;
  onReset: () => void;
}

export function Histogram({ onBrush, onReset }: HistogramProps) {
  const from = useSearchStore((s) => s.from);
  const to = useSearchStore((s) => s.to);
  const timelineBuckets = useSearchStore((s) => s.timelineBuckets);
  const groupedBuckets = useSearchStore((s) => s.groupedBuckets);
  const hasQueried = useSearchStore((s) => s.hasQueried);
  const tailActive = useSearchStore((s) => s.tailActive);
  const histogramBrushed = useSearchStore((s) => s.histogramBrushed);

  return (
    <Timeline
      from={from}
      to={to}
      buckets={timelineBuckets}
      groupedBuckets={groupedBuckets}
      visible={hasQueried && !tailActive}
      onBrush={onBrush}
      onReset={onReset}
      showReset={histogramBrushed}
    />
  );
}
