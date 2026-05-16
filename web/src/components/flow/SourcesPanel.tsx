import { useState, useCallback } from "react";
import type { IndexInfo, ViewSummary } from "../../api/client";
import { cn } from "@/lib/utils";

interface SourcesPanelProps {
  indexes: IndexInfo[];
  views: ViewSummary[];
  onSelectSource?: (name: string) => void;
}

export function SourcesPanel({
  indexes,
  views,
  onSelectSource,
}: SourcesPanelProps) {
  const [expanded, setExpanded] = useState(true);

  const handleToggle = useCallback(() => {
    setExpanded((prev) => !prev);
  }, []);

  const hasContent = indexes.length > 0 || views.length > 0;

  if (!hasContent) {
    return (
      <div className="flex flex-col border-b border-border pb-1">
        <div className="flex flex-row items-center gap-1 py-1.5 px-2.5">
          <span className="text-[0.6875rem] font-semibold uppercase tracking-wider text-muted-foreground flex-1">
            Sources
          </span>
        </div>
        <div className="py-4 px-2.5 text-muted-foreground text-xs text-center">
          No sources available
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col border-b border-border pb-1">
      <button
        type="button"
        className="flex flex-row items-center gap-1 py-1.5 px-2.5 border-none bg-transparent cursor-pointer w-full text-left font-sans transition-colors duration-75 motion-reduce:transition-none hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring"
        onClick={handleToggle}
        aria-expanded={expanded}
      >
        <span
          className={cn(
            "shrink-0 size-3 flex items-center justify-center text-[0.5rem] text-muted-foreground transition-transform duration-150 motion-reduce:transition-none",
            expanded && "rotate-90",
          )}
          aria-hidden="true"
        >
          &#9656;
        </span>
        <span className="text-[0.6875rem] font-semibold uppercase tracking-wider text-muted-foreground flex-1">
          Sources
        </span>
        <span className="shrink-0 text-[0.625rem] text-muted-foreground tabular-nums">
          {indexes.length + views.length}
        </span>
      </button>

      {expanded && (
        <div className="flex flex-col">
          {indexes.map((idx) => (
            <button
              key={idx.name}
              type="button"
              className="flex flex-row items-center gap-1.5 py-0.5 px-2.5 pl-5 border-none bg-transparent cursor-pointer w-full text-left font-sans transition-colors duration-75 motion-reduce:transition-none hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring"
              onClick={() => onSelectSource?.(idx.name)}
              title={`Query index: ${idx.name}`}
            >
              <span className="shrink-0 text-[0.5rem] text-primary" aria-hidden="true">
                &#9632;
              </span>
              <span className="flex-1 text-[0.8125rem] text-foreground overflow-hidden text-ellipsis whitespace-nowrap">
                {idx.name}
              </span>
            </button>
          ))}
          {views.map((view) => (
            <button
              key={view.name}
              type="button"
              className="flex flex-row items-center gap-1.5 py-0.5 px-2.5 pl-5 border-none bg-transparent cursor-pointer w-full text-left font-sans transition-colors duration-75 motion-reduce:transition-none hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring"
              onClick={() => onSelectSource?.(view.name)}
              title={`Query view: ${view.name} (${view.status})`}
            >
              <span className="shrink-0 text-[0.5rem] text-chart-5" aria-hidden="true">
                &#9670;
              </span>
              <span className="flex-1 text-[0.8125rem] text-foreground overflow-hidden text-ellipsis whitespace-nowrap">
                {view.name}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
