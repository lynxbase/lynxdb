import { useState, useCallback } from "preact/hooks";
import type { IndexInfo, ViewSummary } from "../../api/client";
import styles from "./flow.module.css";

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
      <div class={styles.sourcesPanel}>
        <div class={styles.sectionHeader}>
          <span class={styles.sectionTitle}>Sources</span>
        </div>
        <div class={styles.sourcesEmpty}>No sources available</div>
      </div>
    );
  }

  return (
    <div class={styles.sourcesPanel}>
      <button
        type="button"
        class={styles.sectionHeader}
        onClick={handleToggle}
        aria-expanded={expanded}
      >
        <span
          class={`${styles.sectionChevron} ${expanded ? styles.sectionChevronExpanded : ""}`}
          aria-hidden="true"
        >
          &#9656;
        </span>
        <span class={styles.sectionTitle}>Sources</span>
        <span class={styles.sectionCount}>
          {indexes.length + views.length}
        </span>
      </button>

      {expanded && (
        <div class={styles.sourceList}>
          {indexes.map((idx) => (
            <button
              key={idx.name}
              type="button"
              class={styles.sourceItem}
              onClick={() => onSelectSource?.(idx.name)}
              title={`Query index: ${idx.name}`}
            >
              <span class={styles.sourceIcon} aria-hidden="true">
                &#9632;
              </span>
              <span class={styles.sourceName}>{idx.name}</span>
            </button>
          ))}
          {views.map((view) => (
            <button
              key={view.name}
              type="button"
              class={styles.sourceItem}
              onClick={() => onSelectSource?.(view.name)}
              title={`Query view: ${view.name} (${view.status})`}
            >
              <span class={styles.sourceIconView} aria-hidden="true">
                &#9670;
              </span>
              <span class={styles.sourceName}>{view.name}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
