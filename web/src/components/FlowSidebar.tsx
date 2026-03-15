import type {
  IndexInfo,
  ViewSummary,
  ExplainResult,
  FieldInfo,
} from "../api/client";
import { SourcesPanel } from "./flow/SourcesPanel";
import { PipelinePanel } from "./flow/PipelinePanel";
import { FieldsPanel } from "./FieldsPanel";
import styles from "./FlowSidebar.module.css";

interface FlowSidebarProps {
  visible: boolean;
  indexes: IndexInfo[];
  views: ViewSummary[];
  explainResult: ExplainResult | null;
  fieldTypes?: Map<string, string>;
  selectedFields?: string[];
  catalogFields?: FieldInfo[];
  onFilter?: (field: string, value: string, exclude: boolean) => void;
  onToggle: () => void;
  onSelectSource?: (name: string) => void;
  onInsertCommand?: (template: string) => void;
}

export function FlowSidebar({
  visible,
  indexes,
  views,
  explainResult,
  fieldTypes,
  selectedFields,
  catalogFields,
  onFilter,
  onToggle,
  onSelectSource,
  onInsertCommand,
}: FlowSidebarProps) {
  if (!visible) {
    return (
      <button
        type="button"
        class={styles.collapsedToggle}
        onClick={onToggle}
        aria-label="Show flow sidebar"
        title="Show flow sidebar"
      >
        &#9656;
      </button>
    );
  }

  const pipeline = explainResult?.parsed?.pipeline ?? [];

  return (
    <aside class={styles.sidebar} aria-label="Flow">
      <button
        type="button"
        class={styles.toggleBtn}
        onClick={onToggle}
        aria-label="Hide flow sidebar"
        title="Hide flow sidebar"
      >
        &#9666;
      </button>

      <div class={styles.content}>
        <SourcesPanel
          indexes={indexes}
          views={views}
          onSelectSource={onSelectSource}
        />

        {pipeline.length > 0 && (
          <PipelinePanel
            stages={pipeline}
            fieldTypes={fieldTypes}
            onInsertCommand={onInsertCommand}
          />
        )}

        {pipeline.length === 0 && (
          <div class={styles.emptyPipeline}>
            Run a query to see the pipeline
          </div>
        )}

        <div class={styles.fieldsDivider} />

        <FieldsPanel
          selectedFields={selectedFields ?? []}
          catalogFields={catalogFields ?? []}
          onFilter={onFilter}
        />
      </div>
    </aside>
  );
}
