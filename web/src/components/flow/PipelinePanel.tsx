import { useState, useEffect } from "preact/hooks";
import type { PipelineStage } from "../../api/client";
import { StageNode } from "./StageNode";
import { FieldList } from "./FieldList";
import styles from "./flow.module.css";

interface PipelinePanelProps {
  stages: PipelineStage[];
  fieldTypes?: Map<string, string>;
  onInsertCommand?: (template: string) => void;
}

export function PipelinePanel({
  stages,
  fieldTypes,
  onInsertCommand,
}: PipelinePanelProps) {
  const [selectedIndex, setSelectedIndex] = useState(stages.length - 1);

  // Reset to last stage when stages change
  useEffect(() => {
    setSelectedIndex(stages.length - 1);
  }, [stages]);

  if (stages.length === 0) {
    return null;
  }

  const selected = stages[selectedIndex] ?? stages[stages.length - 1];

  return (
    <div class={styles.pipelinePanel}>
      {/* Hero: Fields for selected stage */}
      <div class={styles.pipelineSectionHeader}>
        <span class={styles.sectionTitle}>Fields</span>
        {selected.fields_unknown && (
          <span class={styles.sectionCount}>+ dynamic</span>
        )}
      </div>
      <div class={styles.fieldsHero}>
        <FieldList
          fields={selected.fields_out ?? []}
          fieldsAdded={selected.fields_added}
          fieldTypes={fieldTypes}
          onInsertCommand={onInsertCommand}
        />
        {(!selected.fields_out || selected.fields_out.length === 0) &&
          !selected.fields_unknown && (
            <div class={styles.stageNoFields}>No field info</div>
          )}
      </div>

      {/* Compact stage selector */}
      <div class={styles.pipelineSectionHeader}>
        <span class={styles.sectionTitle}>Pipeline</span>
        <span class={styles.sectionCount}>
          {stages.length} {stages.length === 1 ? "stage" : "stages"}
        </span>
      </div>
      <div class={styles.pipelineTree}>
        {stages.map((stage, i) => (
          <StageNode
            key={i}
            stage={stage}
            isSelected={i === selectedIndex}
            onSelect={() => setSelectedIndex(i)}
          />
        ))}
      </div>
    </div>
  );
}
