import styles from "./flow.module.css";
import type { PipelineStage } from "../../api/client";

interface StageNodeProps {
  stage: PipelineStage;
  isSelected: boolean;
  onSelect: () => void;
}

export function StageNode({ stage, isSelected, onSelect }: StageNodeProps) {
  const description = stage.description || "";

  return (
    <button
      type="button"
      class={`${styles.stageCompact} ${isSelected ? styles.stageCompactSelected : ""}`}
      onClick={onSelect}
    >
      <span class={styles.stageCompactCmd}>{stage.command}</span>
      {description && (
        <span class={styles.stageCompactDesc} title={description}>
          {description}
        </span>
      )}
    </button>
  );
}
