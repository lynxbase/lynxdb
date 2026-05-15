import { useState, useEffect } from "react";
import type { PipelineStage } from "../../api/client";
import { StageNode } from "./StageNode";
import { FieldList } from "./FieldList";

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

  if (!selected) return null;

  return (
    <div className="flex flex-col flex-1 min-h-0">
      {/* Hero: Fields for selected stage */}
      <div className="flex flex-row items-center gap-1 py-1.5 px-2.5 shrink-0">
        <span className="text-[0.6875rem] font-semibold uppercase tracking-wider text-muted-foreground flex-1">
          Fields
        </span>
        {selected.fields_unknown && (
          <span className="shrink-0 text-[0.625rem] text-muted-foreground">+ dynamic</span>
        )}
      </div>
      <div className="overflow-y-auto flex-1 min-h-0 pb-1 border-b border-border">
        <FieldList
          fields={selected.fields_out ?? []}
          fieldsAdded={selected.fields_added}
          fieldTypes={fieldTypes}
          onInsertCommand={onInsertCommand}
        />
        {(!selected.fields_out || selected.fields_out.length === 0) &&
          !selected.fields_unknown && (
            <div className="text-[0.6875rem] text-muted-foreground py-2 px-2.5">
              No field info
            </div>
          )}
      </div>

      {/* Compact stage selector */}
      <div className="flex flex-row items-center gap-1 py-1.5 px-2.5 shrink-0">
        <span className="text-[0.6875rem] font-semibold uppercase tracking-wider text-muted-foreground flex-1">
          Pipeline
        </span>
        <span className="shrink-0 text-[0.625rem] text-muted-foreground">
          {stages.length} {stages.length === 1 ? "stage" : "stages"}
        </span>
      </div>
      <div className="flex flex-col gap-px overflow-y-auto pb-2">
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
