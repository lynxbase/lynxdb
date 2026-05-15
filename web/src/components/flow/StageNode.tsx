import type { PipelineStage } from "../../api/client";
import { cn } from "@/lib/utils";

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
      className={cn(
        "flex flex-row items-center gap-1.5 py-1 px-2.5 border-none bg-transparent cursor-pointer w-full text-left font-sans transition-colors duration-75 motion-reduce:transition-none hover:bg-muted/50 focus-visible:outline-2 focus-visible:outline-ring focus-visible:outline-offset-[-2px]",
        isSelected && "bg-muted",
      )}
      onClick={onSelect}
    >
      <span className="shrink-0 text-xs font-semibold font-mono text-primary">
        {stage.command}
      </span>
      {description && (
        <span
          className="flex-1 text-[0.6875rem] text-muted-foreground overflow-hidden text-ellipsis whitespace-nowrap"
          title={description}
        >
          {description}
        </span>
      )}
    </button>
  );
}
