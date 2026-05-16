import { useState } from "react";
import type { PipelineStage, OptimizerRule } from "../api/client";
import { cn } from "@/lib/utils";

interface PipelineFlowProps {
  stages: PipelineStage[];
  optimizerRules?: OptimizerRule[];
}

/** Maps optimizer rule names to the pipeline stage commands they typically affect. */
const RULE_STAGE_MAP: Record<string, string[]> = {
  predicate_pushdown: ["WHERE", "SEARCH"],
  column_pruning: ["SEARCH", "TABLE", "FIELDS"],
  constant_folding: ["EVAL", "WHERE"],
  bloom_filter_pruning: ["SEARCH"],
  time_range_pruning: ["SEARCH"],
  partial_aggregation: ["STATS"],
  topk_pushdown: ["SORT", "HEAD"],
  regex_literal_extraction: ["REX", "SEARCH"],
  mv_rewrite: ["FROM"],
  cse_elimination: ["EVAL"],
  join_optimization: ["JOIN"],
};

/**
 * Map optimizer rules to stage indexes. Returns a Map where key is the stage
 * index and value is the list of rules that affect that stage. Rules that
 * don't match any stage are omitted (shown in the Optimizer Rules tab instead).
 */
function mapRulesToStages(
  rules: OptimizerRule[],
  stages: PipelineStage[],
): Map<number, OptimizerRule[]> {
  const result = new Map<number, OptimizerRule[]>();

  for (const rule of rules) {
    const targets = RULE_STAGE_MAP[rule.name];
    if (!targets) continue;

    for (let i = 0; i < stages.length; i++) {
      const stage = stages[i];
      if (!stage) continue;
      const cmd = stage.command.toUpperCase();
      if (targets.includes(cmd)) {
        const existing = result.get(i) ?? [];
        existing.push(rule);
        result.set(i, existing);
      }
    }
  }

  return result;
}

export function PipelineFlow({ stages, optimizerRules }: PipelineFlowProps) {
  const [expandedStage, setExpandedStage] = useState<number | null>(null);

  const ruleMap = optimizerRules
    ? mapRulesToStages(optimizerRules, stages)
    : new Map();

  const handleStageClick = (index: number) => {
    setExpandedStage(expandedStage === index ? null : index);
  };

  return (
    <div aria-label="Pipeline stages">
      <div
        className="flex flex-row items-center overflow-x-auto py-3 px-2 relative after:content-[''] after:sticky after:right-0 after:shrink-0 after:w-6 after:h-full after:pointer-events-none after:bg-gradient-to-r after:from-transparent after:to-card"
        role="list"
      >
        {stages.map((stage, i) => {
          const stageRules = ruleMap.get(i);
          const isSelected = expandedStage === i;

          return (
            <div key={i} className="contents" role="listitem">
              {i > 0 && (
                <span className="shrink-0 px-1.5 text-muted-foreground text-sm select-none" aria-hidden="true">
                  {"→"}
                </span>
              )}
              <div
                className={cn(
                  "shrink-0 relative flex flex-col py-1.5 px-2.5 border rounded-sm bg-background cursor-pointer min-w-[80px] transition-colors duration-150 motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-ring focus-visible:outline-offset-1",
                  isSelected
                    ? "border-primary bg-accent"
                    : "border-border hover:border-primary",
                )}
                onClick={() => handleStageClick(i)}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleStageClick(i);
                  }
                }}
                aria-expanded={isSelected}
                aria-label={`Pipeline stage: ${stage.command}`}
              >
                <span className="font-semibold text-xs font-mono uppercase text-foreground">
                  {stage.command}
                </span>
                {stage.description && (
                  <span
                    className="text-[0.6875rem] text-muted-foreground max-w-[150px] overflow-hidden text-ellipsis whitespace-nowrap"
                    title={stage.description}
                  >
                    {stage.description}
                  </span>
                )}
                {stageRules && stageRules.length > 0 && (
                  <span
                    className="absolute -top-1 -right-1 size-2 rounded-full bg-primary border border-background"
                    title={stageRules
                      .map((r: OptimizerRule) => r.name)
                      .join(", ")}
                    aria-label={`Optimizer: ${stageRules.map((r: OptimizerRule) => r.name).join(", ")}`}
                  />
                )}
              </div>
            </div>
          );
        })}
      </div>

      {expandedStage !== null && stages[expandedStage] && (
        <div className="py-2 px-3 border border-border rounded-sm bg-card mt-1 text-xs text-muted-foreground">
          <div>
            <span className="font-semibold text-muted-foreground mr-1">Fields added:</span>
            <span className="font-mono text-[0.6875rem]">
              {stages[expandedStage].fields_added?.length
                ? stages[expandedStage].fields_added!.join(", ")
                : "none"}
            </span>
          </div>
          <div>
            <span className="font-semibold text-muted-foreground mr-1">Fields removed:</span>
            <span className="font-mono text-[0.6875rem]">
              {stages[expandedStage].fields_removed?.length
                ? stages[expandedStage].fields_removed!.join(", ")
                : "none"}
            </span>
          </div>
          <div>
            <span className="font-semibold text-muted-foreground mr-1">Fields out:</span>
            <span className="font-mono text-[0.6875rem]">
              {stages[expandedStage].fields_out?.length
                ? stages[expandedStage].fields_out!.join(", ")
                : "all"}
            </span>
          </div>
        </div>
      )}
    </div>
  );
}
