import { useState } from "react";
import { PipelineFlow } from "./PipelineFlow";
import type { ExplainResult, QueryStats } from "../api/client";
import type { DetailedStats } from "../api/client";
import { formatMs } from "../utils/format";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "./ui/tabs";
import { Badge } from "./ui/badge";

interface ExplainInspectorProps {
  explain: ExplainResult;
  stats?: QueryStats | null;
}

type TabId = "pipeline" | "optimizer" | "scan" | "timing";

/** Format a rule name: replace underscores with spaces, title case. */
function formatRuleName(name: string): string {
  return name.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

export function ExplainInspector({ explain, stats }: ExplainInspectorProps) {
  const [activeTab, setActiveTab] = useState<TabId>("pipeline");

  const parsed = explain.parsed;
  if (!parsed) return null;

  const ds = stats?.stats as DetailedStats | undefined;

  return (
    <div className="mb-1 max-h-[300px] overflow-y-auto rounded-md border border-border bg-secondary animate-in slide-in-from-top-2 duration-150">
      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as TabId)} className="gap-0">
        <TabsList variant="line" className="sticky top-0 z-[1] bg-secondary h-8 rounded-none border-b border-border px-1">
          <TabsTrigger value="pipeline" className="text-xs h-7 px-2">Pipeline</TabsTrigger>
          <TabsTrigger value="optimizer" className="text-xs h-7 px-2">Optimizer Rules</TabsTrigger>
          <TabsTrigger value="scan" className="text-xs h-7 px-2">Scan Plan</TabsTrigger>
          <TabsTrigger value="timing" className="text-xs h-7 px-2">Timing</TabsTrigger>
        </TabsList>

        <TabsContent value="pipeline" className="p-2 m-0">
          <PipelineFlow
            stages={parsed.pipeline}
            optimizerRules={parsed.optimizer_rules}
          />
        </TabsContent>

        <TabsContent value="optimizer" className="p-2 m-0">
          <OptimizerRulesTab rules={parsed.optimizer_rules} />
        </TabsContent>

        <TabsContent value="scan" className="p-2 m-0">
          <ScanPlanTab parsed={parsed} />
        </TabsContent>

        <TabsContent value="timing" className="p-2 m-0">
          <TimingTab parsed={parsed} ds={ds} />
        </TabsContent>
      </Tabs>
    </div>
  );
}

// Optimizer Rules tab

function OptimizerRulesTab({
  rules,
}: {
  rules?: { name: string; description?: string; count: number }[];
}) {
  if (!rules || rules.length === 0) {
    return <div className="text-xs text-muted-foreground py-2">No optimizer rules applied</div>;
  }

  return (
    <div>
      {rules.map((rule) => (
        <div key={rule.name} className="flex items-baseline gap-2 py-1 border-b border-border last:border-0">
          <span className="font-semibold text-xs font-mono text-foreground">{formatRuleName(rule.name)}</span>
          {rule.description && (
            <span className="text-[0.6875rem] text-muted-foreground flex-1">{rule.description}</span>
          )}
          {rule.count > 1 && (
            <Badge variant="secondary" className="text-[0.625rem] px-1 py-0 h-auto text-primary">
              x{rule.count}
            </Badge>
          )}
        </div>
      ))}
    </div>
  );
}

// Scan Plan tab

function ScanPlanTab({
  parsed,
}: {
  parsed: NonNullable<ExplainResult["parsed"]>;
}) {
  const rows: { label: string; value: string }[] = [];

  if (parsed.source_scope) {
    rows.push({ label: "Source scope", value: parsed.source_scope.type });
    if (parsed.source_scope.resolved_sources?.length) {
      rows.push({
        label: "Resolved sources",
        value: parsed.source_scope.resolved_sources.join(", "),
      });
    }
  }

  rows.push({
    label: "Search terms",
    value: parsed.search_terms?.length
      ? parsed.search_terms.join(", ")
      : "none",
  });

  rows.push({
    label: "Time bounds",
    value: parsed.has_time_bounds ? "Yes" : "No",
  });

  rows.push({
    label: "Full scan",
    value: parsed.uses_full_scan ? "Yes" : "No",
  });

  rows.push({
    label: "Fields read",
    value: parsed.fields_read?.length ? parsed.fields_read.join(", ") : "all",
  });

  if (parsed.physical_plan) {
    const pp = parsed.physical_plan;
    const flags: string[] = [];
    if (pp.count_star_only) flags.push("count(*) optimized");
    if (pp.partial_agg) flags.push("partial aggregation");
    if (pp.topk_agg) flags.push(`TopK (k=${pp.topk ?? "?"})`);
    if (pp.join_strategy) flags.push(`join: ${pp.join_strategy}`);
    if (flags.length > 0) {
      rows.push({ label: "Physical plan", value: flags.join(", ") });
    }
  }

  return (
    <div>
      {rows.map((row) => (
        <div key={row.label} className="flex gap-2 py-1 text-xs">
          <span className="font-semibold text-muted-foreground min-w-[6.25rem] shrink-0">{row.label}</span>
          <span className="text-muted-foreground font-mono text-[0.6875rem]">{row.value}</span>
        </div>
      ))}
    </div>
  );
}

// Timing tab

function TimingTab({
  parsed,
  ds,
}: {
  parsed: NonNullable<ExplainResult["parsed"]>;
  ds?: DetailedStats;
}) {
  const entries: { label: string; ms: number }[] = [];

  if (parsed.parse_ms != null)
    entries.push({ label: "Parse", ms: parsed.parse_ms });
  if (parsed.optimize_ms != null)
    entries.push({ label: "Optimize", ms: parsed.optimize_ms });
  if (ds?.scan_ms != null) entries.push({ label: "Scan", ms: ds.scan_ms });
  if (ds?.pipeline_ms != null)
    entries.push({ label: "Pipeline", ms: ds.pipeline_ms });

  if (entries.length === 0) {
    return <div className="text-xs text-muted-foreground py-2">Timing data not available</div>;
  }

  const maxMs = Math.max(...entries.map((e) => e.ms), 1);

  return (
    <div>
      {entries.map((entry) => {
        const widthPercent = Math.max((entry.ms / maxMs) * 100, 2);
        return (
          <div key={entry.label} className="flex items-center gap-2 py-1">
            <span className="text-xs text-muted-foreground min-w-[4.375rem]">{entry.label}</span>
            <div
              className="h-1.5 rounded-full bg-primary transition-[width] duration-300"
              style={{ width: `${widthPercent}%` }}
            />
            <span className="text-[0.6875rem] text-muted-foreground font-mono">{formatMs(entry.ms)}</span>
          </div>
        );
      })}
    </div>
  );
}
