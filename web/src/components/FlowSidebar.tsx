import { useState, useEffect } from "react";
import { PanelRight } from "lucide-react";
import type {
  IndexInfo,
  ViewSummary,
  ExplainResult,
  FieldInfo,
} from "../api/client";
import { formatShortcut, SHORTCUTS } from "../utils/keyboard";
import { SourcesPanel } from "./flow/SourcesPanel";
import { PipelinePanel } from "./flow/PipelinePanel";
import { FieldsPanel } from "./FieldsPanel";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "./ui/sheet";
import { Button } from "./ui/button";
import { Separator } from "./ui/separator";

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

/**
 * Responsive breakpoint hook. Returns "desktop" (>=1280), "tablet" (1024-1279),
 * or "mobile" (<1024).
 */
function useBreakpoint(): "desktop" | "tablet" | "mobile" {
  const [bp, setBp] = useState<"desktop" | "tablet" | "mobile">(() => {
    if (typeof window === "undefined") return "desktop";
    if (window.innerWidth >= 1280) return "desktop";
    if (window.innerWidth >= 1024) return "tablet";
    return "mobile";
  });

  useEffect(() => {
    const desktopMql = window.matchMedia("(min-width: 1280px)");
    const tabletMql = window.matchMedia("(min-width: 1024px)");

    const update = () => {
      if (desktopMql.matches) setBp("desktop");
      else if (tabletMql.matches) setBp("tablet");
      else setBp("mobile");
    };

    desktopMql.addEventListener("change", update);
    tabletMql.addEventListener("change", update);
    update();

    return () => {
      desktopMql.removeEventListener("change", update);
      tabletMql.removeEventListener("change", update);
    };
  }, []);

  return bp;
}

/** The inner content shared between docked sidebar and sheet */
function FlowContent({
  indexes,
  views,
  explainResult,
  fieldTypes,
  selectedFields,
  catalogFields,
  onFilter,
  onSelectSource,
  onInsertCommand,
}: Omit<FlowSidebarProps, "visible" | "onToggle">) {
  const pipeline = explainResult?.parsed?.pipeline ?? [];

  return (
    <div className="flex flex-col flex-1 overflow-y-auto min-h-0">
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
        <div className="flex items-center justify-center p-8 text-muted-foreground text-[0.8125rem] text-center">
          Run a query to see the pipeline
        </div>
      )}

      <Separator className="mx-3 my-1 w-auto" />

      <FieldsPanel
        selectedFields={selectedFields ?? []}
        catalogFields={catalogFields ?? []}
        onFilter={onFilter}
      />
    </div>
  );
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
  const breakpoint = useBreakpoint();

  const contentProps = {
    indexes,
    views,
    explainResult,
    fieldTypes,
    selectedFields,
    catalogFields,
    onFilter,
    onSelectSource,
    onInsertCommand,
  };

  // Desktop (>=1280): docked panel
  if (breakpoint === "desktop") {
    if (!visible) {
      return (
        <button
          type="button"
          className="absolute top-2 right-0 z-[2] flex items-center justify-center size-6 border border-border rounded-l-sm bg-card text-muted-foreground text-xs cursor-pointer transition-colors duration-150 motion-reduce:transition-none hover:text-foreground hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
          onClick={onToggle}
          aria-label="Show flow sidebar"
          title={`Show flow sidebar (${formatShortcut(SHORTCUTS.toggleSidebar)})`}
        >
          &#9666;
        </button>
      );
    }

    return (
      <aside
        className="flex flex-col w-[280px] shrink-0 bg-card border-l border-border overflow-hidden relative"
        aria-label="Flow"
      >
        <button
          type="button"
          className="absolute top-2 left-[-24px] z-[2] flex items-center justify-center size-6 border border-border rounded-l-sm bg-card text-muted-foreground text-xs cursor-pointer transition-colors duration-150 motion-reduce:transition-none hover:text-foreground hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
          onClick={onToggle}
          aria-label="Hide flow sidebar"
          title={`Hide flow sidebar (${formatShortcut(SHORTCUTS.toggleSidebar)})`}
        >
          &#9656;
        </button>

        <FlowContent {...contentProps} />
      </aside>
    );
  }

  // Tablet (1024-1279) and Mobile (<1024): Sheet trigger
  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button
          variant="ghost"
          size="icon-sm"
          className="shrink-0"
          aria-label="Open flow panel"
          title={`Flow panel (${formatShortcut(SHORTCUTS.toggleSidebar)})`}
        >
          <PanelRight className="size-4" />
        </Button>
      </SheetTrigger>
      <SheetContent side="right" className="w-[300px] sm:max-w-[300px] p-0 gap-0">
        <SheetHeader className="px-3 py-2 border-b border-border">
          <SheetTitle className="text-sm">Flow</SheetTitle>
        </SheetHeader>
        <FlowContent {...contentProps} />
      </SheetContent>
    </Sheet>
  );
}
