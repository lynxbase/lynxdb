import { useState, useCallback } from "react";
import type { JSX } from "react";
import { toast } from "sonner";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "./ui/tabs";
import { Button } from "./ui/button";

export interface EventDetailInlineProps {
  event: Record<string, unknown>;
  onFilter?: (field: string, value: string, exclude: boolean) => void;
}

// Collapsible JSON tree

interface JsonNodeProps {
  data: unknown;
  depth: number;
  keyName?: string;
}

function JsonNode({ data, depth, keyName }: JsonNodeProps): JSX.Element {
  const [expanded, setExpanded] = useState(depth < 2);

  const toggle = useCallback(() => setExpanded((v) => !v), []);

  const indent = { paddingLeft: `${depth * 16}px` };

  // --- Leaf values ---
  if (data === null) {
    return (
      <div className="whitespace-nowrap" style={indent}>
        {keyName != null && (
          <>
            <span className="text-foreground">&quot;{keyName}&quot;</span>
            <span className="text-muted-foreground">: </span>
          </>
        )}
        <span className="text-muted-foreground">null</span>
      </div>
    );
  }

  if (typeof data === "string") {
    return (
      <div className="whitespace-nowrap" style={indent}>
        {keyName != null && (
          <>
            <span className="text-foreground">&quot;{keyName}&quot;</span>
            <span className="text-muted-foreground">: </span>
          </>
        )}
        <span className="text-syntax-string">&quot;{data}&quot;</span>
      </div>
    );
  }

  if (typeof data === "number") {
    return (
      <div className="whitespace-nowrap" style={indent}>
        {keyName != null && (
          <>
            <span className="text-foreground">&quot;{keyName}&quot;</span>
            <span className="text-muted-foreground">: </span>
          </>
        )}
        <span className="text-syntax-number">{String(data)}</span>
      </div>
    );
  }

  if (typeof data === "boolean") {
    return (
      <div className="whitespace-nowrap" style={indent}>
        {keyName != null && (
          <>
            <span className="text-foreground">&quot;{keyName}&quot;</span>
            <span className="text-muted-foreground">: </span>
          </>
        )}
        <span className="text-syntax-bool">{String(data)}</span>
      </div>
    );
  }

  // --- Arrays ---
  if (Array.isArray(data)) {
    const count = data.length;
    if (count === 0) {
      return (
        <div className="whitespace-nowrap" style={indent}>
          {keyName != null && (
            <>
              <span className="text-foreground">&quot;{keyName}&quot;</span>
              <span className="text-muted-foreground">: </span>
            </>
          )}
          <span className="text-muted-foreground">[]</span>
        </div>
      );
    }

    return (
      <div>
        <div className="whitespace-nowrap" style={indent}>
          <span
            role="button"
            tabIndex={0}
            className="inline-block w-3.5 cursor-pointer select-none text-center text-[0.625rem] text-muted-foreground hover:text-primary mr-0.5"
            onClick={toggle}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } }}
          >
            {expanded ? "▼" : "▶"}
          </span>
          {keyName != null && (
            <>
              <span className="text-foreground">&quot;{keyName}&quot;</span>
              <span className="text-muted-foreground">: </span>
            </>
          )}
          {!expanded && (
            <span className="text-muted-foreground italic text-xs">
              {"["} ...{count} {count === 1 ? "item" : "items"} {"]"}
            </span>
          )}
          {expanded && <span className="text-muted-foreground">{"["}</span>}
        </div>
        {expanded && (
          <div>
            {data.map((item, i) => (
              <JsonNode key={i} data={item} depth={depth + 1} />
            ))}
          </div>
        )}
        {expanded && (
          <div className="whitespace-nowrap" style={indent}>
            <span className="text-muted-foreground">{"]"}</span>
          </div>
        )}
      </div>
    );
  }

  // --- Objects ---
  if (typeof data === "object") {
    const entries = Object.entries(data as Record<string, unknown>);
    const count = entries.length;
    if (count === 0) {
      return (
        <div className="whitespace-nowrap" style={indent}>
          {keyName != null && (
            <>
              <span className="text-foreground">&quot;{keyName}&quot;</span>
              <span className="text-muted-foreground">: </span>
            </>
          )}
          <span className="text-muted-foreground">{"{}"}</span>
        </div>
      );
    }

    return (
      <div>
        <div className="whitespace-nowrap" style={indent}>
          <span
            role="button"
            tabIndex={0}
            className="inline-block w-3.5 cursor-pointer select-none text-center text-[0.625rem] text-muted-foreground hover:text-primary mr-0.5"
            onClick={toggle}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggle(); } }}
          >
            {expanded ? "▼" : "▶"}
          </span>
          {keyName != null && (
            <>
              <span className="text-foreground">&quot;{keyName}&quot;</span>
              <span className="text-muted-foreground">: </span>
            </>
          )}
          {!expanded && (
            <span className="text-muted-foreground italic text-xs">
              {"{"} ...{count} {count === 1 ? "key" : "keys"} {"}"}
            </span>
          )}
          {expanded && <span className="text-muted-foreground">{"{"}</span>}
        </div>
        {expanded && (
          <div>
            {entries.map(([key, val]) => (
              <JsonNode key={key} data={val} depth={depth + 1} keyName={key} />
            ))}
          </div>
        )}
        {expanded && (
          <div className="whitespace-nowrap" style={indent}>
            <span className="text-muted-foreground">{"}"}</span>
          </div>
        )}
      </div>
    );
  }

  // Fallback
  return (
    <div className="whitespace-nowrap" style={indent}>
      {keyName != null && (
        <>
          <span className="text-foreground">&quot;{keyName}&quot;</span>
          <span className="text-muted-foreground">: </span>
        </>
      )}
      <span>{String(data)}</span>
    </div>
  );
}

// Inline event detail accordion (replaces the old slide-out panel)

export function EventDetailInline({ event, onFilter }: EventDetailInlineProps) {
  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(JSON.stringify(event, null, 2)).then(() => {
      toast.success("Copied JSON to clipboard");
    }).catch(() => {
      // clipboard write can fail in non-HTTPS contexts; silently ignore
    });
  }, [event]);

  const entries = Object.entries(event);

  return (
    <div className="flex flex-col max-h-[400px] overflow-hidden border-y border-border bg-secondary">
      <Tabs defaultValue="fields" className="gap-0 flex flex-col flex-1 min-h-0">
        <div className="flex items-center gap-1 px-3 py-1.5 border-b border-border shrink-0">
          <TabsList variant="line" className="h-7 p-0">
            <TabsTrigger value="fields" className="text-xs h-7 px-2">Fields</TabsTrigger>
            <TabsTrigger value="json" className="text-xs h-7 px-2">JSON</TabsTrigger>
          </TabsList>
          <div className="flex-1" />
          <Button variant="outline" size="xs" onClick={handleCopy}>
            Copy JSON
          </Button>
        </div>

        <TabsContent value="fields" className="flex-1 overflow-auto px-3 py-2 m-0">
          <div className="flex flex-col">
            {entries.map(([key, value]) => (
              <div key={key} className="flex items-baseline gap-2 py-1 font-mono text-[0.8125rem] border-b border-muted">
                <span className="text-muted-foreground whitespace-nowrap shrink-0">{key}</span>
                <span className="text-foreground break-all flex-1 min-w-0">
                  {value == null ? "" : String(value)}
                </span>
                <span className="flex gap-0.5 shrink-0">
                  <button
                    type="button"
                    className="inline-flex size-5 items-center justify-center rounded-sm border border-border text-xs font-semibold text-primary hover:bg-primary/10 hover:border-primary cursor-pointer"
                    onClick={() => onFilter?.(key, String(value ?? ""), false)}
                    title={`Filter: ${key}="${value}"`}
                    aria-label={`Include ${key} equals ${value}`}
                  >
                    +
                  </button>
                  <button
                    type="button"
                    className="inline-flex size-5 items-center justify-center rounded-sm border border-border text-xs font-semibold text-destructive hover:bg-destructive/10 hover:border-destructive cursor-pointer"
                    onClick={() => onFilter?.(key, String(value ?? ""), true)}
                    title={`Exclude: ${key}!="${value}"`}
                    aria-label={`Exclude ${key} equals ${value}`}
                  >
                    &minus;
                  </button>
                </span>
              </div>
            ))}
          </div>
        </TabsContent>

        <TabsContent value="json" className="flex-1 overflow-auto px-3 py-2 m-0">
          <div className="font-mono text-[0.8125rem] leading-relaxed">
            <JsonNode data={event} depth={0} />
          </div>
        </TabsContent>
      </Tabs>
    </div>
  );
}
