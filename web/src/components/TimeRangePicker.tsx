import { useEffect, useCallback, useState } from "react";
import { Clock } from "lucide-react";
import {
  PRESETS,
  getTimeRangeLabel,
  toNowExpr,
  parseNowExpression,
} from "../utils/timeFormat";
import { Button } from "./ui/button";
import { Input } from "./ui/input";
import { Popover, PopoverContent, PopoverTrigger } from "./ui/popover";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "./ui/tabs";

interface TimeRangePickerProps {
  from: string;
  to: string | undefined;
  onFromChange: (value: string) => void;
  onToChange: (value: string | undefined) => void;
  onApply?: () => void;
}

export function TimeRangePicker({
  from,
  to,
  onFromChange,
  onToChange,
  onApply,
}: TimeRangePickerProps) {
  const [open, setOpen] = useState(false);
  const [fromInput, setFromInput] = useState("");
  const [toInput, setToInput] = useState("");
  const [quickSearch, setQuickSearch] = useState("");
  const [validationError, setValidationError] = useState<string | null>(null);

  // Sync inputs when dropdown opens
  useEffect(() => {
    if (open) {
      setFromInput(toNowExpr(from));
      setToInput(toNowExpr(to));
      setQuickSearch("");
      setValidationError(null);
    }
  }, [open, from, to]);

  // Apply absolute/relative inputs from left panel
  const handleApply = useCallback(() => {
    setValidationError(null);

    const parsedFrom = parseNowExpression(fromInput);
    const parsedTo = parseNowExpression(toInput);

    if (parsedFrom === null) {
      // Try as ISO date
      const d = new Date(fromInput);
      if (isNaN(d.getTime())) {
        setValidationError("Invalid From value. Use now-3h or ISO date.");
        return;
      }
      onFromChange(d.toISOString());
    } else if (parsedFrom === undefined) {
      // "now" as from doesn't make sense, but allow it
      setValidationError(
        "From cannot be 'now'. Use a relative offset like now-1h.",
      );
      return;
    } else {
      onFromChange(parsedFrom);
    }

    if (parsedTo === null) {
      const d = new Date(toInput);
      if (isNaN(d.getTime())) {
        setValidationError("Invalid To value. Use now or now-30m or ISO date.");
        return;
      }
      onToChange(d.toISOString());
    } else if (parsedTo === undefined) {
      onToChange(undefined);
    } else {
      onToChange(parsedTo);
    }

    setOpen(false);
    onApply?.();
  }, [onFromChange, onToChange, onApply, fromInput, toInput]);

  // Click a quick-range preset
  const handlePreset = useCallback(
    (value: string) => {
      onFromChange(value);
      onToChange(undefined);
      setOpen(false);
      onApply?.();
    },
    [onFromChange, onToChange, onApply],
  );

  // Filter presets by search
  const filteredPresets = quickSearch
    ? PRESETS.filter((p) =>
        p.label.toLowerCase().includes(quickSearch.toLowerCase()),
      )
    : PRESETS;

  // Determine which preset is active
  const activePreset =
    to === undefined || to === "now"
      ? (PRESETS.find((p) => p.value === from)?.value ?? null)
      : null;

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          className="gap-1.5 shrink-0 whitespace-nowrap text-muted-foreground"
          aria-haspopup="dialog"
        >
          <Clock className="size-3.5" />
          {getTimeRangeLabel(from, to)}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        className="w-[480px] p-0"
        aria-label="Time range picker"
      >
        <Tabs defaultValue="relative" className="gap-0">
          <TabsList className="w-full rounded-none border-b border-border">
            <TabsTrigger value="relative" className="text-xs">
              Relative
            </TabsTrigger>
            <TabsTrigger value="absolute" className="text-xs">
              Absolute
            </TabsTrigger>
          </TabsList>

          {/* Relative presets */}
          <TabsContent value="relative" className="mt-0">
            <div className="flex flex-col py-2">
              <div className="px-3 pb-2">
                <Input
                  type="text"
                  className="h-7 text-xs"
                  placeholder="Search quick ranges"
                  value={quickSearch}
                  onInput={(e) =>
                    setQuickSearch((e.target as HTMLInputElement).value)
                  }
                />
              </div>
              <div className="max-h-[340px] overflow-y-auto">
                {filteredPresets.map((preset) => (
                  <button
                    key={preset.value}
                    type="button"
                    className={`flex w-full items-center px-3 py-1.5 text-left text-[0.8125rem] transition-colors cursor-pointer border-l-[3px] ${
                      activePreset === preset.value
                        ? "border-l-primary text-foreground"
                        : "border-l-transparent text-muted-foreground hover:bg-accent hover:text-foreground"
                    }`}
                    onClick={() => handlePreset(preset.value)}
                  >
                    {preset.label}
                  </button>
                ))}
              </div>
            </div>
          </TabsContent>

          {/* Absolute time range */}
          <TabsContent value="absolute" className="mt-0">
            <div className="flex flex-col gap-3 p-4">
              <div className="flex flex-col gap-1">
                <label htmlFor="trp-from" className="text-[0.6875rem] font-medium text-muted-foreground">From</label>
                <Input
                  id="trp-from"
                  type="text"
                  className="h-8 text-xs font-mono"
                  value={fromInput}
                  placeholder="now-1h"
                  onInput={(e) => {
                    setFromInput((e.target as HTMLInputElement).value);
                    setValidationError(null);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      handleApply();
                    }
                  }}
                />
              </div>

              <div className="flex flex-col gap-1">
                <label htmlFor="trp-to" className="text-[0.6875rem] font-medium text-muted-foreground">To</label>
                <Input
                  id="trp-to"
                  type="text"
                  className="h-8 text-xs font-mono"
                  value={toInput}
                  placeholder="now"
                  onInput={(e) => {
                    setToInput((e.target as HTMLInputElement).value);
                    setValidationError(null);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      handleApply();
                    }
                  }}
                />
              </div>

              {validationError && (
                <p className="text-xs text-destructive -mt-1">{validationError}</p>
              )}

              <Button type="button" size="sm" onClick={handleApply} className="w-full">
                Apply time range
              </Button>
            </div>
          </TabsContent>
        </Tabs>
      </PopoverContent>
    </Popover>
  );
}
