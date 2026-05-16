import React, {
  useState,
  useMemo,
  useCallback,
  useRef,
  useEffect,
} from "react";
import type { FieldInfo } from "../api/client";
import { FieldValuePopover } from "./FieldValuePopover";
import { typeAbbrev } from "../utils/fieldType";
import { Input } from "./ui/input";
import { Badge } from "./ui/badge";
import { Skeleton } from "./ui/skeleton";

interface FieldsPanelProps {
  selectedFields: string[];
  catalogFields: FieldInfo[];
  onFilter?: (field: string, value: string, exclude: boolean) => void;
  /** When true, show skeleton loading state */
  isLoading?: boolean;
}

type TypeAbbrev = "str" | "int" | "flt" | "ts" | "bool";

function typeBadgeVariant(abbrev: string): string {
  switch (abbrev as TypeAbbrev) {
    case "str":
      return "bg-[#5794f2]/10 text-[#5794f2]";
    case "int":
      return "bg-[#73bf69]/10 text-[#73bf69]";
    case "flt":
      return "bg-[#ff9830]/10 text-[#ff9830]";
    case "ts":
      return "bg-[#b877d9]/10 text-[#b877d9]";
    case "bool":
      return "bg-[#f2495c]/10 text-[#f2495c]";
    default:
      return "bg-[#5794f2]/10 text-[#5794f2]";
  }
}

export function FieldsPanel({
  selectedFields,
  catalogFields,
  onFilter,
  isLoading,
}: FieldsPanelProps) {
  const [search, setSearch] = useState("");
  const [popoverField, setPopoverField] = useState<string | null>(null);
  const [popoverAnchor, setPopoverAnchor] = useState<DOMRect | null>(null);
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const handleSearchChange = useCallback((e: React.FormEvent<HTMLInputElement>) => {
    const value = (e.target as HTMLInputElement).value;
    setSearch(value);
    clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      setDebouncedSearch(value);
    }, 150);
  }, []);

  // Clear the pending debounce timer on unmount so it cannot fire against
  // an unmounted component.
  useEffect(() => () => clearTimeout(searchTimerRef.current), []);

  // Build a lookup map from catalog fields
  const catalogMap = useMemo(() => {
    const m = new Map<string, FieldInfo>();
    for (const f of catalogFields) {
      m.set(f.name, f);
    }
    return m;
  }, [catalogFields]);

  // Selected fields set for O(1) lookup
  const selectedSet = useMemo(() => new Set(selectedFields), [selectedFields]);

  // Filter by search term
  const searchLower = debouncedSearch.toLowerCase();

  const filteredSelected = useMemo(() => {
    const fields = selectedFields.filter(
      (name) => !searchLower || name.toLowerCase().includes(searchLower),
    );
    return fields;
  }, [selectedFields, searchLower]);

  // Available = catalog fields NOT in selectedFields, filtered by search, sorted alphabetically
  const filteredAvailable = useMemo(() => {
    return catalogFields
      .filter(
        (f) =>
          !selectedSet.has(f.name) &&
          (!searchLower || f.name.toLowerCase().includes(searchLower)),
      )
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [catalogFields, selectedSet, searchLower]);

  const handleFieldClick = useCallback(
    (fieldName: string, e: React.MouseEvent) => {
      const target = e.currentTarget as HTMLElement;
      const rect = target.getBoundingClientRect();
      if (popoverField === fieldName) {
        setPopoverField(null);
        setPopoverAnchor(null);
      } else {
        setPopoverField(fieldName);
        setPopoverAnchor(rect);
      }
    },
    [popoverField],
  );

  const handlePopoverClose = useCallback(() => {
    setPopoverField(null);
    setPopoverAnchor(null);
  }, []);

  const handlePopoverFilter = useCallback(
    (field: string, value: string, exclude: boolean) => {
      onFilter?.(field, value, exclude);
      setPopoverField(null);
      setPopoverAnchor(null);
    },
    [onFilter],
  );

  function renderFieldRow(fieldName: string) {
    const catalog = catalogMap.get(fieldName);
    const abbrev = typeAbbrev(catalog?.type);

    return (
      <div className="flex items-center gap-1.5 py-0.5 px-0 rounded-sm hover:bg-accent cursor-pointer" key={fieldName}>
        <button
          type="button"
          className="flex-1 min-w-0 truncate bg-transparent border-none cursor-pointer text-left p-0 font-mono text-xs text-foreground hover:text-primary"
          onClick={(e: React.MouseEvent) => handleFieldClick(fieldName, e)}
          title={fieldName}
        >
          {fieldName}
        </button>
        {abbrev && (
          <Badge
            variant="ghost"
            className={`text-[0.625rem] px-1 py-0 h-auto rounded-sm font-mono ${typeBadgeVariant(abbrev)}`}
          >
            {abbrev}
          </Badge>
        )}
        {catalog && catalog.coverage > 0 && (
          <span className="shrink-0 text-[0.625rem] tabular-nums text-muted-foreground">
            {Math.round(catalog.coverage)}%
          </span>
        )}
      </div>
    );
  }

  // Loading skeleton state
  if (isLoading) {
    return (
      <div className="px-2.5 py-1 text-[0.8125rem]">
        <Skeleton className="h-6 w-full mb-2" />
        <div className="flex flex-col gap-1.5">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-4 w-full" />
          ))}
        </div>
      </div>
    );
  }

  // Empty placeholder
  if (catalogFields.length === 0 && selectedFields.length === 0) {
    return (
      <div className="px-2.5 py-4 text-center text-xs text-muted-foreground">
        Run a query to populate fields
      </div>
    );
  }

  return (
    <div className="px-2.5 py-1 text-[0.8125rem]">
      <Input
        type="text"
        className="h-6 text-xs mb-1"
        placeholder="Filter fields..."
        value={search}
        onInput={handleSearchChange}
      />

      {/* Selected Fields */}
      <div>
        <div className="flex items-center gap-1 py-1 font-medium text-xs text-muted-foreground select-none">
          Selected Fields ({filteredSelected.length})
        </div>
        {filteredSelected.length > 0 ? (
          filteredSelected.map((name) => renderFieldRow(name))
        ) : (
          <div className="text-xs text-muted-foreground py-2 text-center">No selected fields</div>
        )}
      </div>

      <div className="h-px bg-border my-1.5" />

      {/* Available Fields */}
      <div>
        <div className="flex items-center gap-1 py-1 font-medium text-xs text-muted-foreground select-none">
          Available Fields ({filteredAvailable.length})
        </div>
        {filteredAvailable.length > 0 ? (
          filteredAvailable.map((f) => renderFieldRow(f.name))
        ) : (
          <div className="text-xs text-muted-foreground py-2 text-center">No available fields</div>
        )}
      </div>

      {/* Field value popover */}
      {popoverField && popoverAnchor && (
        <FieldValuePopover
          fieldName={popoverField}
          anchorRect={popoverAnchor}
          onFilter={handlePopoverFilter}
          onClose={handlePopoverClose}
        />
      )}
    </div>
  );
}
