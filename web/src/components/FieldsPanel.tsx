import { useState, useMemo, useCallback, useRef } from "preact/hooks";
import type { FieldInfo } from "../api/client";
import { FieldValuePopover } from "./FieldValuePopover";
import styles from "./FieldsPanel.module.css";

interface FieldsPanelProps {
  selectedFields: string[];
  catalogFields: FieldInfo[];
  onFilter?: (field: string, value: string, exclude: boolean) => void;
}

function typeAbbrev(t?: string): string {
  if (!t) return "";
  switch (t.toLowerCase()) {
    case "string":
      return "str";
    case "integer":
    case "int":
      return "int";
    case "float":
    case "number":
      return "flt";
    case "boolean":
    case "bool":
      return "bool";
    case "datetime":
    case "timestamp":
      return "ts";
    default:
      return t.slice(0, 3);
  }
}

function typeBadgeClass(abbrev: string): string {
  switch (abbrev) {
    case "str":
      return styles.typeBadgeStr;
    case "int":
      return styles.typeBadgeInt;
    case "flt":
      return styles.typeBadgeFlt;
    case "ts":
      return styles.typeBadgeTs;
    case "bool":
      return styles.typeBadgeBool;
    default:
      return styles.typeBadgeStr;
  }
}

export function FieldsPanel({
  selectedFields,
  catalogFields,
  onFilter,
}: FieldsPanelProps) {
  const [search, setSearch] = useState("");
  const [selectedExpanded, setSelectedExpanded] = useState(true);
  const [availableExpanded, setAvailableExpanded] = useState(true);
  const [popoverField, setPopoverField] = useState<string | null>(null);
  const [popoverAnchor, setPopoverAnchor] = useState<DOMRect | null>(null);
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const handleSearchChange = useCallback((e: Event) => {
    const value = (e.target as HTMLInputElement).value;
    setSearch(value);
    clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      setDebouncedSearch(value);
    }, 150);
  }, []);

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
    const fields = selectedFields.filter((name) =>
      !searchLower || name.toLowerCase().includes(searchLower)
    );
    return fields;
  }, [selectedFields, searchLower]);

  // Available = catalog fields NOT in selectedFields, filtered by search, sorted alphabetically
  const filteredAvailable = useMemo(() => {
    return catalogFields
      .filter((f) =>
        !selectedSet.has(f.name) &&
        (!searchLower || f.name.toLowerCase().includes(searchLower))
      )
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [catalogFields, selectedSet, searchLower]);

  const handleFieldClick = useCallback((fieldName: string, e: MouseEvent) => {
    const target = e.currentTarget as HTMLElement;
    const rect = target.getBoundingClientRect();
    if (popoverField === fieldName) {
      setPopoverField(null);
      setPopoverAnchor(null);
    } else {
      setPopoverField(fieldName);
      setPopoverAnchor(rect);
    }
  }, [popoverField]);

  const handlePopoverClose = useCallback(() => {
    setPopoverField(null);
    setPopoverAnchor(null);
  }, []);

  const handlePopoverFilter = useCallback((field: string, value: string, exclude: boolean) => {
    onFilter?.(field, value, exclude);
    setPopoverField(null);
    setPopoverAnchor(null);
  }, [onFilter]);

  function renderFieldRow(fieldName: string) {
    const catalog = catalogMap.get(fieldName);
    const abbrev = typeAbbrev(catalog?.type);

    return (
      <div class={styles.fieldRow} key={fieldName}>
        <button
          type="button"
          class={styles.fieldName}
          onClick={(e: MouseEvent) => handleFieldClick(fieldName, e)}
          title={fieldName}
        >
          {fieldName}
        </button>
        {abbrev && (
          <span class={`${styles.typeBadge} ${typeBadgeClass(abbrev)}`}>
            {abbrev}
          </span>
        )}
        {catalog && catalog.coverage > 0 && (
          <span class={styles.coverage}>{catalog.coverage}%</span>
        )}
      </div>
    );
  }

  return (
    <div class={styles.fieldsPanel}>
      <input
        type="text"
        class={styles.searchInput}
        placeholder="Filter fields..."
        value={search}
        onInput={handleSearchChange}
      />

      {/* Selected Fields */}
      <div class={!selectedExpanded ? styles.sectionCollapsed : undefined}>
        <button
          type="button"
          class={styles.sectionHeader}
          onClick={() => setSelectedExpanded(!selectedExpanded)}
        >
          <span class={styles.sectionChevron}>&#9662;</span>
          Selected Fields ({filteredSelected.length})
        </button>
        {selectedExpanded && (
          filteredSelected.length > 0
            ? filteredSelected.map((name) => renderFieldRow(name))
            : <div class={styles.emptyFields}>No selected fields</div>
        )}
      </div>

      <div class={styles.divider} />

      {/* Available Fields */}
      <div class={!availableExpanded ? styles.sectionCollapsed : undefined}>
        <button
          type="button"
          class={styles.sectionHeader}
          onClick={() => setAvailableExpanded(!availableExpanded)}
        >
          <span class={styles.sectionChevron}>&#9662;</span>
          Available Fields ({filteredAvailable.length})
        </button>
        {availableExpanded && (
          filteredAvailable.length > 0
            ? filteredAvailable.map((f) => renderFieldRow(f.name))
            : <div class={styles.emptyFields}>No available fields</div>
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
