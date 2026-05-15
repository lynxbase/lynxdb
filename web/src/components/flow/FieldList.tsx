import { useState, useCallback } from "react";
import { FieldItem } from "./FieldItem";
import { cn } from "@/lib/utils";

interface FieldListProps {
  fields: string[];
  fieldsAdded?: string[];
  fieldTypes?: Map<string, string>;
  onInsertCommand?: (template: string) => void;
}

export function FieldList({
  fields,
  fieldsAdded,
  fieldTypes,
  onInsertCommand,
}: FieldListProps) {
  const [allExpanded, setAllExpanded] = useState(true);

  const handleToggleAll = useCallback(() => {
    setAllExpanded((prev) => !prev);
  }, []);

  const addedSet = new Set(fieldsAdded ?? []);

  // Split into new fields (added) and remaining fields
  const newFields = fields.filter((f) => addedSet.has(f));
  const defaultFields = fields.filter((f) => !addedSet.has(f));

  if (fields.length === 0) {
    return null;
  }

  return (
    <div className="flex flex-col">
      {newFields.length > 0 && (
        <>
          <div className="flex flex-row items-center justify-between gap-1 py-1 px-2.5 border-none bg-transparent cursor-default w-full text-left font-sans text-[0.625rem] font-semibold uppercase tracking-wider text-muted-foreground">
            New Fields
          </div>
          {newFields.map((name) => (
            <FieldItem
              key={name}
              name={name}
              type={fieldTypes?.get(name)}
              isAdded
              onInsertCommand={onInsertCommand}
            />
          ))}
        </>
      )}

      {defaultFields.length > 0 && (
        <>
          <button
            type="button"
            className="flex flex-row items-center justify-between gap-1 py-1 px-2.5 border-none bg-transparent cursor-pointer w-full text-left font-sans text-[0.625rem] font-semibold uppercase tracking-wider text-muted-foreground hover:text-muted-foreground/80 focus-visible:outline-2 focus-visible:outline-ring"
            onClick={handleToggleAll}
          >
            <span>All Fields ({defaultFields.length})</span>
            <span
              className={cn(
                "shrink-0 size-3 flex items-center justify-center text-[0.5rem] text-muted-foreground transition-transform duration-150 motion-reduce:transition-none",
                allExpanded && "rotate-90",
              )}
              aria-hidden="true"
            >
              &#9656;
            </span>
          </button>
          {allExpanded &&
            defaultFields.map((name) => (
              <FieldItem
                key={name}
                name={name}
                type={fieldTypes?.get(name)}
                onInsertCommand={onInsertCommand}
              />
            ))}
        </>
      )}
    </div>
  );
}
