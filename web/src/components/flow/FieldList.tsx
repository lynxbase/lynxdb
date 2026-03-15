import { useState, useCallback } from "preact/hooks";
import { FieldItem } from "./FieldItem";
import styles from "./flow.module.css";

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
  const [allExpanded, setAllExpanded] = useState(false);

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
    <div class={styles.fieldList}>
      {newFields.length > 0 && (
        <>
          <div class={styles.fieldsSectionHeader}>New Fields</div>
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
            class={styles.fieldsSectionHeader}
            onClick={handleToggleAll}
          >
            <span>All Fields ({defaultFields.length})</span>
            <span
              class={`${styles.sectionChevron} ${allExpanded ? styles.sectionChevronExpanded : ""}`}
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
