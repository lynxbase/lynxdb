import React, { useState, useCallback, useRef } from "react";
import { FieldCommandMenu } from "./FieldCommandMenu";
import { typeAbbrev } from "../../utils/fieldType";
import { cn } from "@/lib/utils";

export interface FieldValue {
  value: string;
  count: number;
}

interface FieldItemProps {
  name: string;
  type?: string;
  isAdded?: boolean;
  onInsertCommand?: (template: string) => void;
}

export function FieldItem({
  name,
  type,
  isAdded,
  onInsertCommand,
}: FieldItemProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const moreBtnRef = useRef<HTMLButtonElement>(null);

  const handleNameClick = useCallback(() => {
    if (onInsertCommand) {
      onInsertCommand(`| where ${name}!=""`);
    }
  }, [onInsertCommand, name]);

  const handleMoreClick = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    setMenuOpen((prev) => !prev);
  }, []);

  const handleCloseMenu = useCallback(() => {
    setMenuOpen(false);
  }, []);

  const abbrev = typeAbbrev(type);

  return (
    <div
      className={cn(
        "flex flex-col relative",
        isAdded && "border-l-2 border-chart-4",
      )}
    >
      <div className="group flex flex-row items-center gap-1 py-0.5 px-2 transition-colors duration-75 motion-reduce:transition-none hover:bg-muted/50">
        <button
          type="button"
          className="flex-1 text-[0.8125rem] text-foreground overflow-hidden text-ellipsis whitespace-nowrap font-mono border-none bg-transparent cursor-pointer p-0 text-left hover:text-primary focus-visible:outline-2 focus-visible:outline-ring"
          onClick={handleNameClick}
          title={`Filter: ${name}!=""`}
        >
          {name}
        </button>
        {abbrev && (
          <span className="shrink-0 text-[0.625rem] text-muted-foreground font-mono">
            {abbrev}
          </span>
        )}
        <button
          ref={moreBtnRef}
          type="button"
          className="shrink-0 flex items-center justify-center size-5 border-none rounded-sm bg-transparent text-muted-foreground text-sm cursor-pointer opacity-0 group-hover:opacity-100 transition-opacity duration-100 motion-reduce:transition-none hover:bg-muted hover:text-foreground focus-visible:opacity-100 focus-visible:outline-2 focus-visible:outline-ring"
          onClick={handleMoreClick}
          aria-label={`Commands for ${name}`}
          title="Insert command"
        >
          &#8943;
        </button>
      </div>

      {menuOpen && moreBtnRef.current && onInsertCommand && (
        <FieldCommandMenu
          field={name}
          fieldType={abbrev}
          anchorRect={moreBtnRef.current.getBoundingClientRect()}
          onInsertCommand={onInsertCommand}
          onClose={handleCloseMenu}
        />
      )}
    </div>
  );
}
