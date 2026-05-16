import { useCallback } from "react";
import { Table2, List, Download } from "lucide-react";
import { ToggleGroup, ToggleGroupItem } from "./ui/toggle-group";
import { Button } from "./ui/button";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "./ui/dropdown-menu";

interface TableToolbarProps {
  viewMode: "table" | "list";
  onViewModeChange: (mode: "table" | "list") => void;
  onExport: (format: "csv" | "json", scope: "page" | "all") => void;
  totalCount: number;
  pageCount: number;
}

const fmtNum = (n: number) => new Intl.NumberFormat().format(n);

export function TableToolbar({
  viewMode,
  onViewModeChange,
  onExport,
  totalCount,
  pageCount,
}: TableToolbarProps) {
  const handleExportClick = useCallback(
    (format: "csv" | "json", scope: "page" | "all") => {
      onExport(format, scope);
    },
    [onExport],
  );

  return (
    <div className="flex h-8 shrink-0 items-center justify-between gap-2 border-b border-border bg-secondary px-3">
      <div className="flex items-center gap-2">
        <ToggleGroup
          type="single"
          variant="outline"
          size="sm"
          value={viewMode}
          onValueChange={(val) => {
            if (val === "table" || val === "list") onViewModeChange(val);
          }}
        >
          <ToggleGroupItem value="table" aria-label="Table view" className="h-6 w-7 px-0">
            <Table2 className="size-3.5" />
          </ToggleGroupItem>
          <ToggleGroupItem value="list" aria-label="List view" className="h-6 w-7 px-0">
            <List className="size-3.5" />
          </ToggleGroupItem>
        </ToggleGroup>
      </div>

      <div className="flex items-center gap-2">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="xs" className="gap-1 text-muted-foreground">
              <Download className="size-3.5" />
              Export
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end">
            <DropdownMenuItem onClick={() => handleExportClick("csv", "page")}>
              CSV - Current page ({fmtNum(pageCount)} rows)
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleExportClick("csv", "all")}>
              CSV - All results ({fmtNum(totalCount)} rows)
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleExportClick("json", "page")}>
              JSON - Current page ({fmtNum(pageCount)} rows)
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleExportClick("json", "all")}>
              JSON - All results ({fmtNum(totalCount)} rows)
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
  );
}
