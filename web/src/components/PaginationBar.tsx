import { useMemo } from "react";
import {
  Pagination,
  PaginationContent,
  PaginationItem,
  PaginationLink,
  PaginationPrevious,
  PaginationNext,
  PaginationEllipsis,
} from "./ui/pagination";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "./ui/select";

interface PaginationBarProps {
  page: number;
  pageSize: number;
  total: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: number) => void;
}

const PAGE_SIZES = [50, 100, 500, 1000];
const fmtNum = (n: number) => new Intl.NumberFormat().format(n);

/**
 * Compute which page numbers to display. Always shows first, last,
 * and 2 pages around the current page, with "..." for gaps.
 */
function computePageNumbers(
  current: number,
  total: number,
): (number | "...")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }

  const pages = new Set<number>();
  pages.add(1);
  pages.add(total);
  for (
    let i = Math.max(2, current - 1);
    i <= Math.min(total - 1, current + 1);
    i++
  ) {
    pages.add(i);
  }

  const sorted = Array.from(pages).sort((a, b) => a - b);
  const result: (number | "...")[] = [];

  for (let i = 0; i < sorted.length; i++) {
    const curr = sorted[i];
    const prev = sorted[i - 1];
    if (curr === undefined) continue;
    if (i > 0 && prev !== undefined && curr - prev > 1) {
      result.push("...");
    }
    result.push(curr);
  }

  return result;
}

export function PaginationBar({
  page,
  pageSize,
  total,
  onPageChange,
  onPageSizeChange,
}: PaginationBarProps) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const pageNumbers = useMemo(
    () => computePageNumbers(page, totalPages),
    [page, totalPages],
  );

  return (
    <div className="flex h-9 shrink-0 items-center justify-between border-t border-border bg-secondary px-3 text-xs font-sans">
      <div className="flex items-center min-w-[6.25rem]">
        <span className="whitespace-nowrap text-muted-foreground">{fmtNum(total)} results</span>
      </div>

      <Pagination className="mx-0 w-auto">
        <PaginationContent className="gap-0.5">
          <PaginationItem>
            <PaginationPrevious
              href="#"
              className={`h-6 text-xs px-2 ${page <= 1 ? "pointer-events-none opacity-40" : ""}`}
              onClick={(e) => {
                e.preventDefault();
                if (page > 1) onPageChange(page - 1);
              }}
              aria-label="Previous page"
            />
          </PaginationItem>

          {pageNumbers.map((item, idx) =>
            item === "..." ? (
              <PaginationItem key={`ellipsis-${idx}`}>
                <PaginationEllipsis className="size-6" />
              </PaginationItem>
            ) : (
              <PaginationItem key={item}>
                <PaginationLink
                  href="#"
                  isActive={item === page}
                  className="size-6 text-xs"
                  onClick={(e) => {
                    e.preventDefault();
                    onPageChange(item);
                  }}
                  aria-label={`Page ${item}`}
                >
                  {item}
                </PaginationLink>
              </PaginationItem>
            ),
          )}

          <PaginationItem>
            <PaginationNext
              href="#"
              className={`h-6 text-xs px-2 ${page >= totalPages ? "pointer-events-none opacity-40" : ""}`}
              onClick={(e) => {
                e.preventDefault();
                if (page < totalPages) onPageChange(page + 1);
              }}
              aria-label="Next page"
            />
          </PaginationItem>
        </PaginationContent>
      </Pagination>

      <div className="flex items-center justify-end min-w-[6.25rem]">
        <Select
          value={String(pageSize)}
          onValueChange={(val) => onPageSizeChange(Number(val))}
        >
          <SelectTrigger size="sm" className="h-6 text-xs px-2 min-w-[5.5rem]" aria-label="Page size">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {PAGE_SIZES.map((size) => (
              <SelectItem key={size} value={String(size)}>
                {size} / page
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
