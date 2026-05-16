import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface PageContainerProps {
  title: string;
  /** Right-aligned header actions (buttons, status, etc.). */
  actions?: ReactNode;
  /** Content max width; defaults to a comfortable reading column. */
  width?: "default" | "narrow";
  children: ReactNode;
}

/**
 * Consistent scrollable page shell for the document-style views
 * (Status, Saved Queries, Settings). Guarantees identical header
 * alignment, padding, and max-width across every route.
 */
export function PageContainer({
  title,
  actions,
  width = "default",
  children,
}: PageContainerProps) {
  return (
    <div className="h-full overflow-y-auto">
      <div
        className={cn(
          "mx-auto w-full px-6 py-5",
          width === "narrow" ? "max-w-3xl" : "max-w-5xl",
        )}
      >
        <header className="mb-5 flex min-h-8 items-center gap-4">
          <h1 className="text-base font-semibold tracking-tight text-foreground">
            {title}
          </h1>
          {actions ? (
            <div className="ml-auto flex items-center gap-2">{actions}</div>
          ) : null}
        </header>
        {children}
      </div>
    </div>
  );
}
