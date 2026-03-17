import { useState, useEffect, useRef } from "preact/hooks";
import type { ComponentChildren } from "preact";
import { MoreHorizontal } from "lucide-preact";
import { route } from "preact-router";
import styles from "./PanelChrome.module.css";

interface PanelChromeProps {
  title: string;
  query?: string;
  from?: string;
  loading?: boolean;
  error?: string | null;
  onRefresh?: () => void;
  onClone?: () => void;
  editMode?: boolean;
  onEdit?: () => void;
  onDelete?: () => void;
  children?: ComponentChildren;
}

export function PanelChrome({
  title,
  query,
  from,
  loading,
  error,
  onRefresh,
  onClone,
  editMode,
  onEdit,
  onDelete,
  children,
}: PanelChromeProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  // Close menu on click outside
  useEffect(() => {
    if (!menuOpen) return;
    function handleClick(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    }
    document.addEventListener("click", handleClick, true);
    return () => document.removeEventListener("click", handleClick, true);
  }, [menuOpen]);

  function handleOpenInSearch() {
    setMenuOpen(false);
    if (query) {
      const params = new URLSearchParams();
      params.set("q", query);
      if (from) params.set("from", from);
      route(`/?${params.toString()}`);
    }
  }

  return (
    <div class={styles.chrome}>
      {loading && <div class={styles.loadingBar} />}
      <div class={styles.header}>
        <span class={styles.title}>{title}</span>
        <div class={styles.menuWrap} ref={menuRef}>
          <button
            type="button"
            class={styles.menuBtn}
            onClick={(e) => {
              e.stopPropagation();
              setMenuOpen((o) => !o);
            }}
            aria-label="Panel menu"
          >
            <MoreHorizontal size={14} />
          </button>
          {menuOpen && (
            <div class={styles.menu}>
              {onRefresh && (
                <button
                  type="button"
                  class={styles.menuItem}
                  onClick={() => {
                    setMenuOpen(false);
                    onRefresh();
                  }}
                >
                  Refresh
                </button>
              )}
              {query && (
                <button
                  type="button"
                  class={styles.menuItem}
                  onClick={handleOpenInSearch}
                >
                  Open in Search
                </button>
              )}
              {onClone && !editMode && (
                <button
                  type="button"
                  class={styles.menuItem}
                  onClick={() => {
                    setMenuOpen(false);
                    onClone();
                  }}
                >
                  Clone
                </button>
              )}
              {editMode && onEdit && (
                <button
                  type="button"
                  class={styles.menuItem}
                  onClick={() => {
                    setMenuOpen(false);
                    onEdit();
                  }}
                >
                  Edit
                </button>
              )}
              {editMode && onDelete && (
                <button
                  type="button"
                  class={styles.menuItemDanger}
                  onClick={() => {
                    setMenuOpen(false);
                    onDelete();
                  }}
                >
                  Delete
                </button>
              )}
            </div>
          )}
        </div>
      </div>
      <div class={styles.content}>
        {error ? (
          <div class={styles.error}>
            <div class={styles.errorText}>{error}</div>
            {onRefresh && (
              <button type="button" class={styles.retryBtn} onClick={onRefresh}>
                Retry
              </button>
            )}
          </div>
        ) : (
          children
        )}
      </div>
    </div>
  );
}
