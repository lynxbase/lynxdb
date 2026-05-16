/**
 * Hook that encapsulates the live tail SSE lifecycle:
 * start/stop tail, manage tail cleanup, auto-scroll pause detection,
 * and new-events badge click.
 *
 * All imperative resources (cleanup function, auto-scroll ref) are
 * per-mount refs with proper useEffect cleanup.
 */

import { useCallback, useEffect, useRef } from "react";
import { startTail } from "../api/sse";
import { useSearchStore } from "../stores/search";
import type { TailEvent } from "../api/sse";

/** Maximum events to keep in the live tail buffer */
const TAIL_BUFFER_CAP = 10_000;

/** Shorthand for imperative store access */
const ss = useSearchStore;

export function useLiveTail() {
  const tailCleanupRef = useRef<(() => void) | null>(null);
  const resultsAreaRef = useRef<HTMLDivElement>(null);
  /** Tracks whether auto-scroll is paused (user scrolled away from top) */
  const autoScrollPaused = useRef(false);

  // --- Tail toggle ---

  const handleTailToggle = useCallback(() => {
    const state = ss.getState();
    if (state.tailActive) {
      // Stop tailing
      if (tailCleanupRef.current) {
        tailCleanupRef.current();
        tailCleanupRef.current = null;
      }
      ss.setState({
        tailActive: false,
        tailEvents: [],
        tailNewCount: 0,
        tailCatchupDone: false,
        tailReconnecting: false,
      });
      autoScrollPaused.current = false;
      return;
    }

    // Start tailing
    const q = state.query.trim();
    ss.setState({
      tailActive: true,
      tailEvents: [],
      tailNewCount: 0,
      tailCatchupDone: false,
      result: null,
      stats: null,
      error: null,
    });
    autoScrollPaused.current = false;

    const cleanup = startTail(q, state.from, 100, {
      onEvent(event: TailEvent) {
        const prev = ss.getState().tailEvents;
        const next = [event, ...prev];
        ss.setState({
          tailEvents:
            next.length > TAIL_BUFFER_CAP
              ? next.slice(0, TAIL_BUFFER_CAP)
              : next,
        });

        if (autoScrollPaused.current) {
          ss.setState((s) => ({ tailNewCount: s.tailNewCount + 1 }));
        }
      },
      onCatchupDone(_count: number) {
        ss.setState({ tailCatchupDone: true });
      },
      onError(message: string) {
        ss.setState({ error: message });
      },
      onWarning(message: string) {
        // Show warning briefly in the error slot, then clear
        ss.setState({ error: message });
        setTimeout(() => {
          if (ss.getState().error === message) {
            ss.setState({ error: null });
          }
        }, 3000);
      },
      onReconnecting(isReconnecting: boolean) {
        ss.setState({ tailReconnecting: isReconnecting });
      },
    });

    tailCleanupRef.current = cleanup;
  }, []);

  // --- New events badge click ---

  const handleNewEventsBadgeClick = useCallback(() => {
    if (!resultsAreaRef.current) return;
    const viewport = resultsAreaRef.current.querySelector(
      "[class*='viewport']",
    );
    if (viewport) {
      viewport.scrollTop = 0;
    }
    autoScrollPaused.current = false;
    ss.setState({ tailNewCount: 0 });
  }, []);

  // Capture-phase scroll listener for auto-scroll pause detection.
  useEffect(() => {
    const el = resultsAreaRef.current;
    if (!el) return;

    function onScroll(e: Event) {
      if (!ss.getState().tailActive) return;
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const scrolledFromTop = target.scrollTop;
      autoScrollPaused.current = scrolledFromTop > 10;
      if (!autoScrollPaused.current) {
        ss.setState({ tailNewCount: 0 });
      }
    }

    el.addEventListener("scroll", onScroll, true);
    return () => el.removeEventListener("scroll", onScroll, true);
  }, []);

  // Cleanup SSE on unmount
  useEffect(() => {
    return () => {
      if (tailCleanupRef.current) {
        tailCleanupRef.current();
        tailCleanupRef.current = null;
      }
    };
  }, []);

  return {
    resultsAreaRef,
    handleTailToggle,
    handleNewEventsBadgeClick,
  };
}
