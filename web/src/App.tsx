import { lazy, Suspense } from "react";
import { BrowserRouter, Routes, Route } from "react-router";
import { Sidebar } from "./components/Sidebar";
import { AuthGate } from "./components/AuthGate";
import { CommandPalette } from "./components/CommandPalette";
import { HelpOverlay } from "./components/HelpOverlay";
import { uiBase } from "./utils/base";
import { Toaster } from "./components/ui/sonner";
import { TooltipProvider } from "./components/ui/tooltip";
import { useKeyboardShortcuts } from "./hooks/useKeyboardShortcuts";
import {
  useOverlayStore,
  setPaletteOpen,
  setHelpOverlayOpen,
} from "./utils/keyboard";

const SearchView = lazy(() =>
  import("./views/SearchView").then((m) => ({ default: m.SearchView })),
);
const QueriesView = lazy(() => import("./views/QueriesView"));
const SettingsView = lazy(() => import("./views/SettingsView"));
const StatusView = lazy(() =>
  import("./views/StatusView").then((m) => ({ default: m.StatusView })),
);

/**
 * App-shell keyboard shortcuts (command palette, help). Registered here so
 * they work regardless of the active route or lazy-loaded chunk.
 */
function ShellShortcuts() {
  useKeyboardShortcuts({
    onOpenPalette: () => {
      setHelpOverlayOpen(false);
      setPaletteOpen(!useOverlayStore.getState().paletteOpen);
    },
    onOpenHelp: () => {
      setPaletteOpen(false);
      setHelpOverlayOpen(!useOverlayStore.getState().helpOverlayOpen);
    },
  });
  return null;
}

export function App() {
  return (
    <AuthGate>
      <BrowserRouter basename={uiBase || "/"}>
        <TooltipProvider delayDuration={200}>
          <ShellShortcuts />
          <div className="flex h-dvh w-full overflow-hidden bg-background text-foreground">
            <Sidebar />
            <main className="flex min-w-0 flex-1 flex-col overflow-hidden">
              <Suspense fallback={null}>
                <Routes>
                  <Route path="/" element={<SearchView />} />
                  <Route path="/queries" element={<QueriesView />} />
                  <Route path="/status" element={<StatusView />} />
                  <Route path="/settings" element={<SettingsView />} />
                </Routes>
              </Suspense>
            </main>
          </div>
          <CommandPalette />
          <HelpOverlay />
          <Toaster position="bottom-right" richColors closeButton />
        </TooltipProvider>
      </BrowserRouter>
    </AuthGate>
  );
}
