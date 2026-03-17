import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { GridStack } from "gridstack";
import "gridstack/dist/gridstack.min.css";
import type { DashboardPanel, DashboardSummary } from "../../api/client";
import {
  fetchDashboard,
  createDashboard,
  updateDashboard,
  deleteDashboard,
} from "../../api/client";
import { DashboardHeader } from "./DashboardHeader";
import { PanelRenderer } from "./PanelRenderer";
import { PanelEditForm } from "./PanelEditForm";
import styles from "./DashboardDetail.module.css";

interface DashboardDetailProps {
  dashboardId: string | null;
  editMode?: boolean;
}

export function DashboardDetail({
  dashboardId,
  editMode: initialEditMode,
}: DashboardDetailProps) {
  const [dashboard, setDashboard] = useState<DashboardSummary | null>(null);
  const [loading, setLoading] = useState(!!dashboardId);
  const [error, setError] = useState<string | null>(null);
  const [editMode, setEditMode] = useState(initialEditMode ?? false);
  const [from, setFrom] = useState("-1h");
  const [refreshInterval, setRefreshInterval] = useState(0);
  const [refreshTick, setRefreshTick] = useState(0);
  const [dashName, setDashName] = useState("");
  const [unsavedPanels, setUnsavedPanels] = useState<DashboardPanel[]>([]);
  const [variables] = useState<Record<string, string>>({});
  const [showPanelForm, setShowPanelForm] = useState(false);
  const [editingPanel, setEditingPanel] = useState<DashboardPanel | null>(null);

  const gridRef = useRef<HTMLDivElement>(null);
  const gsRef = useRef<GridStack | null>(null);

  // Fetch dashboard
  useEffect(() => {
    if (!dashboardId) {
      setDashName("Untitled Dashboard");
      setUnsavedPanels([]);
      setEditMode(true);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchDashboard(dashboardId)
      .then((d) => {
        if (cancelled) return;
        setDashboard(d);
        setDashName(d.name);
        setUnsavedPanels(d.panels);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load dashboard");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dashboardId]);

  // Initialize gridstack
  useEffect(() => {
    const el = gridRef.current;
    if (!el || unsavedPanels.length === 0) return;

    // Destroy previous instance if exists
    if (gsRef.current) {
      gsRef.current.destroy(false);
      gsRef.current = null;
    }

    const gs = GridStack.init(
      {
        column: 12,
        cellHeight: 80,
        disableResize: !editMode,
        disableDrag: !editMode,
        float: true,
        animate: true,
      },
      el,
    );
    gsRef.current = gs;

    // Listen for layout changes in edit mode
    gs.on("change", () => {
      if (!editMode) return;
      const items = gs.getGridItems();
      setUnsavedPanels((prev) => {
        const updated = [...prev];
        for (const item of items) {
          const node = item.gridstackNode;
          if (!node) continue;
          const panelId = item.getAttribute("gs-id");
          const idx = updated.findIndex((p) => p.id === panelId);
          if (idx >= 0) {
            updated[idx] = {
              ...updated[idx],
              position: {
                x: node.x ?? updated[idx].position.x,
                y: node.y ?? updated[idx].position.y,
                w: node.w ?? updated[idx].position.w,
                h: node.h ?? updated[idx].position.h,
              },
            };
          }
        }
        return updated;
      });
    });

    return () => {
      gs.destroy(false);
      gsRef.current = null;
    };
  }, [unsavedPanels.length > 0, editMode]);

  // Update gridstack enable/disable on editMode change
  useEffect(() => {
    const gs = gsRef.current;
    if (!gs) return;
    gs.enableMove(editMode);
    gs.enableResize(editMode);
  }, [editMode]);

  // Auto-refresh
  useEffect(() => {
    if (refreshInterval <= 0) return;
    const id = setInterval(() => {
      setRefreshTick((t) => t + 1);
    }, refreshInterval);
    return () => clearInterval(id);
  }, [refreshInterval]);

  // Re-trigger all panels on time change
  const handleTimeChange = useCallback((newFrom: string) => {
    setFrom(newFrom);
    setRefreshTick((t) => t + 1);
  }, []);

  // Save handler
  async function handleSave() {
    if (!dashName.trim() || unsavedPanels.length === 0) return;
    try {
      const input = {
        name: dashName.trim(),
        panels: unsavedPanels,
        variables: dashboard?.variables,
      };
      if (dashboardId && dashboard) {
        await updateDashboard(dashboardId, input);
        setDashboard({ ...dashboard, ...input });
        setEditMode(false);
      } else {
        const created = await createDashboard(input);
        route(`/dashboards/${created.id}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save");
    }
  }

  function handleDiscard() {
    if (dashboard) {
      setUnsavedPanels(dashboard.panels);
      setDashName(dashboard.name);
    }
    setEditMode(false);
  }

  function handleAddPanel() {
    setEditingPanel(null);
    setShowPanelForm(true);
  }

  function handleEditPanel(panel: DashboardPanel) {
    setEditingPanel(panel);
    setShowPanelForm(true);
  }

  function handlePanelFormSave(panel: DashboardPanel) {
    if (editingPanel) {
      // Update existing panel
      setUnsavedPanels((prev) =>
        prev.map((p) => (p.id === panel.id ? panel : p)),
      );
    } else {
      // Add new panel
      setUnsavedPanels((prev) => [...prev, panel]);
      const gs = gsRef.current;
      if (gs) {
        gs.addWidget({
          id: panel.id,
          x: panel.position.x,
          y: panel.position.y,
          w: panel.position.w,
          h: panel.position.h,
          content: `<div id="panel-${panel.id}"></div>`,
        });
      }
    }
    setShowPanelForm(false);
    setEditingPanel(null);
  }

  async function handleDeleteDashboard() {
    if (!dashboardId) return;
    if (!window.confirm("Delete this dashboard? This action cannot be undone.")) return;
    try {
      await deleteDashboard(dashboardId);
      route("/dashboards");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete dashboard");
    }
  }

  function handleDeletePanel(panelId: string) {
    setUnsavedPanels((prev) => prev.filter((p) => p.id !== panelId));
    const gs = gsRef.current;
    if (gs) {
      const item = gs
        .getGridItems()
        .find((el) => el.getAttribute("gs-id") === panelId);
      if (item) gs.removeWidget(item);
    }
  }

  const saveDisabled = !dashName.trim() || unsavedPanels.length === 0;

  if (loading) {
    return (
      <div class={styles.detail}>
        <div class={styles.loading}>Loading dashboard...</div>
      </div>
    );
  }

  if (error && !dashboard && dashboardId) {
    return (
      <div class={styles.detail}>
        <div class={styles.error}>
          <div>{error}</div>
          <button
            type="button"
            class={styles.retryBtn}
            onClick={() => {
              setError(null);
              setLoading(true);
              fetchDashboard(dashboardId!)
                .then((d) => {
                  setDashboard(d);
                  setDashName(d.name);
                  setUnsavedPanels(d.panels);
                })
                .catch((e) =>
                  setError(
                    e instanceof Error ? e.message : "Failed to load",
                  ),
                )
                .finally(() => setLoading(false));
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div class={styles.detail}>
      <DashboardHeader
        name={dashName}
        editable={true}
        onNameChange={setDashName}
        from={from}
        onTimeChange={handleTimeChange}
        refreshInterval={refreshInterval}
        onRefreshIntervalChange={setRefreshInterval}
        editMode={editMode}
        onToggleEdit={() => setEditMode(true)}
        onSave={handleSave}
        onDiscard={handleDiscard}
        saveDisabled={saveDisabled}
        onDelete={dashboardId ? handleDeleteDashboard : undefined}
      />

      <div class={styles.gridArea}>
        {unsavedPanels.length === 0 && editMode && (
          <div class={styles.emptyState}>
            <div>No panels yet. Add your first panel to get started.</div>
          </div>
        )}

        {unsavedPanels.length > 0 && (
          <div ref={gridRef} class="grid-stack">
            {unsavedPanels.map((panel) => (
              <div
                key={panel.id}
                class="grid-stack-item"
                gs-id={panel.id}
                gs-x={panel.position.x}
                gs-y={panel.position.y}
                gs-w={panel.position.w}
                gs-h={panel.position.h}
              >
                <div class="grid-stack-item-content">
                  <PanelRenderer
                    panel={panel}
                    from={from}
                    variables={variables}
                    refreshTick={refreshTick}
                    editMode={editMode}
                    onEdit={() => handleEditPanel(panel)}
                    onDelete={() => handleDeletePanel(panel.id)}
                  />
                </div>
              </div>
            ))}
          </div>
        )}

        {editMode && (
          <button
            type="button"
            class={styles.addPanelBtn}
            onClick={handleAddPanel}
          >
            + Add Panel
          </button>
        )}
      </div>

      {showPanelForm && (
        <PanelEditForm
          panel={editingPanel ?? undefined}
          onSave={handlePanelFormSave}
          onCancel={() => {
            setShowPanelForm(false);
            setEditingPanel(null);
          }}
        />
      )}
    </div>
  );
}
