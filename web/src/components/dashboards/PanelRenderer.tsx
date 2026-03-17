import type { DashboardPanel, AggregateResult } from "../../api/client";
import { usePanelQuery } from "./usePanelQuery";
import { PanelChrome } from "./PanelChrome";
import { TimechartPanel } from "./TimechartPanel";
import { BarPanel } from "./BarPanel";
import { LinePanel } from "./LinePanel";
import { AreaPanel } from "./AreaPanel";
import { TablePanel } from "./TablePanel";
import { StatPanel } from "./StatPanel";
import { PiePanel } from "./PiePanel";

interface PanelRendererProps {
  panel: DashboardPanel;
  from: string;
  to?: string;
  variables: Record<string, string>;
  refreshTick?: number;
  editMode?: boolean;
  onEdit?: () => void;
  onDelete?: () => void;
}

export function PanelRenderer({
  panel,
  from,
  to,
  variables,
  refreshTick,
  editMode,
  onEdit,
  onDelete,
}: PanelRendererProps) {
  const { result, loading, error, refresh } = usePanelQuery(
    panel.q,
    from,
    to,
    variables,
    refreshTick,
  );

  const isEmpty =
    result &&
    ((result.type === "events" && result.events.length === 0) ||
      ((result.type === "aggregate" || result.type === "timechart") &&
        result.rows.length === 0));

  let content = null;

  if (result && !isEmpty) {
    switch (panel.type) {
      case "timechart":
        content = (
          <TimechartPanel data={result as AggregateResult} />
        );
        break;
      case "bar":
        content = <BarPanel data={result as AggregateResult} />;
        break;
      case "line":
        content = <LinePanel data={result as AggregateResult} />;
        break;
      case "area":
        content = <AreaPanel data={result as AggregateResult} />;
        break;
      case "table":
        content = <TablePanel data={result} />;
        break;
      case "stat":
        content = <StatPanel data={result} panel={panel} />;
        break;
      case "pie":
        content = <PiePanel data={result as AggregateResult} />;
        break;
      default:
        content = (
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              height: "100%",
              color: "var(--text-secondary)",
              fontSize: "0.8rem",
            }}
          >
            Unknown panel type: {panel.type}
          </div>
        );
    }
  }

  if (isEmpty) {
    content = (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          color: "var(--text-secondary)",
          fontSize: "0.8rem",
        }}
      >
        No data
      </div>
    );
  }

  return (
    <PanelChrome
      title={panel.title}
      query={panel.q}
      from={from}
      loading={loading}
      error={error}
      onRefresh={refresh}
      editMode={editMode}
      onEdit={onEdit}
      onDelete={onDelete}
    >
      {content}
    </PanelChrome>
  );
}
