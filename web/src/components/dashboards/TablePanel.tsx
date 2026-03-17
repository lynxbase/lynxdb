import type { QueryResult, AggregateResult, EventsResult } from "../../api/client";

const tableStyle: Record<string, string> = {
  width: "100%",
  fontSize: "0.75rem",
  borderCollapse: "collapse",
};

const thStyle: Record<string, string> = {
  position: "sticky",
  top: "0",
  padding: "4px 8px",
  textAlign: "left",
  fontWeight: "600",
  background: "var(--bg-secondary)",
  borderBottom: "1px solid var(--border)",
  whiteSpace: "nowrap",
};

const tdStyle: Record<string, string> = {
  padding: "3px 8px",
  borderBottom: "1px solid var(--border)",
  whiteSpace: "nowrap",
  overflow: "hidden",
  textOverflow: "ellipsis",
  maxWidth: "300px",
};

function renderAggregate(data: AggregateResult) {
  return (
    <div style={{ overflow: "auto", height: "100%", width: "100%" }}>
      <table style={tableStyle}>
        <thead>
          <tr>
            {data.columns.map((col) => (
              <th key={col} style={thStyle}>
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.rows.slice(0, 100).map((row, ri) => (
            <tr
              key={ri}
              style={
                ri % 2 === 1
                  ? { background: "var(--bg-secondary)" }
                  : undefined
              }
            >
              {row.map((cell, ci) => (
                <td key={ci} style={tdStyle}>
                  {cell == null ? "" : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function renderEvents(data: EventsResult) {
  if (data.events.length === 0) return null;

  const columns = Object.keys(data.events[0]);

  return (
    <div style={{ overflow: "auto", height: "100%", width: "100%" }}>
      <table style={tableStyle}>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col} style={thStyle}>
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.events.slice(0, 100).map((ev, ri) => (
            <tr
              key={ri}
              style={
                ri % 2 === 1
                  ? { background: "var(--bg-secondary)" }
                  : undefined
              }
            >
              {columns.map((col) => (
                <td key={col} style={tdStyle}>
                  {ev[col] == null ? "" : String(ev[col])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function TablePanel({ data }: { data: QueryResult }) {
  if (data.type === "events") {
    return renderEvents(data as EventsResult);
  }
  return renderAggregate(data as AggregateResult);
}
