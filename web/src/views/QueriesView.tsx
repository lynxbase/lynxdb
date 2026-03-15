interface Props {
  path?: string;
}

export default function QueriesView(_props: Props) {
  return (
    <div style={{ padding: "var(--space-6)", color: "var(--text-secondary)" }}>
      <h1 style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)" }}>
        Saved Queries
      </h1>
      <p style={{ marginTop: "var(--space-2)" }}>
        Saved query management will be available in a future update.
      </p>
    </div>
  );
}
