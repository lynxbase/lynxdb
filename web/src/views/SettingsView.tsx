interface Props {
  path?: string;
}

export default function SettingsView(_props: Props) {
  return (
    <div style={{ padding: "var(--space-6)", color: "var(--text-secondary)" }}>
      <h1 style={{ fontSize: "1.25rem", fontWeight: 600, color: "var(--text-primary)" }}>
        Settings
      </h1>
      <p style={{ marginTop: "var(--space-2)" }}>
        Server settings and configuration will be available in a future update.
      </p>
    </div>
  );
}
