import { useEffect, useState, useCallback } from "react";
import type { ReactNode } from "react";
import { AlertCircle, Sun, Moon, Monitor } from "lucide-react";
import { fetchConfig, patchConfig } from "../api/client";
import type { ServerConfig } from "../api/client";
import { useThemeStore, toggleTheme } from "../stores/ui";
import { Card, CardContent } from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Button } from "../components/ui/button";
import { Skeleton } from "../components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import { PageContainer } from "../components/PageContainer";

function Section({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="mb-6">
      <h2 className="mb-2 text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
        {title}
      </h2>
      <Card className="rounded-md border shadow-none">
        <CardContent className="flex flex-col gap-3 p-4">
          {children}
        </CardContent>
      </Card>
    </section>
  );
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex items-center gap-4">
      <span className="w-36 shrink-0 text-[0.8125rem] text-muted-foreground">
        {label}
      </span>
      <div className="flex min-w-0 flex-1 items-center gap-2">{children}</div>
    </div>
  );
}

export default function SettingsView() {
  const theme = useThemeStore((s) => s.theme);
  const [config, setConfig] = useState<ServerConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [patchable, setPatchable] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveMsg, setSaveMsg] = useState<string | null>(null);
  const [logLevel, setLogLevel] = useState("");

  const load = useCallback(async () => {
    try {
      const data = await fetchConfig();
      setConfig(data);
      setLogLevel(data.log_level ?? "info");
      setError(null);
      try {
        await patchConfig({ log_level: data.log_level ?? "info" });
        setPatchable(true);
      } catch {
        setPatchable(false);
      }
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : "Failed to fetch config";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const handleSaveLogLevel = useCallback(
    async (level: string) => {
      if (!patchable) return;
      setSaving(true);
      setSaveMsg(null);
      try {
        const result = await patchConfig({ log_level: level });
        setConfig(result.config);
        setLogLevel(level);
        if (result.restart_required && result.restart_required.length > 0) {
          setSaveMsg(
            `Saved. Restart required for: ${result.restart_required.join(", ")}`,
          );
        } else {
          setSaveMsg("Log level updated.");
        }
      } catch (err: unknown) {
        const message =
          err instanceof Error ? err.message : "Failed to update config";
        setSaveMsg(message);
      } finally {
        setSaving(false);
      }
    },
    [patchable],
  );

  const readonlyInput = "max-w-[320px] bg-muted text-sm";

  return (
    <PageContainer title="Settings" width="narrow">
      {loading ? (
        <div className="flex flex-col gap-4">
          {Array.from({ length: 3 }).map((_, i) => (
            <Skeleton key={i} className="h-32 w-full rounded-md" />
          ))}
        </div>
      ) : error && !config ? (
        <div className="flex flex-col items-center gap-3 py-16">
          <Alert variant="destructive" className="max-w-md rounded-md">
            <AlertCircle className="size-4" />
            <AlertTitle>Failed to load settings</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
          <Button variant="outline" size="sm" onClick={load}>
            Retry
          </Button>
        </div>
      ) : (
        <>
          <Section title="Appearance">
            <Row label="Theme">
              <Button
                variant={theme === "light" ? "default" : "outline"}
                size="sm"
                className="px-3"
                onClick={() => {
                  if (theme !== "light") toggleTheme();
                }}
              >
                <Sun className="size-3.5" />
                Light
              </Button>
              <Button
                variant={theme === "dark" ? "default" : "outline"}
                size="sm"
                className="px-3"
                onClick={() => {
                  if (theme !== "dark") toggleTheme();
                }}
              >
                <Moon className="size-3.5" />
                Dark
              </Button>
            </Row>
          </Section>

          {config && (
            <Section title="Server Configuration">
              <Row label="Listen Address">
                <Input
                  value={config.listen ?? "--"}
                  readOnly
                  className={readonlyInput}
                />
              </Row>
              <Row label="Data Directory">
                <Input
                  value={config.data_dir ?? "(in-memory)"}
                  readOnly
                  className="max-w-[320px] bg-muted font-mono text-xs"
                />
              </Row>
              <Row label="Retention">
                <Input
                  value={config.retention ?? "--"}
                  readOnly
                  className={readonlyInput}
                />
              </Row>
              <Row label="Log Level">
                {patchable ? (
                  <>
                    <Select
                      value={logLevel}
                      onValueChange={(val) => {
                        setLogLevel(val);
                        handleSaveLogLevel(val);
                      }}
                      disabled={saving}
                    >
                      <SelectTrigger className="w-[140px]">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="debug">debug</SelectItem>
                        <SelectItem value="info">info</SelectItem>
                        <SelectItem value="warn">warn</SelectItem>
                        <SelectItem value="error">error</SelectItem>
                      </SelectContent>
                    </Select>
                    {saving && (
                      <span className="text-xs text-muted-foreground">
                        Saving...
                      </span>
                    )}
                  </>
                ) : (
                  <Input
                    value={config.log_level ?? "--"}
                    readOnly
                    className={readonlyInput}
                  />
                )}
              </Row>
              {saveMsg && (
                <p className="text-xs text-muted-foreground">{saveMsg}</p>
              )}
            </Section>
          )}

          {config?.query && (
            <Section title="Query Limits">
              <Row label="Max Concurrent">
                <Input
                  value={String(config.query.max_concurrent ?? "--")}
                  readOnly
                  className="max-w-[140px] bg-muted text-sm"
                />
              </Row>
              <Row label="Default Limit">
                <Input
                  value={String(config.query.default_result_limit ?? "--")}
                  readOnly
                  className="max-w-[140px] bg-muted text-sm"
                />
              </Row>
              <Row label="Max Limit">
                <Input
                  value={String(config.query.max_result_limit ?? "--")}
                  readOnly
                  className="max-w-[140px] bg-muted text-sm"
                />
              </Row>
            </Section>
          )}

          {!patchable && config && (
            <p className="text-xs text-muted-foreground">
              <Monitor className="mb-0.5 mr-1 inline size-3" />
              Server configuration is read-only. Log level and retention can
              be changed via{" "}
              <code className="rounded bg-muted px-1 py-0.5 font-mono text-[0.7rem]">
                PATCH /api/v1/config
              </code>{" "}
              with admin credentials, or via{" "}
              <code className="rounded bg-muted px-1 py-0.5 font-mono text-[0.7rem]">
                lynxdb config reload
              </code>
              .
            </p>
          )}
        </>
      )}
    </PageContainer>
  );
}
