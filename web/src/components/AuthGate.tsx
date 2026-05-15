import { useState, useCallback } from "react";
import type { ReactNode, FormEvent } from "react";
import { AlertCircle } from "lucide-react";
import { useAuthStore, setToken } from "../stores/auth";
import { uiPath } from "../utils/base";
import { Card, CardContent } from "./ui/card";
import { Input } from "./ui/input";
import { Button } from "./ui/button";
import { Alert, AlertDescription } from "./ui/alert";

interface Props {
  children: ReactNode;
}

/**
 * Wraps the app and shows a token input when authentication is needed.
 *
 * Auth is needed when:
 * - No token is stored and the first API call returns 401
 * - A stored token becomes invalid (401 response sets authRequired)
 *
 * When no auth is configured on the server, API calls succeed without
 * a token and this gate is never shown.
 */
export function AuthGate({ children }: Props) {
  const token = useAuthStore((s) => s.token);
  const authRequired = useAuthStore((s) => s.authRequired);

  // If we have a token and auth hasn't been flagged as required, pass through
  if (token && !authRequired) {
    return <>{children}</>;
  }

  // On first load with no token, try to render the app -- if the server
  // has auth disabled, everything will work. If 401 comes back,
  // authRequired flips to true and we re-render with the login form.
  if (!token && !authRequired) {
    return <>{children}</>;
  }

  return <LoginForm />;
}

function LoginForm() {
  const [inputValue, setInputValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  const handleSubmit = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      const val = inputValue.trim();
      if (!val) return;

      setChecking(true);
      setError(null);

      try {
        // Validate the token by making a lightweight API call
        const resp = await fetch("/api/v1/status", {
          headers: { Authorization: `Bearer ${val}` },
        });

        if (resp.ok) {
          setToken(val);
        } else if (resp.status === 401) {
          setError("Invalid API key");
        } else {
          setError(`Server error: ${resp.status}`);
        }
      } catch {
        setError("connection_error");
      } finally {
        setChecking(false);
      }
    },
    [inputValue],
  );

  const handleRetry = useCallback(() => {
    setError(null);
  }, []);

  return (
    <div className="flex min-h-dvh items-center justify-center bg-background">
      <Card className="w-full max-w-[360px] gap-3 rounded-md border bg-card px-8 py-10 shadow-none">
        <CardContent className="flex flex-col items-center gap-3 px-0">
          <form
            className="flex w-full flex-col items-center gap-3"
            onSubmit={handleSubmit}
          >
            <div className="mb-1 flex items-center gap-2 text-xl font-semibold tracking-tight text-foreground">
              <img
                src={uiPath("/favicon.svg")}
                alt="LynxDB"
                className="size-7 rounded"
              />
              LynxDB
            </div>
            <p className="mb-2 text-[0.8125rem] text-muted-foreground">
              Enter your API key to continue
            </p>

            <Input
              type="password"
              className="w-full font-mono text-sm"
              placeholder="lynx_..."
              value={inputValue}
              onInput={(e) =>
                setInputValue((e.target as HTMLInputElement).value)
              }
              autoFocus
              spellCheck={false}
              autoComplete="off"
            />

            {error && error !== "connection_error" && (
              <Alert variant="destructive" className="rounded-md py-2">
                <AlertCircle className="size-4" />
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}

            {error === "connection_error" && (
              <Alert variant="destructive" className="rounded-md py-2">
                <AlertCircle className="size-4" />
                <AlertDescription>
                  Cannot connect to server -- is LynxDB running?{" "}
                  <button
                    type="button"
                    className="text-destructive underline underline-offset-2 hover:opacity-80"
                    onClick={handleRetry}
                  >
                    Retry
                  </button>
                </AlertDescription>
              </Alert>
            )}

            <Button
              type="submit"
              className="w-full"
              disabled={checking || !inputValue.trim()}
            >
              {checking ? "Verifying..." : "Connect"}
            </Button>

            <p className="mt-1 text-center text-xs text-muted-foreground">
              Generate a key with{" "}
              <code className="rounded bg-muted px-1.5 py-0.5 font-mono text-[0.7rem] text-secondary-foreground">
                lynxdb auth create-key
              </code>
            </p>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}
