import { useState, useCallback } from "preact/hooks";
import type { ComponentChildren } from "preact";
import { token, authRequired, setToken, clearToken } from "../api/auth";
import styles from "./AuthGate.module.css";

interface Props {
  children: ComponentChildren;
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
  // If we have a token and auth hasn't been flagged as required, pass through
  if (token.value && !authRequired.value) {
    return <>{children}</>;
  }

  // On first load with no token, try to render the app -- if the server
  // has auth disabled, everything will work. If 401 comes back,
  // authRequired signal flips to true and we re-render with the login form.
  if (!token.value && !authRequired.value) {
    return <>{children}</>;
  }

  return <LoginForm />;
}

function LoginForm() {
  const [inputValue, setInputValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [checking, setChecking] = useState(false);

  const handleSubmit = useCallback(async (e: Event) => {
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
      setError("Cannot connect to server");
    } finally {
      setChecking(false);
    }
  }, [inputValue]);

  return (
    <div class={styles.backdrop}>
      <form class={styles.card} onSubmit={handleSubmit}>
        <div class={styles.logo}>
          <img src="/lynxdb-icon.png" alt="LynxDB" style={{ height: '32px' }} />
          LynxDB
        </div>
        <div class={styles.subtitle}>Enter your API key to continue</div>

        <input
          type="password"
          class={styles.input}
          placeholder="lynx_..."
          value={inputValue}
          onInput={(e) => setInputValue((e.target as HTMLInputElement).value)}
          autoFocus
          spellcheck={false}
          autocomplete="off"
        />

        {error && <div class={styles.error}>{error}</div>}

        <button
          type="submit"
          class={styles.submitBtn}
          disabled={checking || !inputValue.trim()}
        >
          {checking ? "Verifying..." : "Connect"}
        </button>

        <div class={styles.hint}>
          Generate a key with <code>lynxdb auth create-key</code>
        </div>
      </form>
    </div>
  );
}

/** Logout button for the nav bar. Only renders when a token is stored. */
export function LogoutButton() {
  if (!token.value) return null;

  return (
    <button
      type="button"
      class={styles.logoutBtn}
      onClick={clearToken}
      title="Sign out"
      aria-label="Sign out"
    >
      Sign out
    </button>
  );
}
