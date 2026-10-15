import { useRef, useState } from "react";
import type { FormEvent } from "react";

import { describeError } from "../error-messages.js";
import { auth } from "../services.js";
import { useAuth } from "./use-auth.js";

export function AuthPanel() {
  const { user } = useAuth();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | undefined>();
  const [busy, setBusy] = useState(false);
  const formRef = useRef<HTMLFormElement>(null);

  async function submit(mode: "login" | "register"): Promise<void> {
    setBusy(true);
    setError(undefined);
    try {
      if (mode === "login") {
        await auth.login(email, password);
      } else {
        await auth.register(email, password);
      }
      setPassword("");
    } catch (caught) {
      setError(describeError(caught));
    } finally {
      setBusy(false);
    }
  }

  if (user !== null) {
    return (
      <div className="auth">
        <span>{user.email}</span>
        <button type="button" onClick={() => void auth.logout()}>
          Sign out
        </button>
      </div>
    );
  }

  return (
    <form
      ref={formRef}
      className="auth"
      onSubmit={(event: FormEvent) => {
        event.preventDefault();
        void submit("login");
      }}
    >
      <input
        type="email"
        placeholder="email"
        value={email}
        onChange={(event) => setEmail(event.target.value)}
        required
      />
      <input
        type="password"
        placeholder="password (12+ chars)"
        value={password}
        onChange={(event) => setPassword(event.target.value)}
        minLength={12}
        required
      />
      <button type="submit" disabled={busy}>
        Sign in
      </button>
      <button
        type="button"
        disabled={busy}
        onClick={() => {
          // A type="button" skips the form's own validation, so run it here.
          const isValid = formRef.current?.reportValidity() ?? false;
          if (isValid) {
            void submit("register");
          }
        }}
      >
        Register
      </button>
      {error !== undefined && <span className="error">{error}</span>}
    </form>
  );
}
