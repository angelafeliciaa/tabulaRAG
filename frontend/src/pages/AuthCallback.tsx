import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { exchangeGithubCode, exchangeGoogleCode, verifyOAuthState } from "../api";

interface AuthCallbackProps {
  onLogin: () => void;
}

export default function AuthCallback({ onLogin }: AuthCallbackProps) {
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const params = new URLSearchParams(window.location.search);
  const code = params.get("code");
  const state = params.get("state");
  const provider = params.get("provider") || "github";

  const stateValid = useMemo(() => verifyOAuthState(state), [state]);

  useEffect(() => {
    if (!code || !stateValid) return;

    async function exchange() {
      try {
        if (provider === "google") {
          await exchangeGoogleCode(code!);
        } else {
          await exchangeGithubCode(code!);
        }
        onLogin();
        navigate("/", { replace: true });
      } catch {
        setError(`${provider === "google" ? "Google" : "GitHub"} authentication failed. Please try again.`);
      }
    }

    exchange();
  }, [code, stateValid, provider, onLogin, navigate]);

  if (!code) {
    return (
      <div className="login-page">
        <div className="login-card">
          <p className="login-error" role="alert">
            No authorization code received.
          </p>
          <button
            type="button"
            className="login-btn"
            onClick={() => navigate("/", { replace: true })}
          >
            Back to login
          </button>
        </div>
      </div>
    );
  }

  if (!stateValid) {
    return (
      <div className="login-page">
        <div className="login-card">
          <p className="login-error" role="alert">
            Invalid OAuth state. Please try again.
          </p>
          <button
            type="button"
            className="login-btn"
            onClick={() => navigate("/", { replace: true })}
          >
            Back to login
          </button>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="login-page">
        <div className="login-card">
          <p className="login-error" role="alert">
            {error}
          </p>
          <button
            type="button"
            className="login-btn"
            onClick={() => navigate("/", { replace: true })}
          >
            Back to login
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <p className="login-subtitle" role="status" aria-live="polite">
          Signing in with {provider === "google" ? "Google" : "GitHub"}...
        </p>
      </div>
    </div>
  );
}
