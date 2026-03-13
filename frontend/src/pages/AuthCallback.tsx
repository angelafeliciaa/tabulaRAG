import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { exchangeGithubCode } from "../api";

interface AuthCallbackProps {
  onLogin: () => void;
}

export default function AuthCallback({ onLogin }: AuthCallbackProps) {
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const code = new URLSearchParams(window.location.search).get("code");

  useEffect(() => {
    if (!code) return;

    exchangeGithubCode(code)
      .then(() => {
        onLogin();
        navigate("/", { replace: true });
      })
      .catch(() => {
        setError("GitHub authentication failed. Please try again.");
      });
  }, [code, onLogin, navigate]);

  if (!code) {
    return (
      <div className="login-page">
        <div className="login-card">
          <p className="login-error" role="alert">
            No authorization code received from GitHub.
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
          Signing in with GitHub...
        </p>
      </div>
    </div>
  );
}
