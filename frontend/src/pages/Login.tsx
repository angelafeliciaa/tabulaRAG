import { useState } from "react";
import logo from "../images/logo.png";
import { verifyApiKey } from "../api";

interface LoginProps {
  onLogin: () => void;
}

export default function Login({ onLogin }: LoginProps) {
  const [key, setKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = key.trim();
    if (!trimmed) {
      setError("Please enter an API key.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const result = await verifyApiKey(trimmed);
      if (result.valid) {
        localStorage.setItem("tabularag_api_key", trimmed);
        onLogin();
      } else {
        setError("Invalid or missing API key.");
      }
    } catch {
      setError("Invalid or missing API key.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <img src={logo} alt="TabulaRAG" className="login-logo" />
        <h1 className="login-title">TabulaRAG</h1>
        <p className="login-subtitle">Enter your API key to continue</p>
        <form onSubmit={handleSubmit} className="login-form">
          <input
            type="password"
            className="login-input"
            placeholder="API key"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            autoFocus
            autoComplete="current-password"
          />
          {error && <p className="login-error">{error}</p>}
          <button
            type="submit"
            className="login-btn"
            disabled={loading}
          >
            {loading ? "Connecting..." : "Connect"}
          </button>
        </form>
      </div>
    </div>
  );
}
