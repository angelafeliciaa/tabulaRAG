import { useEffect, useState } from "react";
import logo from "../images/logo.png";
import { getGithubClientId, getGoogleClientId, generateOAuthState, type OAuthProviders } from "../api";

export default function Login() {
  const [loading, setLoading] = useState<"github" | "google" | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [providers, setProviders] = useState<OAuthProviders>({ github: false, google: false });
  const [checking, setChecking] = useState(true);

  useEffect(() => {
    let mounted = true;
    async function check() {
      const [gh, go] = await Promise.allSettled([
        getGithubClientId(),
        getGoogleClientId(),
      ]);
      if (mounted) {
        setProviders({
          github: gh.status === "fulfilled",
          google: go.status === "fulfilled",
        });
        setChecking(false);
      }
    }
    check();
    return () => { mounted = false; };
  }, []);

  async function handleGithubLogin() {
    setLoading("github");
    setError(null);
    try {
      const clientId = await getGithubClientId();
      const state = generateOAuthState();
      const redirectUri = `${window.location.origin}/auth/callback?provider=github`;
      window.location.href =
        `https://github.com/login/oauth/authorize?client_id=${clientId}&redirect_uri=${encodeURIComponent(redirectUri)}&scope=read:user&state=${state}`;
    } catch {
      setError("Failed to start GitHub login. Is the server running?");
      setLoading(null);
    }
  }

  async function handleGoogleLogin() {
    setLoading("google");
    setError(null);
    try {
      const clientId = await getGoogleClientId();
      const state = generateOAuthState();
      const redirectUri = `${window.location.origin}/auth/callback?provider=google`;
      window.location.href =
        `https://accounts.google.com/o/oauth2/v2/auth?client_id=${clientId}&redirect_uri=${encodeURIComponent(redirectUri)}&response_type=code&scope=${encodeURIComponent("openid email profile")}&access_type=offline&prompt=select_account&state=${state}`;
    } catch {
      setError("Failed to start Google login. Is the server running?");
      setLoading(null);
    }
  }

  return (
    <div className="login-page">
      <div className="login-card">
        <img src={logo} alt="TabulaRAG" className="login-logo" />
        <h1 className="login-title">TabulaRAG</h1>
        <p className="login-subtitle">Sign in to continue</p>
        {error && (
          <p className="login-error" role="alert">
            {error}
          </p>
        )}
        {checking ? (
          <p className="login-subtitle">Loading...</p>
        ) : (
          <>
            {providers.github && (
              <button
                type="button"
                className="login-btn github-btn"
                onClick={handleGithubLogin}
                disabled={loading !== null}
              >
                <svg
                  className="github-icon"
                  viewBox="0 0 24 24"
                  width="20"
                  height="20"
                  fill="currentColor"
                >
                  <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z" />
                </svg>
                {loading === "github" ? "Redirecting..." : "Sign in with GitHub"}
              </button>
            )}
            {providers.google && (
              <button
                type="button"
                className="login-btn google-btn"
                onClick={handleGoogleLogin}
                disabled={loading !== null}
              >
                <svg
                  className="google-icon"
                  viewBox="0 0 24 24"
                  width="20"
                  height="20"
                >
                  <path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 01-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z" fill="#4285F4"/>
                  <path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/>
                  <path d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" fill="#FBBC05"/>
                  <path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" fill="#EA4335"/>
                </svg>
                {loading === "google" ? "Redirecting..." : "Sign in with Google"}
              </button>
            )}
            {!providers.github && !providers.google && (
              <p className="login-error" role="alert">
                No OAuth providers configured. Check server settings.
              </p>
            )}
          </>
        )}
      </div>
    </div>
  );
}
