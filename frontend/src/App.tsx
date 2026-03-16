import { useCallback, useEffect, useState } from "react";
import { Link, Route, Routes, useLocation } from "react-router-dom";
import { logout, getUser, isAuthenticated, getServerStatus, type ServerStatus } from "./api";
import logo from "./images/logo.png";
import moonIcon from "./images/moon.png";
import sunIcon from "./images/sun.png";
import HighlightView from "./pages/HighlightView";
import TableView from "./pages/TableView";
import Upload from "./pages/Upload";
import AggregateTableView from "./pages/AggregateTable";
import AuthCallback from "./pages/AuthCallback";
import Login from "./pages/Login";

export default function App() {
  const location = useLocation();
  useEffect(() => {
    window.scrollTo(0, 0);
    document.documentElement.scrollTop = 0;
    document.body.scrollTop = 0;
  }, [location.pathname, location.search]);

  const [theme, setTheme] = useState<"dark" | "light">(() => {
    const storedTheme = window.localStorage.getItem("theme");
    if (storedTheme === "light" || storedTheme === "dark") {
      return storedTheme;
    }
    return "light";
  });
  const [serverStatus, setServerStatus] = useState<ServerStatus>("Unknown");
  const [authed, setAuthed] = useState(isAuthenticated());

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    let mounted = true;

    async function checkStatus() {
      const result = await getServerStatus();
      if (mounted) {
        setServerStatus(result.status);
      }
    }

    checkStatus();
    const intervalId = window.setInterval(checkStatus, 5000);

    return () => {
      mounted = false;
      window.clearInterval(intervalId);
    };
  }, []);

  const handleLogin = useCallback(() => {
    setAuthed(true);
  }, []);

  const user = getUser();

  function handleLogout() {
    logout();
    setAuthed(false);
  }

  // Show login for unauthenticated users (except the callback route)
  if (!authed && !window.location.pathname.startsWith("/auth/callback")) {
    return <Login />;
  }

  return (
    <div className="app-shell">
      <Link className="app-brand" to="/" aria-label="Go to home">
        <img src={logo} alt="" aria-hidden="true" />
      </Link>

      <a className="skip-link" href="#main-content">
        Skip to main content
      </a>

      <div
        className={`server-status ${serverStatus}`}
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        <span className="status-dot" />
        <span>Server Connection: {serverStatus}</span>
      </div>

      <div className="top-bar">
        <div className="user-menu">
          {user?.avatar_url && (
            <img src={user.avatar_url} alt="" className="user-avatar" />
          )}
          <span className="user-name">{user?.name || user?.login}</span>
          <button
            className="logout-btn"
            onClick={handleLogout}
            type="button"
          >
            Sign out
          </button>
        </div>

        <button
          className="theme-toggle"
          onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          aria-label={`Switch to ${theme === "dark" ? "light" : "dark"} theme`}
          aria-pressed={theme === "dark"}
          type="button"
        >
          <span className="sr-only">
            {theme === "dark" ? "Dark theme enabled" : "Light theme enabled"}
          </span>
          <span className="toggle-track">
            <span className="toggle-thumb">
              <img src={theme === "dark" ? moonIcon : sunIcon} alt="" />
            </span>
          </span>
        </button>
      </div>

      <main id="main-content" className="content" tabIndex={-1}>
        <Routes>
          <Route path="/" element={<Upload />} />
          <Route path="/tables/virtual" element={<AggregateTableView />} />
          <Route path="/tables/:datasetId" element={<TableView />} />
          <Route path="/highlight/:highlightId" element={<HighlightView />} />
          <Route path="/auth/callback" element={<AuthCallback onLogin={handleLogin} />} />
        </Routes>
      </main>
    </div>
  );
}
