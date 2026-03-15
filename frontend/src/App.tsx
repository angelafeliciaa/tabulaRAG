import { useEffect, useState } from "react";
import { Link, Route, Routes, useLocation } from "react-router-dom";
import { logout, getUser, getServerStatus, type ServerStatus } from "./api";
import logo from "./images/logo.png";
import moonIcon from "./images/moon.png";
import sunIcon from "./images/sun.png";
import HighlightView from "./pages/HighlightView";
import TableView from "./pages/TableView";
import Upload from "./pages/Upload";
import AggregateTableView from "./pages/AggregateTable";
import AuthCallback from "./pages/AuthCallback";

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
  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    let pointerActive = false;
    function handlePointerDown() {
      pointerActive = true;
    }
    function handleKeyDown() {
      pointerActive = false;
    }
    function handleFocusIn(e: FocusEvent) {
      if (!pointerActive) return;
      const el = e.target as Node;
      if (
        el &&
        el instanceof HTMLElement &&
        (el.tagName === "BUTTON" || el.tagName === "A" || el.getAttribute("role") === "button")
      ) {
        requestAnimationFrame(() => el.blur());
      }
    }
    document.addEventListener("pointerdown", handlePointerDown, true);
    document.addEventListener("keydown", handleKeyDown, true);
    document.addEventListener("focusin", handleFocusIn, true);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown, true);
      document.removeEventListener("keydown", handleKeyDown, true);
      document.removeEventListener("focusin", handleFocusIn, true);
    };
  }, []);

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

  const user = getUser();

  function handleLogout() {
    logout();
  }

  return (
    <div className="app-shell">
      {location.pathname !== "/" && (
        <Link className="app-brand" to="/" aria-label="Go to home">
          <img src={logo} alt="" aria-hidden="true" />
          <span className="app-brand-text">TabulaRAG</span>
        </Link>
      )}

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
            hidden
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
          <Route path="/auth/callback" element={<AuthCallback onLogin={() => {}} />} />
        </Routes>
      </main>
    </div>
  );
}
