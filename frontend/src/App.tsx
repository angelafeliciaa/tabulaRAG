import { useEffect, useState } from "react";
import { Route, Routes } from "react-router-dom";
import { isAuthenticated, logout, getUser, getServerStatus, type ServerStatus } from "./api";
import moonIcon from "./images/moon.png";
import sunIcon from "./images/sun.png";
import HighlightView from "./pages/HighlightView";
import TableView from "./pages/TableView";
import Upload from "./pages/Upload";
import AggregateTableView from "./pages/AggregateTable";
import Login from "./pages/Login";
import AuthCallback from "./pages/AuthCallback";

export default function App() {
  const [theme, setTheme] = useState<"dark" | "light">(() => {
    const storedTheme = window.localStorage.getItem("theme");
    if (storedTheme === "light" || storedTheme === "dark") {
      return storedTheme;
    }
    return "light";
  });
  const [serverStatus, setServerStatus] = useState<ServerStatus>("Unknown");
  const [authed, setAuthed] = useState<boolean>(() => isAuthenticated());

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

  if (!authed) {
    return (
      <Routes>
        <Route
          path="/auth/callback"
          element={<AuthCallback onLogin={() => setAuthed(true)} />}
        />
        <Route path="*" element={<Login />} />
      </Routes>
    );
  }

  const user = getUser();

  function handleLogout() {
    logout();
    setAuthed(false);
  }

  return (
    <div className="app-shell">
      <div className={`server-status ${serverStatus}`}>
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
          aria-label="Toggle theme"
          aria-pressed={theme === "light"}
          type="button"
        >
          <span className="toggle-track">
            <span className="toggle-thumb">
              <img src={theme === "dark" ? moonIcon : sunIcon} alt="" />
            </span>
          </span>
        </button>
      </div>

      <div className="content">
        <Routes>
          <Route path="/" element={<Upload />} />
          <Route path="/tables/virtual" element={<AggregateTableView />} />
          <Route path="/tables/:datasetId" element={<TableView />} />
          <Route path="/highlight/:highlightId" element={<HighlightView />} />
          <Route path="/auth/callback" element={<AuthCallback onLogin={() => setAuthed(true)} />} />
        </Routes>
      </div>
    </div>
  );
}
