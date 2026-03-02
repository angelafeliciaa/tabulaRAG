import { useEffect, useState } from "react";
import { Route, Routes } from "react-router-dom";
import { getServerStatus, type ServerStatus } from "./api";
import moonIcon from "./images/moon.png";
import sunIcon from "./images/sun.png";
import HighlightView from "./pages/HighlightView";
import TableView from "./pages/TableView";
import Upload from "./pages/Upload";

export default function App() {
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

  return (
    <div className="app-shell">
      <div className={`server-status ${serverStatus}`}>
        <span className="status-dot" />
        <span>Server Connection: {serverStatus}</span>
      </div>

      <div className="theme-toggle-wrap">
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
        <div className="toggle-label">
          {theme === "dark" ? "Dark mode" : "Light mode"}
        </div>
      </div>

      <div className="content">
        <Routes>
          <Route path="/" element={<Upload />} />
          <Route path="/tables/:datasetId" element={<TableView />} />
          <Route path="/highlight/:highlightId" element={<HighlightView />} />
        </Routes>
      </div>
    </div>
  );
}
