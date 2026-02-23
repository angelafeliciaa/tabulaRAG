import { useEffect, useState } from "react";
import { Route, Routes } from "react-router-dom";
import { getMcpStatus, type ServerStatus } from "./api";
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
  const [mcpStatus, setMcpStatus] = useState<ServerStatus>("unknown");

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("theme", theme);
  }, [theme]);

  useEffect(() => {
    let mounted = true;

    async function checkStatus() {
      const result = await getMcpStatus();
      if (mounted) {
        setMcpStatus(result.status);
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
      <div className={`mcp-status ${mcpStatus}`}>
        <span className="status-dot" />
        <span>
          MCP Server: {mcpStatus === "online" ? "Online" : mcpStatus === "offline" ? "Offline" : "Unknown"}
        </span>
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
        <div className="toggle-label">{theme === "dark" ? "Dark mode" : "Light mode"}</div>
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
