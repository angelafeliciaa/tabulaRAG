import { useEffect, useState } from "react";
import { getServerStatus } from "./api";

export default function App() {
  const [serverStatus, setServerStatus] = useState<
    "online" | "offline" | "unknown"
  >("unknown");

  useEffect(() => {
    let mounted = true;

    async function checkStatus() {
    const status = await getServerStatus();
    if (mounted) setServerStatus(status);
}

    checkStatus();
    const id = window.setInterval(checkStatus, 1000);
    return () => {
      mounted = false;
      window.clearInterval(id);
    };
  }, []);

  return (
    <div className="app-shell">
      <div className={`server-status ${serverStatus}`}>
        <span className="status-dot" />
        <span>
          Server Status:{" "}
          {serverStatus === "online"
            ? "Online"
            : serverStatus === "offline"
              ? "Offline"
              : "Unknown"}
        </span>
      </div>
    </div>
  );
}
