const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

// Tables
export async function listTables() {
  const res = await fetch(`${API_BASE}/tables`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function getSlice(datasetId: number, offset = 0, limit = 30) {
  const res = await fetch(
    `${API_BASE}/tables/${datasetId}/slice?offset=${offset}&limit=${limit}`,
  );
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function deleteTable(datasetId: number) {
  const res = await fetch(`${API_BASE}/tables/${datasetId}`, {
    method: "DELETE",
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function renameTable(datasetId: number, name: string) {
  const res = await fetch(`${API_BASE}/tables/${datasetId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) throw new Error(await res.text());
}

// Upload and Ingest
export async function uploadTable(file: File, name: string) {
  const form = new FormData();
  form.append("file", file);
  form.append("dataset_name", name);
  const res = await fetch(`${API_BASE}/ingest`, {
    method: "POST",
    body: form,
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

// Highlights
export async function getHighlight(highlightId: string) {
  const res = await fetch(`${API_BASE}/highlights/${highlightId}`);
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

// Health
export async function getServerStatus(): Promise<"online" | "offline"> {
    try {
        const res = await fetch(`${API_BASE}/health`);
        console.log("health response:", res.status);
        if (!res.ok) return "offline";
        const data = await res.json();
        console.log("health data:", data);
        return data.status === "ok" ? "online" : "offline";
    } catch (e) {
        console.log("health error:", e);
        return "offline";
    }
}
