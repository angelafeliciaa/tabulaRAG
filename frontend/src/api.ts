const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export type ServerStatus = "online" | "offline" | "unknown";
export type IndexJobState = "queued" | "indexing" | "ready" | "error";

export type TableRow = Record<string, unknown>;

export interface TableSummary {
  dataset_id: number;
  name: string;
  source_filename: string | null;
  row_count: number;
  column_count: number;
  created_at: string;
}

export interface TableSlice {
  dataset_id: number;
  offset: number;
  limit: number;
  row_count: number;
  column_count: number;
  has_header: boolean;
  columns: string[];
  rows: TableRow[];
}

export interface TableIndexStatus {
  dataset_id: number;
  state: IndexJobState;
  progress: number;
  processed_rows: number;
  total_rows: number;
  message: string;
  started_at: string | null;
  updated_at: string | null;
  finished_at: string | null;
}

interface TableSliceApiRow {
  row_index: number;
  data?: unknown;
  row_data?: unknown;
}

interface TableSliceApiResponse {
  dataset_id: number;
  offset: number;
  limit: number;
  row_count: number;
  column_count: number;
  has_header: boolean;
  columns: string[];
  rows: TableSliceApiRow[];
}

function normalizeRowData(raw: unknown): TableRow {
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    return raw as TableRow;
  }

  if (typeof raw === "string") {
    try {
      let parsed: unknown = JSON.parse(raw);
      if (typeof parsed === "string") {
        parsed = JSON.parse(parsed);
      }
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed as TableRow;
      }
    } catch {
      return {};
    }
  }

  return {};
}

export interface HighlightResponse {
  highlight_id: string;
  dataset_id: number;
  row_index: number;
  column: string;
  value: unknown;
  row_context: TableRow;
}

interface IngestResponse {
  dataset_id: number;
  name: string;
  rows: number;
  columns: number;
  delimiter: string;
  has_header: boolean;
}

export interface UploadProgress {
  percent: number;
  phase: "uploading" | "processing";
}

export async function uploadTable(
  file: File,
  name: string,
  onProgress?: (progress: UploadProgress) => void,
): Promise<IngestResponse> {
  const form = new FormData();
  form.append("file", file);
  form.append("has_header", "true");

  const trimmed = name.trim();
  if (trimmed) {
    form.append("dataset_name", trimmed);
  }

  return await new Promise<IngestResponse>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    let processingTimer: number | null = null;

    const report = (percent: number, phase: UploadProgress["phase"]) => {
      if (!onProgress) {
        return;
      }
      onProgress({ percent: Math.max(0, Math.min(100, percent)), phase });
    };

    const stopProcessingTimer = () => {
      if (processingTimer !== null) {
        window.clearInterval(processingTimer);
        processingTimer = null;
      }
    };

    xhr.open("POST", `${API_BASE}/ingest`);

    xhr.upload.onloadstart = () => {
      report(2, "uploading");
    };

    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable) {
        return;
      }
      // Reserve final percentage points for server-side parse/store/indexing.
      const uploadPercent = (event.loaded / event.total) * 82;
      report(uploadPercent, "uploading");
    };

    xhr.upload.onload = () => {
      let processingPercent = 82;
      const processingStart = Date.now();
      report(processingPercent, "processing");
      processingTimer = window.setInterval(() => {
        const elapsedMs = Date.now() - processingStart;
        // Keep progress moving while backend parses/stores/indexes rows.
        const easedTarget = 82 + 17.2 * (1 - Math.exp(-elapsedMs / 9000));
        processingPercent = Math.min(
          99.7,
          Math.max(processingPercent + 0.12, easedTarget),
        );
        report(processingPercent, "processing");
      }, 220);
    };

    xhr.onreadystatechange = () => {
      if (xhr.readyState !== XMLHttpRequest.DONE) {
        return;
      }

      stopProcessingTimer();

      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const response = JSON.parse(xhr.responseText) as IngestResponse;
          report(100, "processing");
          resolve(response);
        } catch {
          reject(new Error("Invalid upload response format."));
        }
        return;
      }

      const detail = xhr.responseText?.trim();
      reject(new Error(detail || `Upload failed with status ${xhr.status}.`));
    };

    xhr.onerror = () => {
      stopProcessingTimer();
      reject(new Error("Network error while uploading file."));
    };

    xhr.onabort = () => {
      stopProcessingTimer();
      reject(new Error("Upload was aborted."));
    };

    xhr.send(form);
  });
}

export async function listTables(): Promise<TableSummary[]> {
  const res = await fetch(`${API_BASE}/tables`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as TableSummary[];
}

export async function listIndexStatus(
  datasetIds?: number[],
): Promise<TableIndexStatus[]> {
  const url = new URL(`${API_BASE}/tables/index-status`);
  (datasetIds || []).forEach((datasetId) => {
    url.searchParams.append("dataset_id", String(datasetId));
  });

  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as TableIndexStatus[];
}

export async function getSlice(
  datasetId: number,
  rowFrom: number,
  rowTo: number,
): Promise<TableSlice> {
  const offset = Math.max(0, rowFrom);
  const limit = Math.max(1, rowTo - rowFrom);
  const url = new URL(`${API_BASE}/tables/${datasetId}/slice`);
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("limit", String(limit));

  const res = await fetch(url.toString());
  if (!res.ok) {
    throw new Error(await res.text());
  }

  const data = (await res.json()) as TableSliceApiResponse;
  return {
    dataset_id: data.dataset_id,
    offset: data.offset,
    limit: data.limit,
    row_count: data.row_count,
    column_count: data.column_count,
    has_header: data.has_header,
    columns: data.columns,
    rows: (data.rows || []).map((row) => normalizeRowData(row.data ?? row.row_data)),
  };
}

export async function getFullTableSlice(
  datasetId: number,
  rowCountHint: number,
  chunkSize = 2000,
): Promise<TableSlice> {
  const normalizedRowCountHint = Math.max(0, Math.trunc(rowCountHint));
  const normalizedChunkSize = Math.max(1, Math.trunc(chunkSize));
  const initialLimit = Math.max(1, Math.min(normalizedChunkSize, normalizedRowCountHint || 1));
  const firstSlice = await getSlice(datasetId, 0, initialLimit);
  const totalRows = Math.max(normalizedRowCountHint, Math.max(0, firstSlice.row_count || 0));

  if (totalRows === 0) {
    return { ...firstSlice, offset: 0, limit: 0, rows: [] };
  }

  const rows: TableRow[] = [...firstSlice.rows];
  let nextOffset = rows.length;

  while (nextOffset < totalRows) {
    const nextLimit = Math.min(totalRows, nextOffset + normalizedChunkSize);
    const nextSlice = await getSlice(datasetId, nextOffset, nextLimit);
    if (!nextSlice.rows.length) {
      break;
    }
    rows.push(...nextSlice.rows);
    nextOffset += nextSlice.rows.length;
  }

  return {
    ...firstSlice,
    offset: 0,
    limit: rows.length,
    rows,
  };
}

export async function getHighlight(highlightId: string): Promise<HighlightResponse> {
  const res = await fetch(`${API_BASE}/highlights/${highlightId}`);
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as HighlightResponse;
}

export async function deleteTable(
  datasetId: number,
  options?: { keepalive?: boolean },
): Promise<{ deleted: number }> {
  const res = await fetch(`${API_BASE}/tables/${datasetId}`, {
    method: "DELETE",
    keepalive: options?.keepalive,
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as { deleted: number };
}

export async function renameTable(datasetId: number, name: string): Promise<{ name: string }> {
  const res = await fetch(`${API_BASE}/tables/${datasetId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as { name: string };
}

export async function getMcpStatus(): Promise<{ status: ServerStatus }> {
  try {
    const res = await fetch(`${API_BASE}/mcp-status`);
    if (!res.ok) {
      return { status: "offline" };
    }

    const data = (await res.json()) as { status?: string };
    if (data.status === "ok") {
      return { status: "online" };
    }
    return { status: "unknown" };
  } catch {
    return { status: "offline" };
  }
}
