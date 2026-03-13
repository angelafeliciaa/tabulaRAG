const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

const TOKEN_KEY = "tabularag_token";
const USER_KEY = "tabularag_user";

export interface AuthUser {
  login: string;
  name: string;
  avatar_url: string;
}

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY);
}

export function getUser(): AuthUser | null {
  const raw = localStorage.getItem(USER_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw) as AuthUser;
  } catch {
    return null;
  }
}

export function isAuthenticated(): boolean {
  return getToken() !== null;
}

export function authHeaders(): Record<string, string> {
  const token = getToken();
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}

export function logout(): void {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
}

async function authFetch(
  input: string | URL,
  init?: RequestInit,
): Promise<Response> {
  const res = await fetch(
    typeof input === "string" ? input : input.toString(),
    init,
  );
  if (res.status === 401) {
    logout();
    window.location.replace("/");
  }
  return res;
}

export async function getGithubClientId(): Promise<string> {
  const res = await fetch(`${API_BASE}/auth/github`);
  if (!res.ok) throw new Error("GitHub OAuth not configured");
  const data = (await res.json()) as { client_id: string };
  return data.client_id;
}

export async function exchangeGithubCode(
  code: string,
): Promise<{ token: string; user: AuthUser }> {
  const res = await fetch(`${API_BASE}/auth/github/callback`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(err || "GitHub authentication failed");
  }
  const data = (await res.json()) as { token: string; user: AuthUser };
  localStorage.setItem(TOKEN_KEY, data.token);
  localStorage.setItem(USER_KEY, JSON.stringify(data.user));
  return data;
}

export type ServerStatus = "Online" | "Offline" | "Unknown";
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

export interface ColumnMeta {
  original_name: string | null;
  normalized_name: string;
}

export interface TableSlice {
  dataset_id: number;
  offset: number;
  limit: number;
  row_count: number;
  column_count: number;
  has_header: boolean;
  columns: string[];
  columns_meta?: ColumnMeta[];
  rows: TableRow[];
  /** When server-side sort is used, row indices for each row (for correct # column). */
  row_indices?: number[];
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
  columns_meta?: ColumnMeta[];
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

/** Flatten row cells that are { original, normalized } to a single value (normalized). */
function flattenRowToNormalized(row: TableRow): TableRow {
  const out: TableRow = {};
  for (const [col, val] of Object.entries(row)) {
    if (
      val != null
      && typeof val === "object"
      && !Array.isArray(val)
      && "normalized" in val
      && "original" in val
    ) {
      out[col] = (val as { normalized?: unknown }).normalized;
    } else {
      out[col] = val;
    }
  }
  return out;
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

    const token = getToken();
    if (token) {
      xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    }

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

      if (xhr.status === 401) {
        logout();
        window.location.replace("/");
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

export async function listTables(options?: {
  includePending?: boolean;
}): Promise<TableSummary[]> {
  const url = new URL(`${API_BASE}/tables`);
  if (options?.includePending) {
    url.searchParams.set("include_pending", "true");
  }

  const res = await authFetch(url.toString(), { headers: authHeaders() });
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

  const res = await authFetch(url.toString(), { headers: authHeaders() });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as TableIndexStatus[];
}

export type SliceOptions = {
  /** If true (default), flatten each row to normalized values only. If false, return raw { original, normalized } per cell for toggle support. */
  flatten?: boolean;
};

export type SliceSort = {
  sortColumn: string;
  sortDirection: "asc" | "desc";
};

export async function getSlice(
  datasetId: number,
  rowFrom: number,
  rowTo: number,
  options?: SliceOptions & { sort?: SliceSort | null },
): Promise<TableSlice> {
  const offset = Math.max(0, rowFrom);
  const limit = Math.max(1, rowTo - rowFrom);
  const url = new URL(`${API_BASE}/tables/${datasetId}/slice`);
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("limit", String(limit));
  if (options?.sort?.sortColumn) {
    url.searchParams.set("sort_column", options.sort.sortColumn);
    url.searchParams.set("sort_direction", options.sort.sortDirection);
  }

  const res = await authFetch(url.toString(), { headers: authHeaders() });
  if (!res.ok) {
    throw new Error(await res.text());
  }

  const data = (await res.json()) as TableSliceApiResponse;
  const rawRows = (data.rows || []).map((row) =>
    normalizeRowData(row.data ?? row.row_data),
  );
  const flattenToNormalized = options?.flatten !== false;
  const rows = flattenToNormalized
    ? rawRows.map(flattenRowToNormalized)
    : rawRows;
  const row_indices = (data.rows || []).map((r) => r.row_index);

  return {
    dataset_id: data.dataset_id,
    offset: data.offset,
    limit: data.limit,
    row_count: data.row_count,
    column_count: data.column_count,
    has_header: data.has_header,
    columns: data.columns,
    columns_meta: data.columns_meta,
    rows,
    row_indices,
  };
}

export async function getHighlight(highlightId: string): Promise<HighlightResponse> {
  const res = await authFetch(`${API_BASE}/highlights/${highlightId}`, {
    headers: authHeaders(),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as HighlightResponse;
}

export async function deleteTable(
  datasetId: number,
  options?: { keepalive?: boolean },
): Promise<{ deleted: number }> {
  const res = await authFetch(`${API_BASE}/tables/${datasetId}`, {
    method: "DELETE",
    keepalive: options?.keepalive,
    headers: authHeaders(),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as { deleted: number };
}

export async function renameTable(
  datasetId: number,
  name: string,
): Promise<{ name: string }> {
  const res = await authFetch(`${API_BASE}/tables/${datasetId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify({ name }),
  });
  if (!res.ok) {
    throw new Error(await res.text());
  }
  return (await res.json()) as { name: string };
}

export async function getServerStatus(): Promise<{ status: ServerStatus }> {
  try {
    const res = await fetch(`${API_BASE}/health/deps`);
    if (!res.ok) {
      return { status: "Offline" };
    }

    const data = (await res.json()) as { status?: string };
    if (data.status === "ok") {
      return { status: "Online" };
    }
    return { status: "Unknown" };
  } catch {
    return { status: "Offline" };
  }
}

export type AggregateResponse = {
  dataset_id: number;
  metric_column: string | null;
  group_by_column: string | null;
  rowsResult: { group_value: string | null; aggregate_value: number }[];
  sql_query: string;
  url: string | null;
};

export async function aggregate(params: unknown): Promise<AggregateResponse> {
  const res = await authFetch(`${API_BASE}/aggregate`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}

export type FilterResponse = {
  dataset_id: number;
  rowsResult: { row_index: number; row_data: Record<string, unknown> }[];
  row_count: number;
  sql_query: string;
  url: string | null;
};

export async function filterRows(params: unknown): Promise<FilterResponse> {
  const res = await authFetch(`${API_BASE}/filter`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(params),
  });
  if (!res.ok) throw new Error(await res.text());
  return res.json();
}
