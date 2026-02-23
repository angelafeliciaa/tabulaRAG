import { useCallback, useEffect, useRef, useState } from "react";
import {
  deleteTable,
  getSlice,
  listIndexStatus,
  listTables,
  renameTable,
  type TableIndexStatus,
  type UploadProgress,
  type TableSlice,
  type TableSummary,
  uploadTable,
} from "../api";
import DataTable from "../components/DataTable";
import logo from "../images/logo.png";
import uploadLogo from "../images/upload.png";

const PENDING_UPLOAD_SESSION_KEY = "tabularag_pending_upload";
const DELETE_UNDO_WINDOW_MS = 5600;
const SUCCESS_TOAST_MS = 2800;

type ToastState =
  | { kind: "success"; message: string }
  | { kind: "delete"; message: string };

type PendingDelete = {
  table: TableSummary;
  indexStatus: TableIndexStatus;
  previousActiveTableId: number | null;
  previousPreview: TableSlice | null;
  timeoutId: number;
};

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export default function Upload() {
  const [file, setFile] = useState<File | null>(null);
  const [name, setName] = useState("Uploaded Table");
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadPhase, setUploadPhase] = useState<"idle" | UploadProgress["phase"]>("idle");
  const [estimatedRows, setEstimatedRows] = useState<number | null>(null);
  const [preview, setPreview] = useState<TableSlice | null>(null);
  const [previewErr, setPreviewErr] = useState<string | null>(null);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [activeTableId, setActiveTableId] = useState<number | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [showScrollHint, setShowScrollHint] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingName, setEditingName] = useState("");
  const [reloadNotice, setReloadNotice] = useState<string | null>(null);
  const [deletingTableIds, setDeletingTableIds] = useState<Record<number, boolean>>({});
  const [indexStatusByTable, setIndexStatusByTable] = useState<
    Record<number, TableIndexStatus>
  >({});
  const tablesScrollRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const estimateJobRef = useRef(0);
  const toastTimerRef = useRef<number | null>(null);
  const pendingDeleteRef = useRef<PendingDelete | null>(null);

  async function estimateDataRows(nextFile: File): Promise<number | null> {
    try {
      // Keep estimation lightweight: sample only the file head instead of scanning full file.
      const sampleBytes = Math.min(nextFile.size, 512 * 1024);
      if (sampleBytes <= 0) {
        return null;
      }

      const sampleText = await nextFile.slice(0, sampleBytes).text();
      const lines = sampleText.split(/\r?\n/);
      const hasTrailingNewline = /\r?\n$/.test(sampleText);
      const sampledLineCount = Math.max(
        1,
        lines.length - (hasTrailingNewline ? 1 : 0),
      );
      const avgBytesPerLine = sampleBytes / sampledLineCount;
      if (!Number.isFinite(avgBytesPerLine) || avgBytesPerLine <= 0) {
        return null;
      }

      const estimatedTotalLines = Math.max(
        sampledLineCount,
        Math.round(nextFile.size / avgBytesPerLine),
      );
      // In this UI we always upload with has_header=true.
      return Math.max(0, estimatedTotalLines - 1);
    } catch {
      return null;
    }
  }

  function clearToastTimer() {
    if (toastTimerRef.current !== null) {
      window.clearTimeout(toastTimerRef.current);
      toastTimerRef.current = null;
    }
  }

  function showSuccessToast(message: string) {
    clearToastTimer();
    setToast({ kind: "success", message });
    toastTimerRef.current = window.setTimeout(() => {
      setToast((current) =>
        current?.kind === "success" && current.message === message ? null : current,
      );
      toastTimerRef.current = null;
    }, SUCCESS_TOAST_MS);
  }

  const refreshIndexStatuses = useCallback(async (nextTables: TableSummary[]) => {
    if (nextTables.length === 0) {
      setIndexStatusByTable({});
      return;
    }

    const datasetIds = nextTables.map((table) => table.dataset_id);
    const statuses = await listIndexStatus(datasetIds);
    const nextStatusByTable: Record<number, TableIndexStatus> = {};

    for (const status of statuses) {
      nextStatusByTable[status.dataset_id] = status;
    }

    for (const table of nextTables) {
      if (!nextStatusByTable[table.dataset_id]) {
        nextStatusByTable[table.dataset_id] = {
          dataset_id: table.dataset_id,
          state: "ready",
          progress: 100,
          processed_rows: table.row_count,
          total_rows: table.row_count,
          message: "Vector index is ready.",
          started_at: null,
          updated_at: null,
          finished_at: null,
        };
      }
    }

    setIndexStatusByTable(nextStatusByTable);
  }, []);

  const refresh = useCallback(async () => {
    const nextTables = await listTables();
    setTables(nextTables);
    try {
      await refreshIndexStatuses(nextTables);
    } catch {
      // Keep table list usable even if status polling fails.
    }
  }, [refreshIndexStatuses]);

  useEffect(() => {
    const pendingRaw = window.sessionStorage.getItem(PENDING_UPLOAD_SESSION_KEY);
    if (!pendingRaw) {
      return;
    }

    let fileLabel = "a previous file";
    try {
      const pending = JSON.parse(pendingRaw) as { file_name?: string };
      if (pending.file_name && pending.file_name.trim()) {
        fileLabel = pending.file_name;
      }
    } catch {
      // Ignore parse failures and show a generic message.
    }

    window.sessionStorage.removeItem(PENDING_UPLOAD_SESSION_KEY);
    setReloadNotice(
      `Page was reloaded during upload for ${fileLabel}. Check Uploaded tables below for the result.`,
    );
  }, []);

  useEffect(() => {
    refresh().catch((error: unknown) => {
      setErr(getErrorMessage(error));
    });
  }, [refresh]);

  useEffect(() => {
    if (!busy) {
      return;
    }

    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };

    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => {
      window.removeEventListener("beforeunload", handleBeforeUnload);
    };
  }, [busy]);

  useEffect(() => {
    return () => {
      clearToastTimer();
      const pendingDelete = pendingDeleteRef.current;
      if (pendingDelete) {
        window.clearTimeout(pendingDelete.timeoutId);
        pendingDeleteRef.current = null;
        void deleteTable(pendingDelete.table.dataset_id).catch(() => {
          // Best-effort cleanup on unmount/navigation.
        });
      }
    };
  }, []);

  useEffect(() => {
    if (tables.length === 0) {
      setIndexStatusByTable({});
      return;
    }

    const timer = window.setInterval(() => {
      refreshIndexStatuses(tables).catch(() => {
        // Keep polling best-effort.
      });
    }, 1400);

    return () => {
      window.clearInterval(timer);
    };
  }, [tables, refreshIndexStatuses]);

  useEffect(() => {
    const element = tablesScrollRef.current;
    if (!element) {
      return;
    }

    const updateHint = () => {
      const atBottom = element.scrollTop + element.clientHeight >= element.scrollHeight - 4;
      const canScroll = element.scrollHeight > element.clientHeight + 2;
      setShowScrollHint(canScroll && !atBottom);
    };

    updateHint();
    element.addEventListener("scroll", updateHint);
    window.addEventListener("resize", updateHint);

    return () => {
      element.removeEventListener("scroll", updateHint);
      window.removeEventListener("resize", updateHint);
    };
  }, [tables.length]);

  async function loadPreview(datasetId: number) {
    setActiveTableId(datasetId);
    setPreviewBusy(true);
    setPreviewErr(null);

    try {
      const slice = await getSlice(datasetId, 0, 30);
      setPreview(slice);
    } catch (error: unknown) {
      setPreviewErr(getErrorMessage(error));
      setPreview(null);
    } finally {
      setPreviewBusy(false);
    }
  }

  async function onUpload() {
    if (!file) {
      return;
    }

    setReloadNotice(null);
    window.sessionStorage.setItem(
      PENDING_UPLOAD_SESSION_KEY,
      JSON.stringify({
        file_name: file.name,
        dataset_name: name.trim() || file.name,
        started_at: new Date().toISOString(),
      }),
    );

    setBusy(true);
    setErr(null);
    setStatus("Uploading file...");
    setUploadPhase("uploading");
    setUploadProgress(2);

    try {
      const result = await uploadTable(file, name, (progress) => {
        setUploadPhase(progress.phase);
        setUploadProgress((previous) => Math.max(previous, progress.percent));
        setStatus(
          progress.phase === "uploading"
            ? "Uploading file..."
            : "Processing table rows...",
        );
      });
      setUploadProgress(100);
      setStatus(null);
      await refresh();
      await loadPreview(result.dataset_id);
      showSuccessToast(
        `File Uploaded Successfully`,
      );
      setFile(null);
      setName("Uploaded Table");
      window.sessionStorage.removeItem(PENDING_UPLOAD_SESSION_KEY);
    } catch (error: unknown) {
      setErr(getErrorMessage(error));
      setStatus(null);
      setUploadProgress(0);
      window.sessionStorage.removeItem(PENDING_UPLOAD_SESSION_KEY);
    } finally {
      setUploadPhase("idle");
      setBusy(false);
    }
  }

  function defaultIndexStatusForTable(table: TableSummary): TableIndexStatus {
    return {
      dataset_id: table.dataset_id,
      state: "ready",
      progress: 100,
      processed_rows: table.row_count,
      total_rows: table.row_count,
      message: "Vector index is ready.",
      started_at: null,
      updated_at: null,
      finished_at: null,
    };
  }

  function restoreDeletedTable(pending: PendingDelete) {
    setTables((previous) => {
      if (previous.some((table) => table.dataset_id === pending.table.dataset_id)) {
        return previous;
      }
      const next = [...previous, pending.table];
      next.sort((a, b) => b.dataset_id - a.dataset_id);
      return next;
    });
    setIndexStatusByTable((previous) => ({
      ...previous,
      [pending.table.dataset_id]: pending.indexStatus,
    }));

    if (pending.previousActiveTableId === pending.table.dataset_id) {
      setActiveTableId(pending.table.dataset_id);
      setPreview(pending.previousPreview);
    }
  }

  async function commitPendingDelete(pending: PendingDelete) {
    const datasetId = pending.table.dataset_id;
    setDeletingTableIds((previous) => ({ ...previous, [datasetId]: true }));

    try {
      await deleteTable(datasetId);
      await refresh();
    } catch (error: unknown) {
      setErr(getErrorMessage(error));
      restoreDeletedTable(pending);
    } finally {
      setDeletingTableIds((previous) => {
        const next = { ...previous };
        delete next[datasetId];
        return next;
      });
    }
  }

  async function onDelete(datasetId: number) {
    if (busy || deletingTableIds[datasetId]) {
      return;
    }
    const table = tables.find((current) => current.dataset_id === datasetId);
    if (!table) {
      return;
    }

    // If another delete is pending undo, commit it immediately before starting a new one.
    const previousPendingDelete = pendingDeleteRef.current;
    if (previousPendingDelete) {
      window.clearTimeout(previousPendingDelete.timeoutId);
      pendingDeleteRef.current = null;
      void commitPendingDelete(previousPendingDelete);
    }

    clearToastTimer();

    const pendingDelete: PendingDelete = {
      table,
      indexStatus: indexStatusByTable[datasetId] || defaultIndexStatusForTable(table),
      previousActiveTableId: activeTableId,
      previousPreview: preview,
      timeoutId: 0,
    };

    setTables((previous) => previous.filter((current) => current.dataset_id !== datasetId));
    setIndexStatusByTable((previous) => {
      const next = { ...previous };
      delete next[datasetId];
      return next;
    });
    if (activeTableId === datasetId) {
      setActiveTableId(null);
      setPreview(null);
    }

    const timeoutId = window.setTimeout(() => {
      const currentPendingDelete = pendingDeleteRef.current;
      if (!currentPendingDelete || currentPendingDelete.table.dataset_id !== datasetId) {
        return;
      }
      pendingDeleteRef.current = null;
      setToast(null);
      void commitPendingDelete(currentPendingDelete);
    }, DELETE_UNDO_WINDOW_MS);

    pendingDelete.timeoutId = timeoutId;
    pendingDeleteRef.current = pendingDelete;
    setToast({
      kind: "delete",
      message: `Successfully deleted table '${table.name}'`,
    });
  }

  function onUndoDelete() {
    const pendingDelete = pendingDeleteRef.current;
    if (!pendingDelete) {
      return;
    }
    window.clearTimeout(pendingDelete.timeoutId);
    pendingDeleteRef.current = null;
    restoreDeletedTable(pendingDelete);
    showSuccessToast(`Restored table '${pendingDelete.table.name}'`);
  }

  async function onRename(datasetId: number) {
    if (busy) {
      return;
    }

    const nextName = editingName.trim();
    if (!nextName) {
      setErr("Name cannot be empty.");
      return;
    }

    try {
      await renameTable(datasetId, nextName);
      setEditingId(null);
      setEditingName("");
      await refresh();
    } catch (error: unknown) {
      setErr(getErrorMessage(error));
    }
  }

  function onSelectFile(nextFile: File | null) {
    if (busy) {
      return;
    }
    setFile(nextFile);
    setStatus(null);

    if (!nextFile) {
      setEstimatedRows(null);
      return;
    }

    if (nextFile) {
      const withoutExt = nextFile.name.replace(/\.[^.]+$/, "");
      setName(withoutExt || nextFile.name);
    }

    setEstimatedRows(null);
    const estimateJobId = estimateJobRef.current + 1;
    estimateJobRef.current = estimateJobId;
    estimateDataRows(nextFile).then((rows) => {
      if (estimateJobRef.current !== estimateJobId) {
        return;
      }
      setEstimatedRows(rows);
    });
  }

  const progressLabel =
    uploadPhase === "uploading"
      ? "Step 1/2: Uploading file"
      : "Step 2/2: Parsing + storing rows";
  const progressPercentLabel =
    uploadPhase === "processing"
      ? `${uploadProgress.toFixed(1)}%`
      : `${Math.round(uploadProgress)}%`;
  const estimatedProcessedRows =
    estimatedRows === null
      ? null
      : Math.min(
          estimatedRows,
          Math.max(0, Math.round((Math.max(0, Math.min(100, uploadProgress)) / 100) * estimatedRows)),
        );
  const activeTableName =
    activeTableId !== null
      ? tables.find((table) => table.dataset_id === activeTableId)?.name || "Table"
      : null;

  return (
    <div className="page page-stack">
      {toast && (
        <div className={`toast ${toast.kind === "delete" ? "delete slow" : "success"}`}>
          <span>{toast.message}</span>
          {toast.kind === "delete" && (
            <button type="button" className="toast-action" onClick={onUndoDelete}>
              Undo
            </button>
          )}
        </div>
      )}

      <div className="hero">
        <div className="hero-title-row">
          <img src={logo} alt="TabulaRAG" className="hero-logo" />
          <div className="hero-title">TabulaRAG</div>
        </div>
        <div className="hero-subtitle">
          A fast-ingesting tabular data RAG tool backed with cell citations.
        </div>
      </div>

      <div className="panel upload-panel">
        {!file ? (
          <label className="upload-drop">
            <input
              type="file"
              accept=".csv,.tsv"
              onChange={(event) => onSelectFile(event.target.files?.[0] || null)}
            />
            <div className="upload-icon" aria-hidden="true">
              <img src={uploadLogo} alt="" />
            </div>
            <div className="upload-title">Upload a CSV/TSV file</div>
            <div className="upload-subtitle">Click to select a file</div>
          </label>
        ) : (
          <>
            <h2>Upload CSV/TSV</h2>
            <div className="row">
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.tsv"
                onChange={(event) => onSelectFile(event.target.files?.[0] || null)}
                className="file-input-hidden"
              />
              <input
                value={name}
                onChange={(event) => setName(event.target.value)}
                style={{ minWidth: 240 }}
                disabled={busy}
              />
              <button
                onClick={() => fileInputRef.current?.click()}
                type="button"
                className="glass"
                disabled={busy}
              >
                Change file
              </button>
              <button onClick={onUpload} disabled={!file || busy} className="primary" type="button">
                {busy ? "Uploading..." : "Upload"}
              </button>
            </div>
            <div className="small">Selected: {file.name}</div>
            <div className="small">
              Tip: You can rename the table by clicking on the above field before clicking 'Upload'.
            </div>
          </>
        )}

        {err && <p className="error">{err}</p>}
        {reloadNotice && <p className="small status-info">{reloadNotice}</p>}
        {busy && (
          <div className="upload-progress" role="status" aria-live="polite">
            <div className="upload-progress-head">
              <span className="upload-progress-label">{progressLabel}</span>
              <span className="upload-progress-percent">{progressPercentLabel}</span>
            </div>
            <div className="upload-progress-track">
              <div
                className={`upload-progress-fill ${
                  uploadPhase === "processing" ? "processing" : ""
                }`}
                style={{ width: `${Math.max(0, Math.min(100, uploadProgress))}%` }}
              />
            </div>
            <div className="upload-progress-meta">
              {uploadPhase === "uploading"
                ? "Transferring file to backend..."
                : "Normalizing cells and writing rows..."}
            </div>
            <div className="upload-progress-rows">
              {estimatedRows === null || estimatedProcessedRows === null
                ? "Rows: estimating..."
                : `Rows: ${estimatedProcessedRows.toLocaleString()} / ${estimatedRows.toLocaleString()} (estimated)`}
            </div>
          </div>
        )}
        {status && !err && <p className="small status-info">{status}</p>}
      </div>

      <div className="panel">
        <div className="row" style={{ justifyContent: "space-between" }}>
          <h3 style={{ marginBottom: 0 }}>Uploaded tables</h3>
          <span className="small">Tap a table to preview</span>
        </div>

        <div className="tables-scroll" ref={tablesScrollRef}>
          <ul>
            {tables.map((table) => {
              const indexStatus = indexStatusByTable[table.dataset_id];
              const indexState = indexStatus?.state || "ready";
              const indexPercent = Math.max(
                0,
                Math.min(100, Math.round(indexStatus?.progress ?? 100)),
              );
              const indexRowsDone = indexStatus?.processed_rows ?? table.row_count;
              const indexRowsTotal = indexStatus?.total_rows || table.row_count;
              const isIndexing =
                indexState === "queued" || indexState === "indexing";
              const indexLabel =
                indexState === "queued"
                  ? "Queued"
                  : indexState === "indexing"
                    ? "Indexing"
                    : indexState === "error"
                      ? "Index failed"
                      : "Indexed";

              return (
                <li key={table.dataset_id}>
                  <div className="list-row">
                    <div className="list-item">
                      {editingId === table.dataset_id ? (
                        <input
                          value={editingName}
                          onChange={(event) => setEditingName(event.target.value)}
                          onKeyDown={(event) => {
                            if (event.key === "Enter") {
                              void onRename(table.dataset_id);
                            }
                          }}
                          className="rename-input"
                          disabled={busy}
                        />
                      ) : (
                        <button
                          type="button"
                          className="list-button"
                          onClick={() => {
                            void loadPreview(table.dataset_id);
                          }}
                        >
                          <span className="mono">{table.name}</span>{" "}
                          <span className="small">
                            ({table.row_count} rows, {table.column_count} cols)
                          </span>
                        </button>
                      )}
                    </div>

                    <div
                      className={`index-job ${indexState}`}
                      title={indexStatus?.message || indexLabel}
                    >
                      {isIndexing ? (
                        <>
                          <div className="index-job-head">
                            <span className="index-job-label">{indexLabel}</span>
                            <span className="index-job-percent mono">{indexPercent}%</span>
                          </div>
                          <div className="index-job-track">
                            <div
                              className={`index-job-fill ${indexState}`}
                              style={{ width: `${indexPercent}%` }}
                            />
                          </div>
                          <div className="index-job-meta">
                            {indexRowsDone.toLocaleString()}/{indexRowsTotal.toLocaleString()} rows
                          </div>
                        </>
                      ) : (
                        <div className="index-job-ready">{indexLabel}</div>
                      )}
                    </div>

                    <button
                      type="button"
                      className={`icon-button ${editingId === table.dataset_id ? "success" : "edit"}`}
                      onClick={() => {
                        if (editingId === table.dataset_id) {
                          void onRename(table.dataset_id);
                        } else {
                          setEditingId(table.dataset_id);
                          setEditingName(table.name);
                        }
                      }}
                      aria-label={editingId === table.dataset_id ? "Save name" : `Rename ${table.name}`}
                      title={editingId === table.dataset_id ? "Save" : "Rename"}
                      disabled={busy || Boolean(deletingTableIds[table.dataset_id])}
                    >
                      {editingId === table.dataset_id ? (
                        <svg viewBox="0 0 24 24" role="presentation">
                          <path d="M9.2 16.6 4.8 12.2a1 1 0 1 1 1.4-1.4l3 3 8-8a1 1 0 0 1 1.4 1.4l-8.8 8.8a1 1 0 0 1-1.4 0z" />
                        </svg>
                      ) : (
                        <svg viewBox="0 0 24 24" role="presentation">
                          <path d="M15.2 4.2a2 2 0 0 1 2.8 0l1.8 1.8a2 2 0 0 1 0 2.8l-9.8 9.8a1 1 0 0 1-.5.27l-4.5 1a1 1 0 0 1-1.2-1.2l1-4.5a1 1 0 0 1 .27-.5l9.8-9.8zM6.7 15.3l-.6 2.5 2.5-.6 8.6-8.6-1.9-1.9-8.6 8.6z" />
                        </svg>
                      )}
                    </button>

                    <button
                      type="button"
                      className="icon-button danger"
                      onClick={() => {
                        void onDelete(table.dataset_id);
                      }}
                      aria-label={`Delete ${table.name}`}
                      title="Delete table"
                      disabled={busy || Boolean(deletingTableIds[table.dataset_id])}
                    >
                      <svg viewBox="0 0 24 24" role="presentation">
                        <path d="M9 3a1 1 0 0 0-1 1v1H5a1 1 0 0 0 0 2h1v12a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V7h1a1 1 0 1 0 0-2h-3V4a1 1 0 0 0-1-1H9zm1 2h4v0H10zm-1 4a1 1 0 0 1 2 0v8a1 1 0 1 1-2 0V9zm6-1a1 1 0 0 1 1 1v8a1 1 0 1 1-2 0V9a1 1 0 0 1 1-1z" />
                      </svg>
                    </button>
                  </div>
                </li>
              );
            })}
          </ul>
        </div>

        {showScrollHint && <div className="scroll-indicator" aria-hidden="true">▼</div>}

      </div>

      <div className="panel upload-preview">
        <div className="preview-header">
          <h3 style={{ marginBottom: 0 }}>Table preview</h3>
          {activeTableName && (
            <div className="preview-table-name" aria-live="polite">
              <span className="preview-table-name-label">Now viewing</span>
              <span className="mono preview-table-name-value">{activeTableName}</span>
            </div>
          )}
        </div>

        {previewBusy && <p className="small">Loading preview...</p>}
        {previewErr && <p className="error">{previewErr}</p>}

        {preview && (
          <div className="table-area">
            <DataTable columns={preview.columns} rows={preview.rows} />
          </div>
        )}

        {!previewBusy && !preview && !previewErr && (
          <p className="small">Select a table above to preview the first 30 rows.</p>
        )}
      </div>
    </div>
  );
}
