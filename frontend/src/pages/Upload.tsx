import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
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
import openIcon from "../images/open.png";
import plusIcon from "../images/plus.png";
import uploadLogo from "../images/upload.png";

const PENDING_UPLOAD_SESSION_KEY = "tabularag_pending_upload";
const PINNED_TABLES_STORAGE_KEY = "tabularag_pinned_table_ids";
const SUCCESS_TOAST_MS = 2800;
const INDEX_PROGRESS_DRIFT_STEP = 0.35;
const INDEX_PROGRESS_DRIFT_CAP = 99.4;
const SAFE_TABLE_NAME_MAX_LENGTH = 64;

type ToastState = { id: number; kind: "success"; message: string };
type UploadQueuePhase = "idle" | UploadProgress["phase"] | "success" | "error";
type UploadQueueItem = {
  id: string;
  file: File;
  name: string;
  progress: number;
  phase: UploadQueuePhase;
  estimatedRows: number | null;
  estimatedCols: number | null;
  error: string | null;
};

function getErrorMessage(error: unknown): string {
  const normalize = (message: string): string => {
    const trimmed = message.trim();
    if (!(trimmed.startsWith("{") && trimmed.endsWith("}"))) {
      return message;
    }
    try {
      const parsed = JSON.parse(trimmed) as { detail?: unknown };
      if (typeof parsed.detail === "string" && parsed.detail.trim()) {
        return parsed.detail;
      }
    } catch {
      // Keep original message when response is not valid JSON.
    }
    return message;
  };

  if (error instanceof Error) {
    return normalize(error.message);
  }
  return normalize(String(error));
}

function isTableNotFoundError(error: unknown): boolean {
  return /table not found/i.test(getErrorMessage(error));
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function formatFileSize(bytes: number): string {
  const safeBytes = Math.max(0, bytes);
  if (safeBytes < 1024) {
    return `${safeBytes}B`;
  }
  if (safeBytes < 1024 * 1024) {
    return `${Math.round(safeBytes / 1024)}KB`;
  }
  return `${(safeBytes / (1024 * 1024)).toFixed(1)}MB`;
}

function countDelimitedColumns(line: string, delimiter: string): number {
  if (!line.length) {
    return 0;
  }

  let inQuotes = false;
  let count = 1;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      if (inQuotes && line[i + 1] === "\"") {
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (!inQuotes && ch === delimiter) {
      count += 1;
    }
  }
  return count;
}

function stripSupportedFileExtension(name: string): string {
  return name.trim().replace(/\.(csv|tsv)$/i, "").trim();
}

function sanitizeTableNameInput(name: string): string {
  const withoutExtension = stripSupportedFileExtension(name);
  const withoutControlChars = withoutExtension.replace(/[\u0000-\u001f\u007f]/g, "");
  const allowedCharsOnly = withoutControlChars.replace(/[^A-Za-z0-9 _-]/g, "");
  const normalizedSpaces = allowedCharsOnly.replace(/\s+/g, " ").trim();
  return normalizedSpaces.slice(0, SAFE_TABLE_NAME_MAX_LENGTH);
}

function getNameKey(name: string): string {
  return sanitizeTableNameInput(name).toLocaleLowerCase();
}

function claimUniqueTableName(baseName: string, occupiedNameKeys: Set<string>): string {
  const cleanedBaseName = sanitizeTableNameInput(baseName) || "table";
  let candidate = cleanedBaseName;
  let suffix = 2;

  while (occupiedNameKeys.has(getNameKey(candidate))) {
    candidate = `${cleanedBaseName}_${suffix}`;
    suffix += 1;
  }

  occupiedNameKeys.add(getNameKey(candidate));
  return candidate;
}

function smoothIndexStatus(
  previous: TableIndexStatus | undefined,
  incoming: TableIndexStatus,
  fallbackTotalRows: number,
): TableIndexStatus {
  const totalRows =
    incoming.total_rows > 0 ? incoming.total_rows : Math.max(0, fallbackTotalRows);

  if (incoming.state !== "indexing" || !previous || previous.state !== "indexing") {
    return { ...incoming, total_rows: totalRows };
  }

  const previousProgress = clamp(previous.progress || 0, 0, INDEX_PROGRESS_DRIFT_CAP);
  const serverProgress = clamp(incoming.progress || 0, 0, INDEX_PROGRESS_DRIFT_CAP);

  if (serverProgress > previousProgress + 0.05) {
    return { ...incoming, total_rows: totalRows };
  }

  const nextProgress = Math.max(
    serverProgress,
    clamp(previousProgress + INDEX_PROGRESS_DRIFT_STEP, 0, INDEX_PROGRESS_DRIFT_CAP),
  );

  let nextProcessedRows = Math.max(0, incoming.processed_rows || 0);
  if (totalRows > 0) {
    const inferredRows = Math.min(
      totalRows - 1,
      Math.floor((nextProgress / 100) * totalRows),
    );
    nextProcessedRows = Math.max(nextProcessedRows, inferredRows);
  }

  return {
    ...incoming,
    total_rows: totalRows,
    progress: nextProgress,
    processed_rows: nextProcessedRows,
  };
}

export default function Upload() {
  const [uploadQueue, setUploadQueue] = useState<UploadQueueItem[]>([]);
  const [tables, setTables] = useState<TableSummary[]>([]);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [status, setStatus] = useState<string | null>(null);
  const [preview, setPreview] = useState<TableSlice | null>(null);
  const [previewErr, setPreviewErr] = useState<string | null>(null);
  const [previewBusy, setPreviewBusy] = useState(false);
  const [activeTableId, setActiveTableId] = useState<number | null>(null);
  const [toast, setToast] = useState<ToastState | null>(null);
  const [deleteConfirmTable, setDeleteConfirmTable] = useState<TableSummary | null>(null);
  const [showScrollHint, setShowScrollHint] = useState(false);
  const [uploadedAtBottom, setUploadedAtBottom] = useState(false);
  const [showPreviewScrollHint, setShowPreviewScrollHint] = useState(false);
  const [previewAtBottom, setPreviewAtBottom] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingName, setEditingName] = useState("");
  const [renameHintId, setRenameHintId] = useState<number | null>(null);
  const [tableSearchQuery, setTableSearchQuery] = useState("");
  const [reloadNotice, setReloadNotice] = useState<string | null>(null);
  const [deletingTableIds, setDeletingTableIds] = useState<Record<number, boolean>>({});
  const [isDragActive, setIsDragActive] = useState(false);
  const [pinnedTableIds, setPinnedTableIds] = useState<number[]>(() => {
    try {
      const raw = window.localStorage.getItem(PINNED_TABLES_STORAGE_KEY);
      if (!raw) {
        return [];
      }
      const parsed = JSON.parse(raw) as unknown;
      if (!Array.isArray(parsed)) {
        return [];
      }
      return parsed
        .filter((value): value is number => typeof value === "number" && Number.isFinite(value))
        .map((value) => Math.trunc(value));
    } catch {
      return [];
    }
  });
  const [indexStatusByTable, setIndexStatusByTable] = useState<
    Record<number, TableIndexStatus>
  >({});
  const tablesScrollRef = useRef<HTMLDivElement | null>(null);
  const previewAreaRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const firstQueuedNameInputRef = useRef<HTMLInputElement | null>(null);
  const renameInputRef = useRef<HTMLInputElement | null>(null);
  const toastTimerRef = useRef<number | null>(null);

  useEffect(() => {
    window.localStorage.setItem(
      PINNED_TABLES_STORAGE_KEY,
      JSON.stringify(pinnedTableIds),
    );
  }, [pinnedTableIds]);

  async function estimateFileStats(nextFile: File): Promise<{
    rows: number | null;
    cols: number | null;
  }> {
    try {
      // Keep estimation lightweight: sample only the file head instead of scanning full file.
      const sampleBytes = Math.min(nextFile.size, 512 * 1024);
      if (sampleBytes <= 0) {
        return { rows: null, cols: null };
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
        return { rows: null, cols: null };
      }

      const estimatedTotalLines = Math.max(
        sampledLineCount,
        Math.round(nextFile.size / avgBytesPerLine),
      );
      const delimiter = nextFile.name.toLowerCase().endsWith(".tsv") ? "\t" : ",";
      const headerLine = lines.find((line) => line.trim().length > 0) || "";
      const estimatedCols = headerLine ? countDelimitedColumns(headerLine, delimiter) : null;
      // In this UI we always upload with has_header=true.
      return {
        rows: Math.max(0, estimatedTotalLines - 1),
        cols: estimatedCols,
      };
    } catch {
      return { rows: null, cols: null };
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
    const toastId = Date.now() + Math.random();
    setToast({ id: toastId, kind: "success", message });
    toastTimerRef.current = window.setTimeout(() => {
      setToast((current) =>
        current?.kind === "success" && current.id === toastId ? null : current,
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

    setIndexStatusByTable((previous) => {
      const merged: Record<number, TableIndexStatus> = {};
      for (const table of nextTables) {
        merged[table.dataset_id] = smoothIndexStatus(
          previous[table.dataset_id],
          nextStatusByTable[table.dataset_id],
          table.row_count,
        );
      }
      return merged;
    });
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
    if (editingId === null || busy) {
      return;
    }

    const rafId = window.requestAnimationFrame(() => {
      const input = renameInputRef.current;
      if (!input) {
        return;
      }
      input.focus();
      const cursorIndex = input.value.length;
      input.setSelectionRange(cursorIndex, cursorIndex);
    });

    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [editingId, busy]);

  useEffect(() => {
    if (busy || uploadQueue.length === 0) {
      return;
    }

    const rafId = window.requestAnimationFrame(() => {
      const input = firstQueuedNameInputRef.current;
      if (!input) {
        return;
      }
      input.focus();
      const cursorIndex = input.value.length;
      input.setSelectionRange(cursorIndex, cursorIndex);
    });

    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [uploadQueue.length, busy]);

  useEffect(() => {
    return () => {
      clearToastTimer();
    };
  }, []);

  useEffect(() => {
    if (!deleteConfirmTable) {
      return;
    }

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && !deletingTableIds[deleteConfirmTable.dataset_id]) {
        setDeleteConfirmTable(null);
      }
    };

    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [deleteConfirmTable, deletingTableIds]);

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
      setShowScrollHint(false);
      setUploadedAtBottom(false);
      return;
    }

    const updateHint = () => {
      const atBottom = element.scrollTop + element.clientHeight >= element.scrollHeight - 4;
      const canScroll = element.scrollHeight > element.clientHeight + 2;
      setShowScrollHint(canScroll);
      setUploadedAtBottom(atBottom);
    };

    updateHint();
    element.addEventListener("scroll", updateHint);
    window.addEventListener("resize", updateHint);

    return () => {
      element.removeEventListener("scroll", updateHint);
      window.removeEventListener("resize", updateHint);
    };
  }, [tables.length, tableSearchQuery]);

  useEffect(() => {
    const container = previewAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      setShowPreviewScrollHint(false);
      setPreviewAtBottom(false);
      return;
    }

    const updateHint = () => {
      const atBottom = element.scrollTop + element.clientHeight >= element.scrollHeight - 4;
      const canScroll = element.scrollHeight > element.clientHeight + 2;
      setShowPreviewScrollHint(canScroll);
      setPreviewAtBottom(atBottom);
    };

    const rafId = window.requestAnimationFrame(updateHint);
    element.addEventListener("scroll", updateHint);
    window.addEventListener("resize", updateHint);

    return () => {
      window.cancelAnimationFrame(rafId);
      element.removeEventListener("scroll", updateHint);
      window.removeEventListener("resize", updateHint);
    };
  }, [preview?.dataset_id, preview?.rows.length, previewBusy, previewErr]);

  const loadPreview = useCallback(async (datasetId: number) => {
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
  }, []);

  useEffect(() => {
    if (tables.length === 0) {
      setActiveTableId(null);
      setPreview(null);
      setPreviewErr(null);
      setPreviewBusy(false);
      return;
    }

    // Keep preview pinned to the most recent table (top row in Uploaded tables).
    void loadPreview(tables[0].dataset_id);
  }, [tables, loadPreview]);

  async function onUpload() {
    if (busy) {
      return;
    }

    const queuedItems = uploadQueue.filter((item) => item.phase === "idle" || item.phase === "error");
    if (queuedItems.length === 0) {
      return;
    }

    setReloadNotice(null);
    setBusy(true);
    setErr(null);

    let successCount = 0;
    let failureCount = 0;
    let firstFailureMessage: string | null = null;
    let lastUploadedDatasetId: number | null = null;
    const occupiedNameKeys = new Set(
      tables.map((table) => getNameKey(stripSupportedFileExtension(table.name) || "table")),
    );
    const preparedItems = queuedItems.map((item) => {
      const preferredName =
        sanitizeTableNameInput(item.name)
        || sanitizeTableNameInput(item.file.name)
        || "table";
      return {
        item,
        normalizedName: claimUniqueTableName(preferredName, occupiedNameKeys),
      };
    });
    let completedCount = 0;
    setStatus(`Uploading ${queuedItems.length} file${queuedItems.length === 1 ? "" : "s"}...`);
    window.sessionStorage.setItem(
      PENDING_UPLOAD_SESSION_KEY,
      JSON.stringify({
        file_name:
          queuedItems.length === 1 ? queuedItems[0].file.name : `${queuedItems.length} files`,
        started_at: new Date().toISOString(),
      }),
    );

    await Promise.allSettled(
      preparedItems.map(async ({ item, normalizedName }) => {
        setUploadQueue((previous) =>
          previous.map((current) =>
            current.id === item.id
              ? {
                ...current,
                name: normalizedName,
                phase: "uploading",
                progress: 2,
                error: null,
              }
              : current,
          ),
        );

        try {
          const result = await uploadTable(item.file, normalizedName, (progress) => {
            setUploadQueue((previous) =>
              previous.map((current) =>
                current.id === item.id
                  ? {
                    ...current,
                    phase: progress.phase,
                    progress: Math.max(current.progress, progress.percent),
                  }
                  : current,
              ),
            );
          });

          successCount += 1;
          lastUploadedDatasetId = result.dataset_id;
          setUploadQueue((previous) =>
            previous.map((current) =>
              current.id === item.id
                ? {
                  ...current,
                  phase: "success",
                  progress: 100,
                  error: null,
                }
                : current,
            ),
          );
        } catch (error: unknown) {
          failureCount += 1;
          const message = getErrorMessage(error);
          if (!firstFailureMessage) {
            firstFailureMessage = message;
          }
          setUploadQueue((previous) =>
            previous.map((current) =>
              current.id === item.id
                ? { ...current, phase: "error", progress: 0, error: message }
                : current,
            ),
          );
        } finally {
          completedCount += 1;
          setStatus(
            `Completed ${completedCount}/${queuedItems.length} file${queuedItems.length === 1 ? "" : "s"}...`,
          );
        }
      }),
    );

    window.sessionStorage.removeItem(PENDING_UPLOAD_SESSION_KEY);

    try {
      await refresh();
      if (lastUploadedDatasetId !== null) {
        await loadPreview(lastUploadedDatasetId);
      }
    } catch (error: unknown) {
      setErr(getErrorMessage(error));
    }

    if (successCount > 0) {
      showSuccessToast(
        successCount === 1
          ? "1 file uploaded successfully"
          : `${successCount} files uploaded successfully`,
      );
    }
    if (failureCount > 0 && firstFailureMessage) {
      setErr(firstFailureMessage);
    }
    if (failureCount === 0 && successCount === queuedItems.length) {
      setUploadQueue([]);
    }

    setStatus(null);
    setBusy(false);
  }

  async function onDelete(datasetId: number) {
    if (busy || deletingTableIds[datasetId]) {
      return;
    }
    const table = tables.find((current) => current.dataset_id === datasetId);
    if (!table) {
      setDeleteConfirmTable(null);
      return;
    }

    clearToastTimer();
    setErr(null);
    setDeletingTableIds((previous) => ({ ...previous, [datasetId]: true }));

    try {
      await deleteTable(datasetId);
      setTables((previous) => previous.filter((current) => current.dataset_id !== datasetId));
      setPinnedTableIds((previous) =>
        previous.filter((currentId) => currentId !== datasetId),
      );
      setIndexStatusByTable((previous) => {
        const next = { ...previous };
        delete next[datasetId];
        return next;
      });
      setDeleteConfirmTable(null);
      showSuccessToast(`Successfully deleted table '${table.name}'`);
    } catch (error: unknown) {
      if (isTableNotFoundError(error)) {
        setTables((previous) => previous.filter((current) => current.dataset_id !== datasetId));
        setPinnedTableIds((previous) =>
          previous.filter((currentId) => currentId !== datasetId),
        );
        setIndexStatusByTable((previous) => {
          const next = { ...previous };
          delete next[datasetId];
          return next;
        });
        setDeleteConfirmTable(null);
        showSuccessToast(`Successfully deleted table '${table.name}'`);
      } else {
        setErr(getErrorMessage(error));
      }
    } finally {
      setDeletingTableIds((previous) => {
        const next = { ...previous };
        delete next[datasetId];
        return next;
      });
    }
  }

  async function onRename(datasetId: number) {
    if (busy) {
      return;
    }

    const nextName = sanitizeTableNameInput(editingName);
    if (!nextName) {
      setRenameHintId(datasetId);
      setEditingName("");
      setErr(null);
      return;
    }

    const nextNameKey = getNameKey(nextName);
    const hasDuplicateName = tables.some(
      (table) =>
        table.dataset_id !== datasetId
        && getNameKey(stripSupportedFileExtension(table.name) || "table") === nextNameKey,
    );
    if (hasDuplicateName) {
      setErr("Name already exists, please choose a different name.");
      return;
    }

    try {
      await renameTable(datasetId, nextName);
      setEditingId(null);
      setEditingName("");
      setRenameHintId(null);
      await refresh();
    } catch (error: unknown) {
      setErr(getErrorMessage(error));
    }
  }

  function validateSelectedFile(nextFile: File): string | null {
    const lowerName = nextFile.name.toLowerCase();
    if (!(lowerName.endsWith(".csv") || lowerName.endsWith(".tsv"))) {
      return "File must have a .csv or .tsv extension.";
    }
    return null;
  }

  function onSelectFiles(nextFiles: FileList | null) {
    if (busy) {
      return;
    }
    setStatus(null);

    if (!nextFiles || nextFiles.length === 0) {
      return;
    }

    const existingFileKeys = new Set(
      uploadQueue.map((item) => `${item.file.name}:${item.file.size}:${item.file.lastModified}`),
    );
    const occupiedNameKeys = new Set<string>([
      ...tables.map((table) => getNameKey(stripSupportedFileExtension(table.name) || "table")),
      ...uploadQueue.map((item) => getNameKey(stripSupportedFileExtension(item.name) || "table")),
    ]);
    const nextItems: UploadQueueItem[] = [];
    const rejectedMessages: string[] = [];

    for (const nextFile of Array.from(nextFiles)) {
      const validationError = validateSelectedFile(nextFile);
      if (validationError) {
        rejectedMessages.push(`${nextFile.name}: ${validationError}`);
        continue;
      }

      const fileKey = `${nextFile.name}:${nextFile.size}:${nextFile.lastModified}`;
      if (existingFileKeys.has(fileKey)) {
        rejectedMessages.push(`${nextFile.name}: already selected.`);
        continue;
      }
      existingFileKeys.add(fileKey);

      const withoutExt = sanitizeTableNameInput(nextFile.name) || "table";
      const uniqueName = claimUniqueTableName(withoutExt, occupiedNameKeys);
      nextItems.push({
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 9)}`,
        file: nextFile,
        name: uniqueName,
        progress: 0,
        phase: "idle",
        estimatedRows: null,
        estimatedCols: null,
        error: null,
      });
    }

    if (nextItems.length === 0) {
      setErr(rejectedMessages[0] || "No valid files selected.");
      return;
    }

    setErr(
      rejectedMessages.length
        ? `${rejectedMessages[0]}${rejectedMessages.length > 1 ? ` (+${rejectedMessages.length - 1} more)` : ""}`
        : null,
    );
    setUploadQueue((previous) => [...previous, ...nextItems]);

    for (const item of nextItems) {
      estimateFileStats(item.file).then((stats) => {
        setUploadQueue((previous) =>
          previous.map((current) =>
            current.id === item.id
              ? { ...current, estimatedRows: stats.rows, estimatedCols: stats.cols }
              : current,
          ),
        );
      });
    }
  }

  function onRemoveQueuedFile(queueItemId: string) {
    if (busy) {
      return;
    }
    const nextQueue = uploadQueue.filter((item) => item.id !== queueItemId);
    setUploadQueue(nextQueue);
    if (nextQueue.length === 0) {
      setErr(null);
      setStatus(null);
    }
  }

  function onCancelAllQueuedFiles() {
    if (busy) {
      return;
    }
    setUploadQueue([]);
    setErr(null);
    setStatus(null);
  }

  function isQueuedNameDuplicate(queueItemId: string, candidateName: string): boolean {
    const normalizedCandidate = sanitizeTableNameInput(candidateName);
    if (!normalizedCandidate) {
      return false;
    }
    const candidateKey = getNameKey(normalizedCandidate);

    const existsInTables = tables.some(
      (table) => getNameKey(stripSupportedFileExtension(table.name) || "table") === candidateKey,
    );
    if (existsInTables) {
      return true;
    }

    return uploadQueue.some((item) => {
      if (item.id === queueItemId) {
        return false;
      }
      const normalizedItemName = sanitizeTableNameInput(item.name);
      if (!normalizedItemName) {
        return false;
      }
      return getNameKey(normalizedItemName) === candidateKey;
    });
  }

  function onChangeQueuedName(queueItemId: string, nextValue: string) {
    if (busy) {
      return;
    }

    const nextName = sanitizeTableNameInput(nextValue);
    setUploadQueue((previous) =>
      previous.map((item) =>
        item.id === queueItemId
          ? {
            ...item,
            name: nextName,
          }
          : item,
      ),
    );
  }

  function onUploadDragEnter(event: React.DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    event.stopPropagation();
    setIsDragActive(true);
  }

  function onUploadDragOver(event: React.DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    event.stopPropagation();
    event.dataTransfer.dropEffect = "copy";
    if (!isDragActive) {
      setIsDragActive(true);
    }
  }

  function onUploadDragLeave(event: React.DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    event.stopPropagation();
    const relatedTarget = event.relatedTarget as Node | null;
    if (!relatedTarget || !event.currentTarget.contains(relatedTarget)) {
      setIsDragActive(false);
    }
  }

  function onUploadDrop(event: React.DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    event.stopPropagation();
    setIsDragActive(false);
    onSelectFiles(event.dataTransfer.files);
  }
  const hasPendingUploads = uploadQueue.some(
    (item) => item.phase === "idle" || item.phase === "error",
  );
  const isQueueInProgress = useMemo(() => {
    if (busy) {
      return true;
    }
    return uploadQueue.some(
      (item) => item.phase === "uploading" || item.phase === "processing",
    );
  }, [busy, uploadQueue]);
  const activeTableName =
    activeTableId !== null
      ? tables.find((table) => table.dataset_id === activeTableId)?.name || "Table"
      : null;
  const deleteConfirmBusy = deleteConfirmTable
    ? Boolean(deletingTableIds[deleteConfirmTable.dataset_id])
    : false;
  const pinnedTableIdSet = useMemo(() => new Set(pinnedTableIds), [pinnedTableIds]);
  const sortedTables = useMemo(() => {
    const sortByRecency = (a: TableSummary, b: TableSummary): number => {
      const aTime = Number.isFinite(Date.parse(a.created_at))
        ? Date.parse(a.created_at)
        : a.dataset_id;
      const bTime = Number.isFinite(Date.parse(b.created_at))
        ? Date.parse(b.created_at)
        : b.dataset_id;
      return bTime - aTime;
    };

    const next = [...tables];
    next.sort((a, b) => {
      const aPinned = pinnedTableIdSet.has(a.dataset_id);
      const bPinned = pinnedTableIdSet.has(b.dataset_id);
      if (aPinned !== bPinned) {
        return aPinned ? -1 : 1;
      }
      return sortByRecency(a, b);
    });
    return next;
  }, [tables, pinnedTableIdSet]);
  const normalizedTableSearchQuery = tableSearchQuery.trim().toLowerCase();
  const filteredTables = useMemo(() => {
    if (!normalizedTableSearchQuery) {
      return sortedTables;
    }
    return sortedTables.filter((table) =>
      table.name.toLowerCase().includes(normalizedTableSearchQuery),
    );
  }, [sortedTables, normalizedTableSearchQuery]);
  function onTogglePin(datasetId: number) {
    setPinnedTableIds((previous) => {
      if (previous.includes(datasetId)) {
        return previous.filter((currentId) => currentId !== datasetId);
      }
      return [datasetId, ...previous];
    });
  }

  function scrollUploadedTablesToBottom() {
    const element = tablesScrollRef.current;
    if (!element) {
      return;
    }
    if (uploadedAtBottom) {
      element.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    element.scrollTo({ top: element.scrollHeight, behavior: "smooth" });
  }

  function scrollPreviewToBottom() {
    const container = previewAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      return;
    }
    if (previewAtBottom) {
      element.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    element.scrollTo({ top: element.scrollHeight, behavior: "smooth" });
  }

  return (
    <div className="page page-stack">
      {toast && (
        <div key={toast.id} className="toast success">
          <span>{toast.message}</span>
        </div>
      )}

      {deleteConfirmTable && (
        <div
          className="confirm-modal-overlay"
          onClick={() => {
            if (!deleteConfirmBusy) {
              setDeleteConfirmTable(null);
            }
          }}
        >
          <div
            className="confirm-modal"
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-confirm-title"
            aria-describedby="delete-confirm-description"
            onClick={(event) => {
              event.stopPropagation();
            }}
          >
            <h3 id="delete-confirm-title">Delete table permanently?</h3>
            <p id="delete-confirm-description" className="small">
              This will permanently delete{" "}
              <span className="confirm-modal-table-name">{deleteConfirmTable.name}</span>. This action cannot be undone.
            </p>
            <div className="confirm-modal-actions">
              <button
                type="button"
                className="glass"
                onClick={() => {
                  setDeleteConfirmTable(null);
                }}
                disabled={deleteConfirmBusy}
              >
                Cancel
              </button>
              <button
                type="button"
                className="confirm-delete-button"
                onClick={() => {
                  void onDelete(deleteConfirmTable.dataset_id);
                }}
                disabled={deleteConfirmBusy}
              >
                {deleteConfirmBusy ? "Deleting..." : "Permanently delete"}
              </button>
            </div>
          </div>
        </div>
      )}

      {uploadQueue.length > 0 && <div className="upload-queue-backdrop" aria-hidden="true" />}

      <div className="hero">
        <div className="hero-title-row">
          <img src={logo} alt="TabulaRAG" className="hero-logo" />
          <div className="hero-title">TabulaRAG</div>
        </div>
        <div className="hero-subtitle">
          A fast-ingesting tabular data RAG tool backed with cell citations.
        </div>
      </div>

      <div
        className={`panel upload-panel${uploadQueue.length > 0 ? " has-queue in-modal" : ""}`}
      >
        {uploadQueue.length === 0 ? (
          <label
            className={`upload-drop ${isDragActive ? "drag-active" : ""}`}
            onDragEnter={onUploadDragEnter}
            onDragOver={onUploadDragOver}
            onDragLeave={onUploadDragLeave}
            onDrop={onUploadDrop}
          >
            <input
              type="file"
              multiple
              accept=".csv,.tsv"
              onChange={(event) => {
                onSelectFiles(event.target.files);
                event.currentTarget.value = "";
              }}
            />
            <div className="upload-icon" aria-hidden="true">
              <img src={uploadLogo} alt="" />
            </div>
            <div className="upload-title">Select or Drag &amp; Drop Your File(s) to Start Uploading</div>
            <div className="upload-subtitle">Supported file formats: .csv, .tsv</div>
          </label>
        ) : (
          <>
            <h2>Upload CSV/TSV</h2>
            <div className="row upload-queue-toolbar">
              <input
                ref={fileInputRef}
                type="file"
                multiple
                accept=".csv,.tsv"
                onChange={(event) => {
                  onSelectFiles(event.target.files);
                  event.currentTarget.value = "";
                }}
                className="file-input-hidden"
              />
              <button
                onClick={() => fileInputRef.current?.click()}
                type="button"
                className="glass upload-add-more-button"
                disabled={busy || isQueueInProgress}
              >
                <img src={plusIcon} alt="" aria-hidden="true" className="upload-add-icon" />
                Add more files
              </button>
            </div>

            <ul className="upload-queue-list" aria-label="Selected files for upload">
              {uploadQueue.map((item, index) => {
                const progressValue = Math.max(0, Math.min(100, item.progress));
                const canEditQueuedName = item.phase === "idle" || item.phase === "error";
                const queueNameIsEmpty = sanitizeTableNameInput(item.name).length === 0;
                const queueNameIsDuplicate =
                  !queueNameIsEmpty && isQueuedNameDuplicate(item.id, item.name);
                const estimatedRowsText =
                  item.estimatedRows === null ? "..." : item.estimatedRows.toLocaleString();
                const estimatedColsText =
                  item.estimatedCols === null ? "..." : item.estimatedCols.toLocaleString();
                const processedRows =
                  item.estimatedRows === null
                    ? null
                    : item.phase === "success"
                      ? item.estimatedRows
                      : Math.max(
                        0,
                        Math.min(
                          item.estimatedRows,
                          Math.round((progressValue / 100) * item.estimatedRows),
                        ),
                      );
                const stateLabel =
                  item.phase === "success"
                    ? "Uploaded"
                    : item.phase === "error"
                      ? "Failed"
                      : item.phase === "processing"
                        ? "Processing"
                        : item.phase === "uploading"
                          ? "Uploading"
                          : "In Queue";
                const progressLabel =
                  item.phase === "idle"
                    ? null
                    : item.phase === "error"
                      ? "Failed"
                      : item.phase === "success"
                        ? "100%"
                      : item.phase === "processing"
                        ? `${progressValue.toFixed(1)}%`
                        : `${Math.round(progressValue)}%`;
                const rowsLabel =
                  item.estimatedRows === null
                    ? "Rows: estimating..."
                    : `Rows: ${(processedRows ?? 0).toLocaleString()} / ${item.estimatedRows.toLocaleString()}`;
                const progressFillWidth =
                  item.phase === "error" ? 100 : progressValue;
                const progressFillClassName = `upload-progress-fill ${
                  item.phase === "processing"
                    ? "processing"
                    : ""
                } ${
                  item.phase === "success"
                    ? "success"
                    : ""
                } ${item.phase === "error" ? "error" : ""}`;

                return (
                  <li key={item.id} className={`upload-queue-item ${item.phase}`}>
                    <div className="upload-queue-head compact">
                      <span className="upload-queue-file-icon" aria-hidden="true">
                        <svg viewBox="0 0 24 24" role="presentation">
                          <path d="M6 2h8l4 4v14a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2zm7 1.5V7h3.5L13 3.5zM7.5 11a1 1 0 0 0 0 2h9a1 1 0 1 0 0-2h-9zm0 4a1 1 0 0 0 0 2h9a1 1 0 1 0 0-2h-9z" />
                        </svg>
                      </span>
                      <div className="upload-queue-file-text">
                        <input
                          ref={index === 0 && canEditQueuedName ? firstQueuedNameInputRef : null}
                          type="text"
                          className={`upload-queue-name-input ${canEditQueuedName && (queueNameIsEmpty || queueNameIsDuplicate) ? "invalid" : ""}`}
                          value={item.name}
                          onChange={(event) => {
                            onChangeQueuedName(item.id, event.target.value);
                          }}
                          autoCapitalize="none"
                          autoCorrect="off"
                          spellCheck={false}
                          placeholder="Enter table name"
                          maxLength={SAFE_TABLE_NAME_MAX_LENGTH}
                          disabled={busy || !canEditQueuedName}
                        />
                        <div className="upload-queue-file-subtitle">
                          {item.file.name} - {formatFileSize(item.file.size)} (
                          {estimatedRowsText} rows, {estimatedColsText} cols){" "}
                          <span className={`upload-queue-state ${item.phase}`}>{stateLabel}</span>
                        </div>
                      </div>
                      <div className="upload-queue-right">
                        {progressLabel && (
                          <span className="upload-progress-percent upload-queue-percent">
                            {progressLabel}
                          </span>
                        )}
                      </div>
                      {!isQueueInProgress && (
                        <button
                          type="button"
                          className="upload-queue-remove"
                          onClick={() => onRemoveQueuedFile(item.id)}
                          aria-label={`Remove ${item.file.name}`}
                          title="Remove file"
                          disabled={busy}
                        >
                          ×
                        </button>
                      )}
                    </div>
                    {item.phase !== "idle" && (
                      <div className="upload-progress-track upload-queue-track compact">
                        <div
                          className={progressFillClassName}
                          style={{
                            width: `${progressFillWidth}%`,
                          }}
                        />
                      </div>
                    )}
                    {item.phase !== "idle" && (
                      <div className="upload-queue-rows">{rowsLabel}</div>
                    )}
                    {queueNameIsEmpty && canEditQueuedName && (
                      <p className="small error upload-queue-error">Table name cannot be empty.</p>
                    )}
                    {queueNameIsDuplicate && canEditQueuedName && (
                      <p className="small error upload-queue-error">
                        Name already exists, please choose a different name.
                      </p>
                    )}
                    {item.error && <p className="small error upload-queue-error">{item.error}</p>}
                  </li>
                );
              })}
            </ul>
            <div className="upload-queue-footer">
              <div className="small">
                Tip: You can rename each table before clicking 'Upload all files'.
              </div>
              {!isQueueInProgress && (
                <div className="upload-queue-footer-actions">
                  <button
                    type="button"
                    className="glass upload-cancel-all-button"
                    onClick={onCancelAllQueuedFiles}
                    disabled={busy}
                  >
                    Cancel all
                  </button>
                  <button
                    onClick={onUpload}
                    disabled={!hasPendingUploads || busy}
                    className="primary"
                    type="button"
                  >
                    {busy ? "Uploading..." : "Upload all files"}
                  </button>
                </div>
              )}
            </div>
          </>
        )}

        {err && <p className="error">{err}</p>}
        {reloadNotice && <p className="small status-info">{reloadNotice}</p>}
        {status && !err && <p className="small status-info">{status}</p>}
      </div>

      <div className="panel">
        <div className="row tables-header-row">
          <h3 style={{ marginBottom: 0 }}>Uploaded tables</h3>
          <div className="tables-header-controls">
            <label className="tables-search-input-wrap" aria-label="Search table name">
              <svg viewBox="0 0 24 24" role="presentation" className="tables-search-icon">
                <path d="M10.5 3a7.5 7.5 0 0 1 5.96 12.06l4.24 4.24a1 1 0 0 1-1.42 1.42l-4.24-4.24A7.5 7.5 0 1 1 10.5 3zm0 2a5.5 5.5 0 1 0 0 11 5.5 5.5 0 0 0 0-11z" />
              </svg>
              <input
                type="text"
                className="tables-search-input"
                value={tableSearchQuery}
                onChange={(event) => setTableSearchQuery(event.target.value)}
                placeholder="Search"
                aria-label="Search table name"
              />
            </label>
          </div>
        </div>

        <div className="tables-scroll" ref={tablesScrollRef}>
          <ul>
            {filteredTables.map((table) => {
              const indexStatus = indexStatusByTable[table.dataset_id];
              const indexState = indexStatus?.state || "ready";
              const isPinned = pinnedTableIdSet.has(table.dataset_id);
              const indexProgress = Math.max(
                0,
                Math.min(
                  100,
                  Math.round(
                    typeof indexStatus?.progress === "number"
                      ? indexStatus.progress
                      : indexState === "ready"
                        ? 100
                        : 0,
                  ),
                ),
              );
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
                    <button
                      type="button"
                      className={`icon-button ${isPinned ? "pinned" : "pin"}`}
                      onClick={() => onTogglePin(table.dataset_id)}
                      aria-label={isPinned ? `Unpin ${table.name}` : `Pin ${table.name}`}
                      title={isPinned ? "Unpin table" : "Pin table"}
                      disabled={busy || Boolean(deletingTableIds[table.dataset_id])}
                    >
                      <svg viewBox="0 0 24 24" role="presentation">
                        <path d="M9 3h6l-1 5 3 3v1h-4v7l-1 1-1-1v-7H7v-1l3-3-1-5z" />
                      </svg>
                    </button>

                    <div
                      className={`list-item ${activeTableId === table.dataset_id ? "selected" : ""
                        }`}
                    >
                      {editingId === table.dataset_id ? (
                        <input
                          ref={renameInputRef}
                          value={editingName}
                          onChange={(event) => {
                            setEditingName(sanitizeTableNameInput(event.target.value));
                            if (renameHintId === table.dataset_id) {
                              setRenameHintId(null);
                            }
                          }}
                          onKeyDown={(event) => {
                            if (event.key === "Enter") {
                              void onRename(table.dataset_id);
                            }
                          }}
                          className={`rename-input ${renameHintId === table.dataset_id ? "invalid" : ""
                            }`}
                          autoCapitalize="none"
                          autoCorrect="off"
                          spellCheck={false}
                          maxLength={SAFE_TABLE_NAME_MAX_LENGTH}
                          placeholder={
                            renameHintId === table.dataset_id
                              ? "Name cannot be empty."
                              : "Enter table name"
                          }
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
                          <span className="uploaded-table-name">{table.name}</span>{" "}
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
                      <div className="index-job-ready">{indexLabel}</div>
                      {indexState === "indexing" && (
                        <div className="index-job-track" aria-hidden="true">
                          <div
                            className="index-job-fill indexing"
                            style={{ width: `${Math.max(4, indexProgress)}%` }}
                          />
                        </div>
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
                          setRenameHintId(null);
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
                        setDeleteConfirmTable(table);
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

        {showScrollHint && (
          <button
            type="button"
            className="scroll-indicator uploaded-scroll-indicator"
            onClick={scrollUploadedTablesToBottom}
            aria-label={uploadedAtBottom ? "Scroll uploaded tables to top" : "Scroll uploaded tables to bottom"}
            title={uploadedAtBottom ? "Scroll to top" : "Scroll to bottom"}
          >
            {uploadedAtBottom ? "▲" : "▼"}
          </button>
        )}

      </div>

      <div className="panel upload-preview">
        <div className="preview-header">
          <div className="preview-header-left">
            <h3 style={{ marginBottom: 0 }}>Table preview</h3>
            {activeTableName && (
              <div className="preview-table-name" aria-live="polite">
                <span className="preview-table-name-value">{activeTableName}</span>
              </div>
            )}
          </div>
          {activeTableId !== null && (
            <Link
              className="preview-open-icon-link"
              to={`/tables/${activeTableId}`}
              aria-label="Open full table"
              title="Open Full Table"
            >
              <img src={openIcon} alt="" aria-hidden="true" />
            </Link>
          )}
        </div>

        {previewBusy && <p className="small">Loading preview...</p>}
        {previewErr && <p className="error">{previewErr}</p>}

        {preview && (
          <div className="table-area" ref={previewAreaRef}>
            <DataTable columns={preview.columns} rows={preview.rows} sortable={false} />
          </div>
        )}

        {showPreviewScrollHint && (
          <button
            type="button"
            className="scroll-indicator preview-scroll-indicator"
            onClick={scrollPreviewToBottom}
            aria-label={previewAtBottom ? "Scroll table preview to top" : "Scroll table preview to bottom"}
            title={previewAtBottom ? "Scroll to top" : "Scroll to bottom"}
          >
            {previewAtBottom ? "▲" : "▼"}
          </button>
        )}

        {!previewBusy && !preview && !previewErr && (
          <p className="small">Select a table above to preview the first 30 rows.</p>
        )}
      </div>
    </div>
  );
}
