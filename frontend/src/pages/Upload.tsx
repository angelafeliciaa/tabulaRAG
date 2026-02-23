import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
const PENDING_DELETE_SESSION_KEY = "tabularag_pending_delete";
const PINNED_TABLES_STORAGE_KEY = "tabularag_pinned_table_ids";
const DELETE_UNDO_WINDOW_MS = 5600;
const SUCCESS_TOAST_MS = 2800;
const MAX_UPLOAD_SIZE_MB = 100;
const MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024;

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

type TableSortMode = "alphabet" | "rows" | "recent";

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
  const [showPreviewScrollHint, setShowPreviewScrollHint] = useState(false);
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editingName, setEditingName] = useState("");
  const [renameHintId, setRenameHintId] = useState<number | null>(null);
  const [tableSearchQuery, setTableSearchQuery] = useState("");
  const [tableSortMode, setTableSortMode] = useState<TableSortMode>("recent");
  const [sortMenuOpen, setSortMenuOpen] = useState(false);
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
  const renameInputRef = useRef<HTMLInputElement | null>(null);
  const sortMenuRef = useRef<HTMLDivElement | null>(null);
  const estimateJobRef = useRef(0);
  const toastTimerRef = useRef<number | null>(null);
  const pendingDeleteRef = useRef<PendingDelete | null>(null);
  const pendingDeleteIdsRef = useRef<Set<number>>(new Set());

  useEffect(() => {
    window.localStorage.setItem(
      PINNED_TABLES_STORAGE_KEY,
      JSON.stringify(pinnedTableIds),
    );
  }, [pinnedTableIds]);

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
    const nextTables = (await listTables()).filter(
      (table) => !pendingDeleteIdsRef.current.has(table.dataset_id),
    );
    setTables(nextTables);
    try {
      await refreshIndexStatuses(nextTables);
    } catch {
      // Keep table list usable even if status polling fails.
    }
  }, [refreshIndexStatuses]);

  function setPendingDeleteSession(datasetId: number, tableName: string) {
    window.sessionStorage.setItem(
      PENDING_DELETE_SESSION_KEY,
      JSON.stringify({
        dataset_id: datasetId,
        table_name: tableName,
        created_at: new Date().toISOString(),
      }),
    );
  }

  function clearPendingDeleteSession(datasetId?: number) {
    if (datasetId === undefined) {
      window.sessionStorage.removeItem(PENDING_DELETE_SESSION_KEY);
      return;
    }

    const pendingRaw = window.sessionStorage.getItem(PENDING_DELETE_SESSION_KEY);
    if (!pendingRaw) {
      return;
    }

    try {
      const pending = JSON.parse(pendingRaw) as { dataset_id?: unknown };
      if (typeof pending.dataset_id === "number" && pending.dataset_id === datasetId) {
        window.sessionStorage.removeItem(PENDING_DELETE_SESSION_KEY);
      }
    } catch {
      window.sessionStorage.removeItem(PENDING_DELETE_SESSION_KEY);
    }
  }

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
    const pendingRaw = window.sessionStorage.getItem(PENDING_DELETE_SESSION_KEY);
    if (!pendingRaw) {
      return;
    }

    let datasetId: number | null = null;

    try {
      const pending = JSON.parse(pendingRaw) as { dataset_id?: unknown };
      if (typeof pending.dataset_id === "number" && Number.isFinite(pending.dataset_id)) {
        datasetId = pending.dataset_id;
      }
    } catch {
      window.sessionStorage.removeItem(PENDING_DELETE_SESSION_KEY);
      return;
    }

    if (datasetId === null) {
      window.sessionStorage.removeItem(PENDING_DELETE_SESSION_KEY);
      return;
    }

    pendingDeleteIdsRef.current.add(datasetId);

    void (async () => {
      try {
        await deleteTable(datasetId);
      } catch (error: unknown) {
        if (!isTableNotFoundError(error)) {
          setErr(getErrorMessage(error));
        }
      } finally {
        pendingDeleteIdsRef.current.delete(datasetId);
        clearPendingDeleteSession(datasetId);
        await refresh().catch(() => {
          // Keep UI responsive even if immediate refresh fails.
        });
      }
    })();
  }, [refresh]);

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
    if (!sortMenuOpen) {
      return;
    }

    const onPointerDown = (event: MouseEvent) => {
      const target = event.target as Node;
      if (sortMenuRef.current && !sortMenuRef.current.contains(target)) {
        setSortMenuOpen(false);
      }
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setSortMenuOpen(false);
      }
    };

    document.addEventListener("mousedown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [sortMenuOpen]);

  useEffect(() => {
    return () => {
      clearToastTimer();
      const pendingDelete = pendingDeleteRef.current;
      if (pendingDelete) {
        window.clearTimeout(pendingDelete.timeoutId);
        pendingDeleteRef.current = null;
        void deleteTable(pendingDelete.table.dataset_id, { keepalive: true })
          .catch(() => {
            // Best-effort cleanup on unmount/navigation.
          })
          .finally(() => {
            clearPendingDeleteSession(pendingDelete.table.dataset_id);
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
  }, [tables.length, tableSearchQuery]);

  useEffect(() => {
    const container = previewAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      setShowPreviewScrollHint(false);
      return;
    }

    const updateHint = () => {
      const atBottom = element.scrollTop + element.clientHeight >= element.scrollHeight - 4;
      const canScroll = element.scrollHeight > element.clientHeight + 2;
      setShowPreviewScrollHint(canScroll && !atBottom);
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
        setStatus(progress.phase === "uploading" ? "Uploading file..." : null);
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
    pendingDeleteIdsRef.current.delete(pending.table.dataset_id);
    clearPendingDeleteSession(pending.table.dataset_id);
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
    } catch (error: unknown) {
      if (isTableNotFoundError(error)) {
        return;
      }
      setErr(getErrorMessage(error));
      restoreDeletedTable(pending);
    } finally {
      setPinnedTableIds((previous) =>
        previous.filter((currentId) => currentId !== datasetId),
      );
      pendingDeleteIdsRef.current.delete(datasetId);
      clearPendingDeleteSession(datasetId);
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
    setErr(null);

    const pendingDelete: PendingDelete = {
      table,
      indexStatus: indexStatusByTable[datasetId] || defaultIndexStatusForTable(table),
      previousActiveTableId: activeTableId,
      previousPreview: preview,
      timeoutId: 0,
    };

    pendingDeleteIdsRef.current.add(datasetId);
    setPendingDeleteSession(datasetId, table.name);
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
    clearPendingDeleteSession(pendingDelete.table.dataset_id);
    restoreDeletedTable(pendingDelete);
    showSuccessToast(`Restored table '${pendingDelete.table.name}'`);
  }

  async function onRename(datasetId: number) {
    if (busy) {
      return;
    }

    const nextName = editingName.trim();
    if (!nextName) {
      setRenameHintId(datasetId);
      setEditingName("");
      setErr(null);
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
    if (nextFile.size > MAX_UPLOAD_SIZE_BYTES) {
      return `File is too large. Maximum size is ${MAX_UPLOAD_SIZE_MB} MB.`;
    }
    return null;
  }

  function onSelectFile(nextFile: File | null) {
    if (busy) {
      return;
    }
    setStatus(null);

    if (!nextFile) {
      setErr(null);
      setFile(null);
      setEstimatedRows(null);
      return;
    }

    const validationError = validateSelectedFile(nextFile);
    if (validationError) {
      setErr(validationError);
      return;
    }

    setErr(null);
    setFile(nextFile);
    const withoutExt = nextFile.name.replace(/\.[^.]+$/, "");
    setName(withoutExt || nextFile.name);
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
    const droppedFile = event.dataTransfer.files?.[0] || null;
    onSelectFile(droppedFile);
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
  const pinnedTableIdSet = useMemo(() => new Set(pinnedTableIds), [pinnedTableIds]);
  const sortedTables = useMemo(() => {
    const sortByMode = (a: TableSummary, b: TableSummary): number => {
      if (tableSortMode === "alphabet") {
        const byName = a.name.localeCompare(b.name, undefined, { sensitivity: "base" });
        if (byName !== 0) {
          return byName;
        }
      } else if (tableSortMode === "rows") {
        const byRows = b.row_count - a.row_count;
        if (byRows !== 0) {
          return byRows;
        }
      }

      // Default/fallback ordering is most recent first.
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
      return sortByMode(a, b);
    });
    return next;
  }, [tables, pinnedTableIdSet, tableSortMode]);
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
    element.scrollTo({ top: element.scrollHeight, behavior: "smooth" });
  }

  function scrollPreviewToBottom() {
    const container = previewAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      return;
    }
    element.scrollTo({ top: element.scrollHeight, behavior: "smooth" });
  }

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
          <label
            className={`upload-drop ${isDragActive ? "drag-active" : ""}`}
            onDragEnter={onUploadDragEnter}
            onDragOver={onUploadDragOver}
            onDragLeave={onUploadDragLeave}
            onDrop={onUploadDrop}
          >
            <input
              type="file"
              accept=".csv,.tsv"
              onChange={(event) => onSelectFile(event.target.files?.[0] || null)}
            />
            <div className="upload-icon" aria-hidden="true">
              <img src={uploadLogo} alt="" />
            </div>
            <div className="upload-title">Choose a file or drag and drop it here</div>
            <div className="upload-subtitle">Accepts .csv &amp; .tsv formats, up to 100 MB</div>
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
                className={`upload-progress-fill ${uploadPhase === "processing" ? "processing" : ""
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
                : `Rows: ${estimatedProcessedRows.toLocaleString()} / ${estimatedRows.toLocaleString()}`}
            </div>
          </div>
        )}
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
            <div className="sort-menu-wrap" ref={sortMenuRef}>
              <button
                type="button"
                className={`sort-toggle-button ${sortMenuOpen ? "active" : ""}`}
                onClick={() => setSortMenuOpen((current) => !current)}
                aria-haspopup="dialog"
                aria-expanded={sortMenuOpen}
                aria-label="Sort tables"
                title="Sort tables"
              >
                <svg viewBox="0 0 24 24" role="presentation" className="sort-toggle-icon">
                  <path d="M8.7 3.3a1 1 0 0 1 1.4 0l3 3a1 1 0 1 1-1.4 1.4L10.4 6.4V20a1 1 0 1 1-2 0V6.4L7.1 7.7a1 1 0 1 1-1.4-1.4l3-3zm6.6 17.4a1 1 0 0 1-1.4 0l-3-3a1 1 0 0 1 1.4-1.4l1.3 1.3V4a1 1 0 1 1 2 0v13.6l1.3-1.3a1 1 0 1 1 1.4 1.4l-3 3z" />
                </svg>
                <span className="sort-toggle-text">Sort</span>
              </button>
              {sortMenuOpen && (
                <div className="sort-menu" role="dialog" aria-label="Sort options">
                  <button
                    type="button"
                    className={`sort-menu-item ${tableSortMode === "recent" ? "active" : ""}`}
                    onClick={() => {
                      setTableSortMode("recent");
                      setSortMenuOpen(false);
                    }}
                  >
                    Most recent
                  </button>
                  <button
                    type="button"
                    className={`sort-menu-item ${tableSortMode === "rows" ? "active" : ""}`}
                    onClick={() => {
                      setTableSortMode("rows");
                      setSortMenuOpen(false);
                    }}
                  >
                    Most rows
                  </button>
                  <button
                    type="button"
                    className={`sort-menu-item ${tableSortMode === "alphabet" ? "active" : ""}`}
                    onClick={() => {
                      setTableSortMode("alphabet");
                      setSortMenuOpen(false);
                    }}
                  >
                    Alphabet
                  </button>
                </div>
              )}
            </div>
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
                      className={`list-item ${
                        activeTableId === table.dataset_id ? "selected" : ""
                      }`}
                    >
                      {editingId === table.dataset_id ? (
                        <input
                          ref={renameInputRef}
                          value={editingName}
                          onChange={(event) => {
                            setEditingName(event.target.value);
                            if (renameHintId === table.dataset_id) {
                              setRenameHintId(null);
                            }
                          }}
                          onKeyDown={(event) => {
                            if (event.key === "Enter") {
                              void onRename(table.dataset_id);
                            }
                          }}
                          className={`rename-input ${
                            renameHintId === table.dataset_id ? "invalid" : ""
                          }`}
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

        {showScrollHint && (
          <button
            type="button"
            className="scroll-indicator uploaded-scroll-indicator"
            onClick={scrollUploadedTablesToBottom}
            aria-label="Scroll uploaded tables to bottom"
            title="Scroll to bottom"
          >
            ▼
          </button>
        )}

      </div>

      <div className="panel upload-preview">
        <div className="preview-header">
          <h3 style={{ marginBottom: 0 }}>Table preview</h3>
          {activeTableName && (
            <div className="preview-table-name" aria-live="polite">
              <span className="preview-table-name-label">{activeTableName}</span>
            </div>
          )}
        </div>

        {previewBusy && <p className="small">Loading preview...</p>}
        {previewErr && <p className="error">{previewErr}</p>}

        {preview && (
          <div className="table-area" ref={previewAreaRef}>
            <DataTable columns={preview.columns} rows={preview.rows} />
          </div>
        )}

        {showPreviewScrollHint && (
          <button
            type="button"
            className="scroll-indicator preview-scroll-indicator"
            onClick={scrollPreviewToBottom}
            aria-label="Scroll table preview to bottom"
            title="Scroll to bottom"
          >
            ▼
          </button>
        )}

        {!previewBusy && !preview && !previewErr && (
          <p className="small">Select a table above to preview the first 30 rows.</p>
        )}
      </div>
    </div>
  );
}
