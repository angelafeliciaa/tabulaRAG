import { useEffect, useId, useMemo, useRef, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { focusByIndex, focusByOffset } from "../accessibility";
import {
  filterRowIndices,
  getSlice,
  listTables,
  type FilterRowIndicesResponse,
  type TableSlice,
  type TableSummary,
} from "../api";
import DataTable from "../components/DataTable";

type DateViewMode = "default" | "mm-dd-yyyy" | "mon-dd-yyyy";
type ValueMode = "normalized" | "original";
type DateMenuState = { x: number; y: number } | null;
type FilterConditionPayload = {
  column: string;
  operator: string;
  value?: string;
  logical_operator?: "AND" | "OR";
};
type QueryPayload =
  | {
    mode: "filter";
    dataset_id: number;
    filters?: FilterConditionPayload[];
    limit?: number;
    offset?: number;
  }
  | {
    dataset_id: number;
    operation: string;
    metric_column?: string;
    group_by?: string;
    filters?: FilterConditionPayload[];
    limit?: number;
  };
type MultiHighlightSpec = {
  dataset_id: number;
  filters?: FilterConditionPayload[];
  label?: string;
  max_rows?: number;
};
const ROWS_PER_PAGE = 500;
const DEFAULT_MULTI_MAX_ROWS = 1000;
const DATE_VIEW_OPTIONS: Array<{ value: DateViewMode; label: string }> = [
  { value: "default", label: "Default" },
  { value: "mm-dd-yyyy", label: "MM-DD-YYYY" },
  { value: "mon-dd-yyyy", label: "Jan 12, 2002" },
];

function resolveCellValue(
  value: unknown,
  mode: ValueMode,
): unknown {
  if (
    value != null
    && typeof value === "object"
    && !Array.isArray(value)
    && "normalized" in value
    && "original" in value
  ) {
    const o = value as { original?: unknown; normalized?: unknown };
    return mode === "original" ? o.original : o.normalized;
  }
  return value;
}

function flattenRowsByValueMode(
  rows: Record<string, unknown>[],
  valueMode: ValueMode,
): Record<string, unknown>[] {
  return rows.map((row) => {
    const out: Record<string, unknown> = {};
    for (const [col, val] of Object.entries(row)) {
      out[col] = resolveCellValue(val, valueMode);
    }
    return out;
  });
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

function parseDateToDate(value: unknown): Date | null {
  if (value === null || value === undefined) {
    return null;
  }
  const text = String(value).trim();
  if (!text) {
    return null;
  }

  const isoMatch = text.match(
    /^(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
  );
  if (isoMatch) {
    const yyyy = isoMatch[1];
    const mm = isoMatch[2].padStart(2, "0");
    const dd = isoMatch[3].padStart(2, "0");
    const parsed = new Date(`${yyyy}-${mm}-${dd}T00:00:00.000Z`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const dmyOrMdy = text.match(
    /^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
  );
  if (dmyOrMdy) {
    const a = Number(dmyOrMdy[1]);
    const b = Number(dmyOrMdy[2]);
    const rawYear = Number(dmyOrMdy[3]);
    const yyyy = String(rawYear < 100 ? 2000 + rawYear : rawYear);
    let day = a;
    let month = b;
    if (a <= 12 && b > 12) {
      month = a;
      day = b;
    } else if (a <= 12 && b <= 12) {
      day = a;
      month = b;
    }
    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      const parsed = new Date(
        `${yyyy}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}T00:00:00.000Z`,
      );
      return Number.isNaN(parsed.getTime()) ? null : parsed;
    }
  }

  if (/[a-zA-Z]/.test(text)) {
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed;
    }
  }
  return null;
}

function detectDateColumns(rows: Record<string, unknown>[], columns: string[]): Set<string> {
  const sample = rows.slice(0, 300);
  const out = new Set<string>();
  for (const column of columns) {
    let nonEmpty = 0;
    let dateHits = 0;
    for (const row of sample) {
      const raw = row[column];
      if (raw === null || raw === undefined || String(raw).trim() === "") {
        continue;
      }
      nonEmpty += 1;
      if (parseDateToDate(raw)) {
        dateHits += 1;
      }
    }
    if (nonEmpty > 0 && dateHits / nonEmpty >= 0.6) {
      out.add(column);
    }
  }
  return out;
}

function parseNonNegativeInt(value: string | null): number | null {
  if (!value) {
    return null;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  const normalized = Math.trunc(parsed);
  return normalized >= 0 ? normalized : null;
}

function resolveReturnPath(search: string): string {
  const params = new URLSearchParams(search);
  const returnTo = (params.get("return_to") || "").trim();
  if (!returnTo) {
    return "/";
  }
  if (returnTo.startsWith("/")) {
    return returnTo;
  }
  try {
    const parsed = new URL(returnTo);
    if (parsed.origin === window.location.origin) {
      return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    }
  } catch {
    return "/";
  }
  return "/";
}

function decodePayload(encoded: string): QueryPayload {
  const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const pad = normalized.length % 4;
  const padded = pad ? normalized + "=".repeat(4 - pad) : normalized;
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}

function decodeMultiHighlightSpec(encoded: string): MultiHighlightSpec {
  const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const pad = normalized.length % 4;
  const padded = pad ? normalized + "=".repeat(4 - pad) : normalized;
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes));
}

function formatFilterSummary(filters?: FilterConditionPayload[]): string {
  if (!filters || filters.length === 0) {
    return "no filters";
  }
  return filters
    .map((f, index) => {
      const clause =
        f.operator === "IS NULL" || f.operator === "IS NOT NULL"
          ? `${f.column} ${f.operator}`
          : `${f.column} ${f.operator} ${f.value ?? ""}`.trim();
      if (index === 0) {
        return clause;
      }
      return `${(f.logical_operator || "AND").toUpperCase()} ${clause}`;
    })
    .join(" ");
}

function buildQueryContextTitle(returnPath: string): string | null {
  if (!returnPath || returnPath === "/") {
    return null;
  }
  try {
    const parsed = new URL(returnPath, window.location.origin);
    const encoded = parsed.searchParams.get("q");
    if (!encoded) {
      return null;
    }
    const payload = decodePayload(encoded);
    if ("mode" in payload && payload.mode === "filter") {
      return `Filter result: ${formatFilterSummary(payload.filters)}`;
    }

    const aggregatePayload = payload as Exclude<QueryPayload, { mode: "filter" }>;
    const operationLabel =
      aggregatePayload.operation.charAt(0).toUpperCase() + aggregatePayload.operation.slice(1);
    const metricCol = aggregatePayload.metric_column ?? "aggregate_value";
    if (aggregatePayload.group_by) {
      return `Aggregate result: ${operationLabel} ${metricCol} by ${aggregatePayload.group_by}`;
    }
    return `Aggregate result: ${operationLabel} of ${metricCol}`;
  } catch {
    return null;
  }
}

export default function TableView() {
  const { datasetId } = useParams();
  const location = useLocation();
  const numericDatasetId = Number(datasetId);
  const queryParams = useMemo(() => new URLSearchParams(location.search), [location.search]);
  const highlightedRow = parseNonNegativeInt(queryParams.get("highlight_row"));
  const highlightMode = queryParams.get("highlight_mode");
  const encodedHighlightSpec = queryParams.get("highlight_spec");
  const isMultiHighlightMode = highlightMode === "multi" && !!encodedHighlightSpec;
  const returnPath = resolveReturnPath(location.search);
  const sourceQueryTitle = useMemo(() => buildQueryContextTitle(returnPath), [returnPath]);
  const returnQueryMode = useMemo<"filter" | "aggregate" | null>(() => {
    if (!returnPath || returnPath === "/") {
      return null;
    }
    try {
      const parsed = new URL(returnPath, window.location.origin);
      const encoded = parsed.searchParams.get("q");
      if (!encoded) {
        return null;
      }
      const payload = decodePayload(encoded);
      if ("mode" in payload && payload.mode === "filter") {
        return "filter";
      }
      return "aggregate";
    } catch {
      return null;
    }
  }, [returnPath]);
  const parsedMultiSpec = useMemo(() => {
    if (!isMultiHighlightMode || !encodedHighlightSpec) {
      return null;
    }
    try {
      return decodeMultiHighlightSpec(encodedHighlightSpec);
    } catch {
      return null;
    }
  }, [encodedHighlightSpec, isMultiHighlightMode]);

  const [data, setData] = useState<TableSlice | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [highlightErr, setHighlightErr] = useState<string | null>(null);
  const [tableName, setTableName] = useState<string | null>(null);
  const [tableNotFound, setTableNotFound] = useState(false);
  const [tableRowCount, setTableRowCount] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [dateViewMode, setDateViewMode] = useState<DateViewMode>("default");
  const [dateMenu, setDateMenu] = useState<DateMenuState>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageInput, setPageInput] = useState("1");
  const [showScrollHint, setShowScrollHint] = useState(false);
  const [tableAtBottom, setTableAtBottom] = useState(false);
  const [valueMode, setValueMode] = useState<ValueMode>("normalized");
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("asc");
  const [multiHighlightRows, setMultiHighlightRows] = useState<number[]>([]);
  const [multiHighlightTotal, setMultiHighlightTotal] = useState(0);
  const [multiHighlightTruncated, setMultiHighlightTruncated] = useState(false);
  const [activeHighlightCursor, setActiveHighlightCursor] = useState(0);
  const [multiHighlightLabel, setMultiHighlightLabel] = useState("All matching rows");
  const dateMenuId = useId();
  const tableAreaRef = useRef<HTMLDivElement | null>(null);
  const dateMenuRef = useRef<HTMLDivElement | null>(null);
  const dateMenuItemRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const pageChangeSourceRef = useRef<"table" | "highlight">("table");

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    setErr(null);
    setData(null);
    setTableName(null);
    setTableNotFound(false);
    setTableRowCount(0);
    setDateMenu(null);

    let mounted = true;
    listTables({ includePending: true })
      .then((tables: TableSummary[]) => {
        if (!mounted) {
          return;
        }
        const table = tables.find((row) => row.dataset_id === numericDatasetId);
        if (!table) {
          setTableNotFound(true);
          return;
        }
        setTableName(table.name);
        if (typeof table.row_count === "number") {
          setTableRowCount(Math.max(0, table.row_count));
        }
      })
      .catch(() => {
        // Keep table view usable even if metadata lookup fails.
      });

    return () => {
      mounted = false;
    };
  }, [numericDatasetId]);

  useEffect(() => {
    if (tableNotFound) {
      document.title = "Error 404 | TabulaRAG";
    } else if (tableName) {
      document.title = `${tableName} | TabulaRAG`;
    }
  }, [tableName, tableNotFound]);

  useEffect(() => {
    if (!isMultiHighlightMode) {
      setHighlightErr(null);
      setMultiHighlightRows([]);
      setMultiHighlightTotal(0);
      setMultiHighlightTruncated(false);
      setActiveHighlightCursor(0);
      setMultiHighlightLabel("All matching rows");
      return;
    }

    if (!parsedMultiSpec) {
      setHighlightErr("This highlight link is invalid or expired.");
      setMultiHighlightRows([]);
      setMultiHighlightTotal(0);
      setMultiHighlightTruncated(false);
      setActiveHighlightCursor(0);
      setMultiHighlightLabel("All matching rows");
      return;
    }

    if (parsedMultiSpec.dataset_id !== numericDatasetId) {
      setHighlightErr("This highlight link does not match the selected dataset.");
      setMultiHighlightRows([]);
      setMultiHighlightTotal(0);
      setMultiHighlightTruncated(false);
      setActiveHighlightCursor(0);
      setMultiHighlightLabel(parsedMultiSpec.label || "All matching rows");
      return;
    }

    const maxRows = Math.max(1, Math.min(parsedMultiSpec.max_rows ?? DEFAULT_MULTI_MAX_ROWS, DEFAULT_MULTI_MAX_ROWS));
    let mounted = true;
    setHighlightErr(null);
    filterRowIndices({
      dataset_id: parsedMultiSpec.dataset_id,
      filters: parsedMultiSpec.filters,
      max_rows: maxRows,
    })
      .then((result: FilterRowIndicesResponse) => {
        if (!mounted) {
          return;
        }
        setMultiHighlightRows(result.row_indices);
        setMultiHighlightTotal(result.total_match_count);
        setMultiHighlightTruncated(result.truncated);
        setActiveHighlightCursor(0);
        setMultiHighlightLabel(parsedMultiSpec.label || "All matching rows");
        if (result.row_indices.length > 0) {
          const initialHighlightPage = Math.floor(result.row_indices[0] / ROWS_PER_PAGE) + 1;
          pageChangeSourceRef.current = "highlight";
          setCurrentPage(initialHighlightPage);
          setPageInput(String(initialHighlightPage));
        }
      })
      .catch((error: unknown) => {
        if (!mounted) {
          return;
        }
        setHighlightErr(getErrorMessage(error));
        setMultiHighlightRows([]);
        setMultiHighlightTotal(0);
        setMultiHighlightTruncated(false);
        setActiveHighlightCursor(0);
        setMultiHighlightLabel(parsedMultiSpec.label || "All matching rows");
      });

    return () => {
      mounted = false;
    };
  }, [isMultiHighlightMode, parsedMultiSpec, numericDatasetId]);

  useEffect(() => {
    const initialPage = highlightedRow !== null ? Math.floor(highlightedRow / ROWS_PER_PAGE) + 1 : 1;
    if (isMultiHighlightMode) {
      pageChangeSourceRef.current = "table";
      setCurrentPage(1);
      setPageInput("1");
      setSearchQuery("");
      return;
    }
    pageChangeSourceRef.current = "table";
    setCurrentPage(initialPage);
    setPageInput(String(initialPage));
    setSearchQuery("");
    setSortColumn(null);
    setSortDirection("asc");
  }, [numericDatasetId, highlightedRow, isMultiHighlightMode]);

  useEffect(() => {
    if (!isMultiHighlightMode || activeHighlightCursor < multiHighlightRows.length) {
      return;
    }
    setActiveHighlightCursor(Math.max(0, multiHighlightRows.length - 1));
  }, [activeHighlightCursor, isMultiHighlightMode, multiHighlightRows.length]);

  useEffect(() => {
    if (!isMultiHighlightMode || multiHighlightRows.length === 0) {
      return;
    }
    if (pageChangeSourceRef.current === "highlight") {
      pageChangeSourceRef.current = "table";
      return;
    }
    const normalizedPage = Math.max(1, currentPage);
    const pageStart = (normalizedPage - 1) * ROWS_PER_PAGE;
    const pageEndExclusive = pageStart + ROWS_PER_PAGE;
    const firstIndexOnPage = multiHighlightRows.findIndex(
      (rowIndex) => rowIndex >= pageStart && rowIndex < pageEndExclusive,
    );
    if (firstIndexOnPage !== -1 && firstIndexOnPage !== activeHighlightCursor) {
      setActiveHighlightCursor(firstIndexOnPage);
    }
  }, [activeHighlightCursor, currentPage, isMultiHighlightMode, multiHighlightRows]);

  const activeMultiHighlightedRow = useMemo(() => {
    if (!isMultiHighlightMode || multiHighlightRows.length === 0) {
      return null;
    }
    return multiHighlightRows[Math.max(0, Math.min(activeHighlightCursor, multiHighlightRows.length - 1))];
  }, [activeHighlightCursor, isMultiHighlightMode, multiHighlightRows]);

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    let mounted = true;
    setErr(null);
    setLoading(true);

    const pageOffset = (currentPage - 1) * ROWS_PER_PAGE;
    const pageEndExclusive = pageOffset + ROWS_PER_PAGE;
    const sort =
      sortColumn != null
        ? { sortColumn, sortDirection }
        : null;

    getSlice(numericDatasetId, pageOffset, pageEndExclusive, { flatten: false, sort })
      .then((slice) => {
        if (!mounted) {
          return;
        }
        setData(slice);
        setTableRowCount((previous) => Math.max(previous, Math.max(0, slice.row_count || 0)));

        const fetchedTotalPages = Math.max(1, Math.ceil(Math.max(0, slice.row_count || 0) / ROWS_PER_PAGE));
        if (currentPage > fetchedTotalPages) {
          setCurrentPage(fetchedTotalPages);
        }
      })
      .catch((error: unknown) => {
        if (!mounted) {
          return;
        }
        setErr(getErrorMessage(error));
        setData(null);
      })
      .finally(() => {
        if (mounted) {
          setLoading(false);
        }
      });

    return () => {
      mounted = false;
    };
  }, [numericDatasetId, currentPage, sortColumn, sortDirection]);

  const normalizedSearch = searchQuery.trim().toLowerCase();
  const effectiveRowCount = Math.max(tableRowCount, Math.max(0, data?.row_count || 0));
  const totalPages = Math.max(1, Math.ceil(effectiveRowCount / ROWS_PER_PAGE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const hasQueryContext = returnPath !== "/";
  const effectiveHighlightRow = isMultiHighlightMode ? activeMultiHighlightedRow : highlightedRow;
  const highlightedRows = isMultiHighlightMode
    ? multiHighlightRows
    : highlightedRow !== null
      ? [highlightedRow]
      : [];
  const pageInputWidthCh = Math.max(2, String(totalPages).length + 1);
  const resolvedRows = useMemo(
    () => (data ? flattenRowsByValueMode(data.rows, valueMode) : []),
    [data, valueMode],
  );
  const normalizedRows = useMemo(
    () => (data ? flattenRowsByValueMode(data.rows, "normalized") : []),
    [data],
  );
  const headerTitle = useMemo(() => {
    if (isMultiHighlightMode) {
      const label = (multiHighlightLabel || parsedMultiSpec?.label || "Result").trim();
      return `Aggregate Result: ${label}`;
    }
    if (highlightedRow !== null && returnQueryMode === "filter") {
      if (sourceQueryTitle) {
        return sourceQueryTitle;
      }
      return "Filter Result";
    }
    if (sourceQueryTitle) {
      return sourceQueryTitle;
    }
    return tableName || "Table";
  }, [highlightedRow, isMultiHighlightMode, multiHighlightLabel, parsedMultiSpec?.label, returnQueryMode, sourceQueryTitle, tableName]);
  const dateColumns = useMemo(
    () => (data ? detectDateColumns(resolvedRows, data.columns) : new Set<string>()),
    [data, resolvedRows],
  );
  const filtered = useMemo(() => {
    if (!data) {
      return { rows: [] as Record<string, unknown>[], rowIndices: [] as number[] };
    }
    const indices = data.row_indices ?? resolvedRows.map((_, index) => data.offset + index);
    if (!normalizedSearch) {
      return {
        rows: resolvedRows,
        rowIndices: indices,
      };
    }

    const nextRows: Record<string, unknown>[] = [];
    const nextRowIndices: number[] = [];
    for (let i = 0; i < resolvedRows.length; i += 1) {
      const row = resolvedRows[i];
      const matches = Object.values(row).some((value) =>
        String(value ?? "").toLowerCase().includes(normalizedSearch),
      );
      if (matches) {
        nextRows.push(row);
        nextRowIndices.push(indices[i]);
      }
    }
    return { rows: nextRows, rowIndices: nextRowIndices };
  }, [data, normalizedSearch, resolvedRows]);

  const displayRows = useMemo(() => {
    if (!data || dateViewMode === "default" || dateColumns.size === 0) {
      return filtered.rows;
    }

    const monthFormatter = new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "2-digit",
      year: "numeric",
      timeZone: "UTC",
    });

    return filtered.rows.map((row) => {
      const next = { ...row };
      for (const col of dateColumns) {
        const parsed = parseDateToDate(next[col]);
        if (!parsed) {
          continue;
        }
        if (dateViewMode === "mm-dd-yyyy") {
          const mm = String(parsed.getUTCMonth() + 1).padStart(2, "0");
          const dd = String(parsed.getUTCDate()).padStart(2, "0");
          const yyyy = parsed.getUTCFullYear();
          next[col] = `${mm}-${dd}-${yyyy}`;
        } else if (dateViewMode === "mon-dd-yyyy") {
          next[col] = monthFormatter.format(parsed);
        }
      }
      return next;
    });
  }, [data, dateColumns, dateViewMode, filtered.rows]);

  const sortRows = useMemo(() => {
    if (!data || normalizedRows.length === 0) {
      return undefined;
    }
    if (data.row_indices) {
      return normalizedRows;
    }
    return filtered.rowIndices.map((ri) => normalizedRows[ri - data.offset]);
  }, [data, filtered.rowIndices, normalizedRows]);

  useEffect(() => {
    if (!dateMenu) {
      return;
    }
    const close = () => setDateMenu(null);
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setDateMenu(null);
      }
    };
    window.addEventListener("click", close);
    window.addEventListener("scroll", close, true);
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("click", close);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [dateMenu]);

  useEffect(() => {
    if (!dateMenu) {
      return;
    }

    const activeIndex = DATE_VIEW_OPTIONS.findIndex(
      (option) => option.value === dateViewMode,
    );
    const rafId = window.requestAnimationFrame(() => {
      focusByIndex(dateMenuItemRefs.current, activeIndex === -1 ? 0 : activeIndex);
    });

    return () => {
      window.cancelAnimationFrame(rafId);
    };
  }, [dateMenu, dateViewMode]);

  useEffect(() => {
    const container = tableAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      setShowScrollHint(false);
      setTableAtBottom(false);
      return;
    }

    const updateHint = () => {
      const atBottom = element.scrollTop + element.clientHeight >= element.scrollHeight - 4;
      const canScroll = element.scrollHeight > element.clientHeight + 2;
      setShowScrollHint(canScroll);
      setTableAtBottom(atBottom);
    };

    const rafId = window.requestAnimationFrame(updateHint);
    element.addEventListener("scroll", updateHint);
    window.addEventListener("resize", updateHint);

    return () => {
      window.cancelAnimationFrame(rafId);
      element.removeEventListener("scroll", updateHint);
      window.removeEventListener("resize", updateHint);
    };
  }, [displayRows.length, data?.columns.length, data?.offset, loading, err, dateViewMode]);

  useEffect(() => {
    const container = tableAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      return;
    }
    element.scrollTo({ top: 0, behavior: "auto" });
  }, [currentPage]);

  useEffect(() => {
    setPageInput(String(safeCurrentPage));
  }, [safeCurrentPage]);

  useEffect(() => {
    if (effectiveHighlightRow === null || !data) {
      return;
    }

    if (effectiveHighlightRow < data.offset || effectiveHighlightRow >= data.offset + data.rows.length) {
      return;
    }

    const targetElement = document.querySelector(
      `[data-row-index="${effectiveHighlightRow}"]`,
    ) as HTMLElement | null;

    const container = tableAreaRef.current?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!targetElement || !container) {
      return;
    }

    window.setTimeout(() => {
      const targetRect = targetElement.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();
      const scrollOffset =
        container.scrollTop +
        (targetRect.top - containerRect.top) +
        targetRect.height / 2 -
        container.clientHeight / 2;
      container.scrollTo({ top: Math.max(0, scrollOffset), behavior: "smooth" });
    }, 0);
  }, [data, effectiveHighlightRow, displayRows.length, dateViewMode]);

  function scrollTableToEdge() {
    const container = tableAreaRef.current;
    const element = container?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (!element) {
      return;
    }
    if (tableAtBottom) {
      element.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    element.scrollTo({ top: element.scrollHeight, behavior: "smooth" });
  }

  function commitPageInput() {
    const trimmed = pageInput.trim();
    if (!trimmed) {
      setPageInput(String(safeCurrentPage));
      return;
    }
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed)) {
      setPageInput(String(safeCurrentPage));
      return;
    }
    const normalized = Math.trunc(parsed);
    const nextPage = Math.min(totalPages, Math.max(1, normalized));
    pageChangeSourceRef.current = "table";
    setCurrentPage(nextPage);
    setPageInput(String(nextPage));
  }

  function jumpToHighlight() {
    if (effectiveHighlightRow === null) {
      return;
    }
    const highlightPage = Math.floor(effectiveHighlightRow / ROWS_PER_PAGE) + 1;
    if (safeCurrentPage !== highlightPage) {
      pageChangeSourceRef.current = "highlight";
      setCurrentPage(highlightPage);
      return;
    }
    const targetElement = document.querySelector(
      `[data-row-index="${effectiveHighlightRow}"]`,
    ) as HTMLElement | null;
    const container = tableAreaRef.current?.querySelector(".table-scroll") as HTMLDivElement | null;
    if (targetElement && container) {
      const targetRect = targetElement.getBoundingClientRect();
      const containerRect = container.getBoundingClientRect();
      const scrollOffset =
        container.scrollTop +
        (targetRect.top - containerRect.top) +
        targetRect.height / 2 -
        container.clientHeight / 2;
      container.scrollTo({ top: Math.max(0, scrollOffset), behavior: "smooth" });
    }
  }

  function moveMultiHighlightCursor(offset: number) {
    if (!isMultiHighlightMode || multiHighlightRows.length === 0) {
      return;
    }
    const nextCursor = Math.max(
      0,
      Math.min(multiHighlightRows.length - 1, activeHighlightCursor + offset),
    );
    if (nextCursor !== activeHighlightCursor) {
      const targetRow = multiHighlightRows[nextCursor];
      const highlightPage = Math.floor(targetRow / ROWS_PER_PAGE) + 1;
      setActiveHighlightCursor(nextCursor);
      pageChangeSourceRef.current = "highlight";
      setCurrentPage(highlightPage);
      setPageInput(String(highlightPage));
    } else {
      jumpToHighlight();
    }
  }

  function selectDateViewMode(nextMode: DateViewMode) {
    setDateViewMode(nextMode);
    setDateMenu(null);
  }

  function onDateMenuKeyDown(event: React.KeyboardEvent<HTMLDivElement>) {
    const currentTarget = event.target as HTMLElement | null;

    if (event.key === "ArrowDown") {
      event.preventDefault();
      focusByOffset(dateMenuItemRefs.current, currentTarget, 1);
      return;
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      focusByOffset(dateMenuItemRefs.current, currentTarget, -1);
      return;
    }

    if (event.key === "Home") {
      event.preventDefault();
      focusByIndex(dateMenuItemRefs.current, 0);
      return;
    }

    if (event.key === "End") {
      event.preventDefault();
      focusByIndex(dateMenuItemRefs.current, DATE_VIEW_OPTIONS.length - 1);
      return;
    }

    if (event.key === "Escape") {
      event.preventDefault();
      setDateMenu(null);
      return;
    }

    if (event.key === "Tab") {
      setDateMenu(null);
    }
  }

  if (!datasetId) {
    return null;
  }

  if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
    return (
      <div className="page-stack">
        <p className="error" role="alert">
          Invalid table id.
        </p>
      </div>
    );
  }

  if (tableNotFound) {
    return (
      <div className="page-stack full-table-page">
        <div className="table-view-back-row">
          <Link className="table-view-context-btn" to="/" aria-label="Back to home" title="Back to home">
            <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
              <path d="M20 11H7.83l4.59-4.59a1 1 0 1 0-1.42-1.41l-6.3 6.29a1 1 0 0 0 0 1.42l6.3 6.29a1 1 0 1 0 1.42-1.41L7.83 13H20a1 1 0 1 0 0-2Z" fill="currentColor" />
            </svg>
            Back to Home
          </Link>
        </div>
        <div className="card" style={{ marginBottom: 12, textAlign: "center" }} role="alert">
          <p style={{ margin: "0 0 4px 0", fontSize: 32, fontWeight: 700, color: "var(--brand-text)", letterSpacing: "-0.02em" }}>
            404
          </p>
          <p style={{ margin: "0 0 8px 0", fontSize: 20, fontWeight: 700, color: "var(--brand-text)" }}>
            Not Found
          </p>
          <p style={{ margin: 0, fontSize: 14, color: "var(--text-muted)" }}>
            The table may have been deleted or the ID might be invalid.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div
      className={`page-stack full-table-page${isMultiHighlightMode && multiHighlightRows.length > 0 ? " has-highlight-nav" : ""}`}
    >
      {hasQueryContext ? (
        (isMultiHighlightMode || highlightedRow !== null) && (
          <div className="table-view-back-row">
            <Link className="table-view-context-btn" to={returnPath} aria-label="Back to Query Results" title="Back to Query Results">
              <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                <path d="M20 11H7.83l4.59-4.59a1 1 0 1 0-1.42-1.41l-6.3 6.29a1 1 0 0 0 0 1.42l6.3 6.29a1 1 0 1 0 1.42-1.41L7.83 13H20a1 1 0 1 0 0-2Z" fill="currentColor" />
              </svg>
              Back to Query Results
            </Link>
          </div>
        )
      ) : (
        <div className="table-view-back-row">
          <Link className="table-view-context-btn" to="/" aria-label="Back to home" title="Back to home">
            <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
              <path d="M20 11H7.83l4.59-4.59a1 1 0 1 0-1.42-1.41l-6.3 6.29a1 1 0 0 0 0 1.42l6.3 6.29a1 1 0 1 0 1.42-1.41L7.83 13H20a1 1 0 1 0 0-2Z" fill="currentColor" />
            </svg>
            Back to Home
          </Link>
        </div>
      )}
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row table-view-header-row" style={{ justifyContent: "space-between" }}>
          <div className="table-view-header-main">
            <div className="table-view-title">{headerTitle}</div>
            {isMultiHighlightMode && (
              <div className="table-view-row-meta table-view-header-status">
                <span>
                  {multiHighlightLabel}: {multiHighlightTotal.toLocaleString()} results
                  {multiHighlightTruncated ? ` (showing first ${DEFAULT_MULTI_MAX_ROWS.toLocaleString()})` : ""}
                  {multiHighlightRows.length > 0
                    ? ` • Selected ${Math.min(activeHighlightCursor + 1, multiHighlightRows.length)} of ${multiHighlightRows.length}`
                    : ""}
                </span>
              </div>
            )}
            {!isMultiHighlightMode && highlightedRow !== null && (
              <div className="table-view-row-meta table-view-header-status">
                <span>Viewing:</span>{" "}
                <button
                  type="button"
                  className="table-view-row-jump"
                  onClick={jumpToHighlight}
                  aria-label={`Viewing row ${highlightedRow}. Click to jump to highlighted row`}
                  title={`Jump to highlighted row ${highlightedRow}`}
                >
                  Row {highlightedRow}
                </button>
              </div>
            )}
            {!isMultiHighlightMode && highlightedRow === null && (
              <div className="small table-view-header-status" role="status" aria-live="polite" aria-atomic="true">
                {loading
                  ? "Loading table page..."
                  : data && data.rows.length > 0
                    ? `Showing rows ${(data.offset + 1).toLocaleString()}-${(data.offset + data.rows.length).toLocaleString()} of ${effectiveRowCount.toLocaleString()}${normalizedSearch ? ` • ${filtered.rows.length.toLocaleString()} matches on this page` : ""}`
                    : `Showing 0 of ${effectiveRowCount.toLocaleString()} rows.`}
              </div>
            )}
          </div>
          <div className="table-view-tools">
            <label className="table-view-value-mode-toggle">
              <span className="table-view-value-mode-label">Values:</span>
              <select
                value={valueMode}
                onChange={(e) => setValueMode(e.target.value as ValueMode)}
                aria-label="Show original or normalized values"
              >
                <option value="normalized">Normalized</option>
                <option value="original">Original</option>
              </select>
            </label>
            <div className="table-view-search-wrap">
              <svg className="table-view-search-icon" viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                <path d="M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z" fill="currentColor" />
              </svg>
              <input
                type="text"
                className="table-view-search"
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                placeholder="Search for values"
                aria-label="Search rows"
              />
            </div>
          </div>
        </div>
      </div>

      {(err || highlightErr) && (
        <p className="error" role="alert">
          {err || highlightErr}
        </p>
      )}
      {data && (
        <div
          className={
            isMultiHighlightMode && multiHighlightRows.length > 0
              ? "table-area-outer has-highlight-nav"
              : "table-area-outer"
          }
        >
          {isMultiHighlightMode && multiHighlightRows.length > 0 && (
            <div className="table-view-highlight-nav-sidebar table-view-highlight-nav-sidebar-absolute" aria-label="Highlight navigation">
              <button
                type="button"
                className="table-view-highlight-nav-btn table-view-highlight-nav-btn-up"
                onClick={() => moveMultiHighlightCursor(-1)}
                disabled={activeHighlightCursor <= 0}
                aria-label="Previous highlighted row"
                title="Previous highlighted row"
              >
                ▲
              </button>
              <button
                type="button"
                className="table-view-highlight-nav-btn table-view-highlight-nav-btn-down"
                onClick={() => moveMultiHighlightCursor(1)}
                disabled={activeHighlightCursor >= multiHighlightRows.length - 1}
                aria-label="Next highlighted row"
                title="Next highlighted row"
              >
                ▼
              </button>
            </div>
          )}
          <div className="table-area-outer">
            <div className="table-area full-table-area" ref={tableAreaRef}>
            <DataTable
              columns={data.columns}
              columnLabels={
                data.columns_meta
                  ? data.columns_meta.map((m) =>
                      valueMode === "original"
                        ? (m.original_name ?? m.normalized_name)
                        : m.normalized_name,
                    )
                  : undefined
              }
              rows={displayRows}
              sortRows={sortRows}
              rowIndices={filtered.rowIndices}
              onRowClick={({ rowIndex, isHighlighted }) => {
                if (!isMultiHighlightMode || !isHighlighted) {
                  return;
                }
                const nextCursor = multiHighlightRows.indexOf(rowIndex);
                if (nextCursor !== -1) {
                  pageChangeSourceRef.current = "highlight";
                  setActiveHighlightCursor(nextCursor);
                }
              }}
              highlight={
                highlightedRows.length > 0
                  ? { rows: highlightedRows, cols: data.columns }
                  : undefined
              }
              sortable
              sortMode="server"
              serverSortColumn={sortColumn}
              serverSortDirection={sortDirection}
              onSortChange={(column, direction) => {
                setSortColumn(column);
                setSortDirection(direction);
                setCurrentPage(1);
                setPageInput("1");
              }}
              caption={`${tableName || "Table"} page ${safeCurrentPage}. ${displayRows.length} row${displayRows.length === 1 ? "" : "s"} shown.`}
              onCellContextMenu={(event, payload) => {
                if (!dateColumns.has(payload.column) || !parseDateToDate(payload.value)) {
                  return;
                }
                event.preventDefault();
                setDateMenu({ x: event.clientX, y: event.clientY });
              }}
            />
            {showScrollHint && (
              <button
                type="button"
                className="scroll-indicator full-table-scroll-indicator"
                onClick={scrollTableToEdge}
                aria-label={tableAtBottom ? "Scroll table to top" : "Scroll table to bottom"}
                title={tableAtBottom ? "Scroll to top" : "Scroll to bottom"}
              >
                {tableAtBottom ? "▲" : "▼"}
              </button>
            )}
            </div>
          </div>
        </div>
      )}
      {data && effectiveRowCount > 0 && (
        <div className="table-view-pagination" aria-label="Full table pagination">
          <div className="table-view-pagination-controls">
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => {
                pageChangeSourceRef.current = "table";
                setCurrentPage(1);
              }}
              disabled={loading || safeCurrentPage <= 1}
              aria-label="First page"
              title="First page"
            >
              {"<<"}
            </button>
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => {
                pageChangeSourceRef.current = "table";
                setCurrentPage(Math.max(1, safeCurrentPage - 1));
              }}
              disabled={loading || safeCurrentPage <= 1}
              aria-label="Previous page"
              title="Previous page"
            >
              {"<"}
            </button>
            <span className="table-view-page-count">
              Page{" "}
              <input
                type="text"
                inputMode="numeric"
                pattern="[0-9]*"
                className="table-view-page-input"
                style={{ width: `${pageInputWidthCh}ch` }}
                value={pageInput}
                onChange={(event) => {
                  const digitsOnly = event.target.value.replace(/[^\d]/g, "");
                  setPageInput(digitsOnly);
                }}
                onBlur={commitPageInput}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    commitPageInput();
                  } else if (event.key === "Escape") {
                    setPageInput(String(safeCurrentPage));
                  }
                }}
                disabled={loading}
                aria-label="Current page number"
                title="Enter page number"
              />{" "}
              of {totalPages}
            </span>
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => {
                pageChangeSourceRef.current = "table";
                setCurrentPage(Math.min(totalPages, safeCurrentPage + 1));
              }}
              disabled={loading || safeCurrentPage >= totalPages}
              aria-label="Next page"
              title="Next page"
            >
              {">"}
            </button>
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => {
                pageChangeSourceRef.current = "table";
                setCurrentPage(totalPages);
              }}
              disabled={loading || safeCurrentPage >= totalPages}
              aria-label="Last page"
              title="Last page"
            >
              {">>"}
            </button>
          </div>
        </div>
      )}
      {dateMenu && (
        <div
          ref={dateMenuRef}
          id={dateMenuId}
          className="date-context-menu"
          style={{ left: dateMenu.x, top: dateMenu.y }}
          role="menu"
          aria-label="Date format options"
          onKeyDown={onDateMenuKeyDown}
        >
          {DATE_VIEW_OPTIONS.map((option, index) => (
            <button
              key={option.value}
              ref={(element) => {
                dateMenuItemRefs.current[index] = element;
              }}
              type="button"
              role="menuitemradio"
              aria-checked={dateViewMode === option.value}
              className={dateViewMode === option.value ? "active" : undefined}
              onClick={() => selectDateViewMode(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
