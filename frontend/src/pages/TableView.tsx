import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getSlice,
  listTables,
  type TableSlice,
  type TableSummary,
} from "../api";
import DataTable from "../components/DataTable";
import returnIcon from "../images/return.png";

type DateViewMode = "default" | "mm-dd-yyyy" | "mon-dd-yyyy";
const ROWS_PER_PAGE = 500;

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

export default function TableView() {
  const { datasetId } = useParams();
  const numericDatasetId = Number(datasetId);

  const [data, setData] = useState<TableSlice | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tableName, setTableName] = useState<string | null>(null);
  const [tableRowCount, setTableRowCount] = useState<number>(0);
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [dateViewMode, setDateViewMode] = useState<DateViewMode>("default");
  const [dateMenu, setDateMenu] = useState<{ x: number; y: number } | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageInput, setPageInput] = useState("1");
  const [showScrollHint, setShowScrollHint] = useState(false);
  const [tableAtBottom, setTableAtBottom] = useState(false);
  const tableAreaRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    setErr(null);
    setData(null);
    setTableName(null);
    setTableRowCount(0);
    setDateMenu(null);

    let mounted = true;
    listTables()
      .then((tables: TableSummary[]) => {
        if (!mounted) {
          return;
        }
        const table = tables.find((row) => row.dataset_id === numericDatasetId);
        setTableName(table?.name || null);
        if (typeof table?.row_count === "number") {
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
    setCurrentPage(1);
    setPageInput("1");
    setSearchQuery("");
  }, [numericDatasetId]);

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    let mounted = true;
    setErr(null);
    setLoading(true);

    const pageOffset = (currentPage - 1) * ROWS_PER_PAGE;
    const pageEndExclusive = pageOffset + ROWS_PER_PAGE;

    getSlice(numericDatasetId, pageOffset, pageEndExclusive)
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
  }, [numericDatasetId, currentPage]);

  const normalizedSearch = searchQuery.trim().toLowerCase();
  const effectiveRowCount = Math.max(tableRowCount, Math.max(0, data?.row_count || 0));
  const totalPages = Math.max(1, Math.ceil(effectiveRowCount / ROWS_PER_PAGE));
  const safeCurrentPage = Math.min(currentPage, totalPages);
  const pageInputWidthCh = Math.max(2, String(totalPages).length + 1);
  const dateColumns = useMemo(
    () => (data ? detectDateColumns(data.rows, data.columns) : new Set<string>()),
    [data],
  );
  const filtered = useMemo(() => {
    if (!data) {
      return { rows: [], rowIndices: [] as number[] };
    }
    if (!normalizedSearch) {
      return {
        rows: data.rows,
        rowIndices: data.rows.map((_, index) => data.offset + index),
      };
    }

    const nextRows: Record<string, unknown>[] = [];
    const nextRowIndices: number[] = [];
    for (let i = 0; i < data.rows.length; i += 1) {
      const row = data.rows[i];
      const matches = Object.values(row).some((value) =>
        String(value ?? "").toLowerCase().includes(normalizedSearch),
      );
      if (matches) {
        nextRows.push(row);
        nextRowIndices.push(data.offset + i);
      }
    }
    return { rows: nextRows, rowIndices: nextRowIndices };
  }, [data, normalizedSearch]);

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
    setCurrentPage(nextPage);
    setPageInput(String(nextPage));
  }

  if (!datasetId) {
    return null;
  }

  if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
    return (
      <div className="page-stack">
        <p className="error">Invalid table id.</p>
      </div>
    );
  }

  return (
    <div className="page-stack full-table-page">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div>
            <div className="table-view-title">{tableName || "Table"}</div>
            <div className="small">
              {loading
                ? "Loading table page..."
                : data && data.rows.length > 0
                  ? `Showing rows ${(data.offset + 1).toLocaleString()}-${(data.offset + data.rows.length).toLocaleString()} of ${effectiveRowCount.toLocaleString()} (Page ${safeCurrentPage} of ${totalPages})${normalizedSearch ? ` • ${filtered.rows.length.toLocaleString()} matches on this page` : ""}.`
                  : `Showing 0 of ${effectiveRowCount.toLocaleString()} rows.`}
            </div>
          </div>
          <div className="table-view-tools">
            <input
              type="text"
              className="table-view-search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search for values"
              aria-label="Search rows"
            />
            <Link className="table-view-back-link" to="/">
              <img src={returnIcon} alt="" aria-hidden="true" />
              Back to All Uploads
            </Link>
          </div>
        </div>
      </div>

      {err && <p className="error">{err}</p>}
      {data && (
        <div className="table-area full-table-area" ref={tableAreaRef}>
          <DataTable
            columns={data.columns}
            rows={displayRows}
            rowIndices={filtered.rowIndices}
            sortable
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
      )}
      {data && effectiveRowCount > 0 && (
        <div className="table-view-pagination" aria-label="Full table pagination">
          <div className="table-view-pagination-controls">
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => setCurrentPage(1)}
              disabled={loading || safeCurrentPage <= 1}
              aria-label="First page"
              title="First page"
            >
              {"<<"}
            </button>
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
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
              onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
              disabled={loading || safeCurrentPage >= totalPages}
              aria-label="Next page"
              title="Next page"
            >
              {">"}
            </button>
            <button
              type="button"
              className="table-view-page-btn"
              onClick={() => setCurrentPage(totalPages)}
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
          className="date-context-menu"
          style={{ left: dateMenu.x, top: dateMenu.y }}
          role="menu"
          aria-label="Date format options"
        >
          <button type="button" onClick={() => setDateViewMode("default")}>
            Default
          </button>
          <button type="button" onClick={() => setDateViewMode("mm-dd-yyyy")}>
            MM-DD-YYYY
          </button>
          <button type="button" onClick={() => setDateViewMode("mon-dd-yyyy")}>
            Jan 12, 2002
          </button>
        </div>
      )}
    </div>
  );
}
