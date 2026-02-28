import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import {
  getFullTableSlice,
  listTables,
  type TableSlice,
  type TableSummary,
} from "../api";
import DataTable from "../components/DataTable";

type DateViewMode = "default" | "mm-dd-yyyy" | "mon-dd-yyyy";

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
    /^(\d{4})[\/.-](\d{1,2})[\/.-](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
  );
  if (isoMatch) {
    const yyyy = isoMatch[1];
    const mm = isoMatch[2].padStart(2, "0");
    const dd = isoMatch[3].padStart(2, "0");
    const parsed = new Date(`${yyyy}-${mm}-${dd}T00:00:00.000Z`);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const dmyOrMdy = text.match(
    /^(\d{1,2})[\/.-](\d{1,2})[\/.-](\d{2,4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
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

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    setErr(null);
    setData(null);
    setLoading(true);

    let mounted = true;
    listTables()
      .then(async (tables: TableSummary[]) => {
        if (!mounted) {
          return;
        }
        const table = tables.find((row) => row.dataset_id === numericDatasetId);
        const rowCount = table?.row_count ?? 0;
        setTableName(table?.name || null);
        setTableRowCount(rowCount);
        const fullSlice = await getFullTableSlice(numericDatasetId, rowCount);
        if (!mounted) {
          return;
        }
        setData(fullSlice);
      })
      .catch((error: unknown) => setErr(getErrorMessage(error)))
      .finally(() => {
        if (mounted) {
          setLoading(false);
        }
      });

    return () => {
      mounted = false;
    };
  }, [numericDatasetId]);

  const normalizedSearch = searchQuery.trim().toLowerCase();
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
        rowIndices: data.rows.map((_, index) => index),
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
        nextRowIndices.push(i);
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
                ? "Loading full table..."
                : `Showing ${filtered.rows.length.toLocaleString()} of ${tableRowCount.toLocaleString()} rows.`}
            </div>
          </div>
          <div className="table-view-tools">
            <input
              type="text"
              className="table-view-search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search all columns"
              aria-label="Search rows"
            />
            <Link className="glass table-view-back-link" to="/">
              Back To All Uploads
            </Link>
          </div>
        </div>
      </div>

      {err && <p className="error">{err}</p>}
      {data && (
        <div className="table-area">
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
