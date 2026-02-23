import { useEffect, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import {
  getHighlight,
  getSlice,
  listTables,
  type HighlightResponse,
  type TableSlice,
  type TableSummary,
} from "../api";
import DataTable from "../components/DataTable";
import logo from "../images/logo.png";
import returnIcon from "../images/return.png";

type HighlightTarget = {
  row: number;
  cols: string[];
};

type HighlightSlice = TableSlice & {
  rowFrom: number;
  highlight: {
    rows: number[];
    cols: string[];
  };
};

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export default function HighlightView() {
  const { highlightId } = useParams();
  const location = useLocation();

  const [highlight, setHighlight] = useState<HighlightResponse | null>(null);
  const [slice, setSlice] = useState<HighlightSlice | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tableName, setTableName] = useState<string | null>(null);
  const [targets, setTargets] = useState<HighlightTarget[]>([]);
  const [targetIndex, setTargetIndex] = useState(0);
  const queryText = new URLSearchParams(location.search).get("q");

  useEffect(() => {
    if (!highlightId) {
      return;
    }

    let cancelled = false;

    (async () => {
      setErr(null);
      const h = await getHighlight(highlightId);
      if (cancelled) {
        return;
      }

      setHighlight(h);
      setTargets([{ row: h.row_index, cols: h.column ? [h.column] : [] }]);

      try {
        const tables = await listTables();
        if (cancelled) {
          return;
        }
        const table = tables.find((row: TableSummary) => row.dataset_id === h.dataset_id);
        setTableName(table?.name || null);
      } catch {
        if (!cancelled) {
          setTableName(null);
        }
      }
    })().catch((error: unknown) => {
      if (!cancelled) {
        setErr(getErrorMessage(error));
        setSlice(null);
      }
    });

    return () => {
      cancelled = true;
    };
  }, [highlightId]);

  useEffect(() => {
    if (!highlight || targets.length === 0) {
      return;
    }

    const currentTarget = targets[targetIndex] || targets[0];
    if (!currentTarget) {
      return;
    }

    let cancelled = false;

    (async () => {
      setErr(null);
      const rowFrom = Math.max(0, currentTarget.row - 5);
      const rowTo = currentTarget.row + 6;
      const loadedSlice = await getSlice(highlight.dataset_id, rowFrom, rowTo);

      if (cancelled) {
        return;
      }

      setSlice({
        ...loadedSlice,
        rowFrom,
        // Highlight the full matching row across all visible columns.
        highlight: { rows: [currentTarget.row], cols: loadedSlice.columns },
      });
    })().catch((error: unknown) => {
      if (!cancelled) {
        setErr(getErrorMessage(error));
      }
    });

    return () => {
      cancelled = true;
    };
  }, [highlight, targets, targetIndex]);

  useEffect(() => {
    if (!slice?.highlight.rows.length) {
      return;
    }

    const targetRow = slice.highlight.rows[0];
    const targetElement = document.querySelector(
      `[data-row-index="${targetRow}"]`,
    ) as HTMLElement | null;

    if (!targetElement) {
      return;
    }

    window.setTimeout(() => {
      targetElement.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 0);
  }, [slice]);

  if (!highlightId) {
    return null;
  }

  const totalTargets = targets.length;
  const pageLabel = totalTargets ? `${targetIndex + 1} of ${totalTargets}` : "0 of 0";

  return (
    <div className="page-stack">
      <div className="highlight-header">
        <img src={logo} alt="TabulaRAG" className="hero-logo" />
        <div className="highlight-title">TabulaRAG</div>
      </div>

      <div className="top-info top-info-center">
        {highlight?.dataset_id && <div className="small">Table: {tableName || "Table"}</div>}
        {queryText && <div className="small">Query: {queryText}</div>}
      </div>

      {err && <p className="error">{err}</p>}

      {slice && (
        <>
          <div className="top-info top-info-row">
            <div className="small">
              Showing rows {slice.rowFrom}..{slice.rowFrom + slice.rows.length - 1}
            </div>
          </div>

          <div className="table-area">
            <DataTable
              columns={slice.columns}
              rows={slice.rows}
              highlight={slice.highlight}
              rowOffset={slice.rowFrom}
            />
          </div>

          <div className="return-row">
            <div className="highlight-pagination" aria-label="Highlighted row pagination">
              <button
                type="button"
                className="pager-btn"
                onClick={() => setTargetIndex((index) => Math.max(0, index - 1))}
                disabled={targetIndex <= 0}
                aria-label="Previous highlight"
              >
                {"<"}
              </button>
              <span className="pager-count">{pageLabel}</span>
              <button
                type="button"
                className="pager-btn"
                onClick={() => setTargetIndex((index) => Math.min(totalTargets - 1, index + 1))}
                disabled={targetIndex >= totalTargets - 1}
                aria-label="Next highlight"
              >
                {">"}
              </button>
            </div>

            <Link className="return-link" to="/">
              <img src={returnIcon} alt="" />
              Return to Upload Page
            </Link>
          </div>
        </>
      )}
    </div>
  );
}
