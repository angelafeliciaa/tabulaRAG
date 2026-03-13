import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import {
  aggregate,
  filterRows,
  type AggregateResponse,
  type FilterResponse,
} from "../api";
import DataTable from "../components/DataTable";
import openIcon from "../images/open.png";

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export default function VirtualTableView() {
  const location = useLocation();
  const [err, setErr] = useState<string | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<(Record<string, unknown> & { __highlight_id?: string })[]>([]);
  const [resultTitle, setResultTitle] = useState<string>("Result");
  const [resultSubtitle, setResultSubtitle] = useState<string>("");
  const [isFilterResult, setIsFilterResult] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");

  type AggregatePayload = {
    dataset_id: number;
    operation: string;
    metric_column?: string;
    group_by?: string;
    filters?: FilterConditionPayload[];
    limit?: number;
  };

  type FilterConditionPayload = {
    column: string;
    operator: string;
    value?: string;
    logical_operator?: "AND" | "OR";
  };

  type FilterPayload = {
    mode: "filter";
    dataset_id: number;
    filters?: FilterConditionPayload[];
    limit?: number;
    offset?: number;
  };

  function decodePayload(encoded: string): AggregatePayload | FilterPayload {
    const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
    const pad = normalized.length % 4;
    const padded = pad ? normalized + "=".repeat(4 - pad) : normalized;
    return JSON.parse(atob(padded));
  }

  function formatFilterSummary(filters?: FilterConditionPayload[]): string {
    if (!filters || filters.length === 0) return "no filters";
    return filters
      .map((f, idx) => {
        const clause =
          f.operator === "IS NULL" || f.operator === "IS NOT NULL"
            ? `${f.column} ${f.operator}`
            : `${f.column} ${f.operator} ${f.value ?? ""}`.trim();
        if (idx === 0) return clause;
        return `${(f.logical_operator || "AND").toUpperCase()} ${clause}`;
      })
      .join(" ");
  }

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const encoded = params.get("q");
    if (!encoded) {
      setErr("This URL is not valid or no longer valid");      return;
    }

    let payload: AggregatePayload | FilterPayload;

    try {
      payload = decodePayload(encoded);
    } catch {
      setErr("This URL is not valid or no longer valid");      return;
    }

    if ("mode" in payload && payload.mode === "filter") {
      filterRows(payload)
        .then((result: FilterResponse) => {
          setIsFilterResult(true);
          setResultTitle(`Filter result: ${formatFilterSummary(payload.filters)}`);
          setResultSubtitle(`${result.row_count} matching rows`);

          const columnSet = new Set<string>(["row_index"]);
          for (const item of result.rowsResult) {
            for (const key of Object.keys(item.row_data || {})) {
              columnSet.add(key);
            }
          }
          const cols = Array.from(columnSet);
          setColumns(cols);

          const mappedRows = result.rowsResult.map((item) => ({
            row_index: item.row_index,
            __highlight_id: item.highlight_id,
            ...(item.row_data || {}),
          }));
          setRows(mappedRows);
        })
        .catch((error: unknown) => setErr(getErrorMessage(error)));
      return;
    }

    aggregate(payload)
      .then((result: AggregateResponse) => {
        setIsFilterResult(false);
        const aggregatePayload = payload as AggregatePayload;

        const operationLabel = aggregatePayload.operation.charAt(0).toUpperCase() + aggregatePayload.operation.slice(1);
        const metricCol = result.metric_column ?? "aggregate_value";

        const filterParts = aggregatePayload.filters
          ? formatFilterSummary(aggregatePayload.filters)
          : null;

        const metricColLabel = filterParts
          ? `${operationLabel} of ${metricCol} (${filterParts})`
          : `${operationLabel} of ${metricCol}`;
        const aggregateSummary = result.group_by_column
          ? `${operationLabel} ${metricCol} by ${result.group_by_column}`
          : `${operationLabel} ${metricCol}`;
        setResultTitle(`Aggregate result: ${aggregateSummary}`);
        setResultSubtitle(filterParts ? `Filters: ${filterParts}` : `${result.rowsResult.length} row(s)`);

        const cols: string[] = [];
        if (result.group_by_column) cols.push(result.group_by_column);
        cols.push(metricColLabel);
        setColumns(cols);

        const remapped = result.rowsResult.map((row) => {
          const r: Record<string, unknown> = {};
          if (result.group_by_column) r[result.group_by_column] = row.group_value;
          r[metricColLabel] = row.aggregate_value;
          return r;
        });
        setRows(remapped);
      })
      .catch((error: unknown) => setErr(getErrorMessage(error)));
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.search]);

  useEffect(() => {
    setSearchQuery("");
  }, [location.search]);

  const normalizedSearch = searchQuery.trim().toLowerCase();
  const filtered = useMemo(() => {
    if (!normalizedSearch) {
      return {
        rows,
        rowIndices: rows.map((row, index) =>
          typeof row.row_index === "number" ? Number(row.row_index) : index,
        ),
      };
    }

    const nextRows: (Record<string, unknown> & { __highlight_id?: string })[] = [];
    const nextRowIndices: number[] = [];
    for (let i = 0; i < rows.length; i += 1) {
      const row = rows[i];
      const matches = Object.values(row).some((value) =>
        String(value ?? "").toLowerCase().includes(normalizedSearch),
      );
      if (!matches) {
        continue;
      }
      nextRows.push(row);
      nextRowIndices.push(
        typeof row.row_index === "number" ? Number(row.row_index) : i,
      );
    }

    return { rows: nextRows, rowIndices: nextRowIndices };
  }, [rows, normalizedSearch]);

  if (err) {
    return (
      <div className="page-stack">
        <p className="error">{err}</p>
      </div>
    );
  }

  return (
    <div className="page-stack virtual-results-page">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row virtual-results-header-row">
          <div>
            <div className="result-title">{resultTitle}</div>
            <div className="result-subtitle">{resultSubtitle}</div>
            <div className="small">
              Showing {filtered.rows.length.toLocaleString()} of {rows.length.toLocaleString()} row(s)
            </div>
          </div>
          <div className="table-view-tools virtual-results-tools">
            <input
              type="text"
              className="table-view-search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search for values"
              aria-label="Search results rows"
            />
            <Link className="table-view-icon-btn" to="/" aria-label="Back to All Uploads" title="Back to All Uploads">
              <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
                <path d="M12 4.2 4 10v9a1 1 0 0 0 1 1h4.8a1 1 0 0 0 1-1v-4.2h2.4V19a1 1 0 0 0 1 1H19a1 1 0 0 0 1-1v-9l-8-5.8Z" fill="currentColor" />
              </svg>
            </Link>
          </div>
        </div>
      </div>

      {filtered.rows.length > 0 && (
        <div className="table-area">
          <DataTable
            columns={columns}
            rows={filtered.rows}
            rowIndices={filtered.rowIndices}
            sortable
            rowAction={
              isFilterResult
                ? ({ row }) => {
                    const highlightId = row.__highlight_id;
                    if (!highlightId || typeof highlightId !== "string") {
                      return null;
                    }
                    return (
                      <Link
                        className="result-row-open-link"
                        to={`/highlight/${encodeURIComponent(highlightId)}?return_to=${encodeURIComponent(`${location.pathname}${location.search}`)}`}
                        aria-label={`Show source row ${String(row.row_index ?? "")}`}
                        title="Show in original table"
                      >
                        <img src={openIcon} alt="" aria-hidden="true" />
                      </Link>
                    );
                  }
                : undefined
            }
            rowActionLabel=""
          />
        </div>
      )}
    </div>
  );
}
