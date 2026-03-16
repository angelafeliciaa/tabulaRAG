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

const MAX_MULTI_HIGHLIGHT_ROWS = 1000;

type FilterConditionPayload = {
  column: string;
  operator: string;
  value?: string;
  logical_operator?: "AND" | "OR";
};

type AggregatePayload = {
  dataset_id: number;
  operation: string;
  metric_column?: string;
  group_by?: string;
  filters?: FilterConditionPayload[];
  limit?: number;
};

type FilterPayload = {
  mode: "filter";
  dataset_id: number;
  filters?: FilterConditionPayload[];
  limit?: number;
  offset?: number;
};

type TableRow = Record<string, unknown> & {
  __highlight_id?: string;
  __row_index?: number;
  __dataset_id?: number;
  __drilldown_filters?: FilterConditionPayload[];
  __drilldown_label?: string;
};

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

const CURRENCY_SYMBOL: Record<string, string> = {
  USD: "$",
  EUR: "€",
  GBP: "£",
  JPY: "¥",
  INR: "₹",
  CAD: "C$",
  AUD: "A$",
  CHF: "CHF",
  CNY: "¥",
  KRW: "₩",
  THB: "฿",
  TRY: "₺",
  RUB: "₽",
};

function formatAggregateValue(
  value: number,
  currency: string | null | undefined,
  unit: string | null | undefined,
): string | number {
  if (currency != null && currency !== "") {
    const symbol = CURRENCY_SYMBOL[currency] ?? `${currency} `;
    const formatted = value.toFixed(2);
    return `${symbol}${formatted}`;
  }
  if (unit != null && unit !== "") {
    const formatted = Number.isInteger(value) ? String(value) : String(value);
    return `${formatted} ${unit}`;
  }
  return value;
}

function encodePayload(value: unknown): string {
  const raw = JSON.stringify(value);
  const bytes = new TextEncoder().encode(raw);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function decodePayload(encoded: string): AggregatePayload | FilterPayload {
  const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const pad = normalized.length % 4;
  const padded = pad ? normalized + "=".repeat(4 - pad) : normalized;
  const binary = atob(padded);
  const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
  const decoded = new TextDecoder().decode(bytes);
  return JSON.parse(decoded);
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

export default function VirtualTableView() {
  const location = useLocation();
  const [err, setErr] = useState<string | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<TableRow[]>([]);
  const [resultTitle, setResultTitle] = useState<string>("Result");
  const [resultSubtitle, setResultSubtitle] = useState<string>("");

  const [searchState, setSearchState] = useState<{ key: string; value: string }>({
    key: "",
    value: "",
  });
  const parsedQuery = useMemo(() => {
    const params = new URLSearchParams(location.search);
    let encoded = params.get("q");
    if (!encoded && location.hash) {
      const hashParams = new URLSearchParams(location.hash.slice(1));
      encoded = hashParams.get("q");
    }
    if (!encoded) {
      return { payload: null as AggregatePayload | FilterPayload | null, error: "This URL is not valid or no longer valid" };
    }
    try {
      return { payload: decodePayload(encoded), error: null as string | null };
    } catch {
      return { payload: null as AggregatePayload | FilterPayload | null, error: "This URL is not valid or no longer valid" };
    }
  }, [location.search, location.hash]);

  useEffect(() => {
    if (parsedQuery.error) {
      return;
    }
    if (!parsedQuery.payload) {
      return;
    }

    const payload = parsedQuery.payload;

    if ("mode" in payload && payload.mode === "filter") {
      filterRows(payload)
        .then((result: FilterResponse) => {
          setErr(null);
          setResultTitle(`Filter result: ${formatFilterSummary(payload.filters)}`);
          setResultSubtitle("");

          const columnSet = new Set<string>(["row_index"]);
          for (const item of result.rowsResult) {
            for (const key of Object.keys(item.row_data || {})) {
              columnSet.add(key);
            }
          }
          setColumns(Array.from(columnSet));

          const mappedRows: TableRow[] = result.rowsResult.map((item) => ({
            row_index: item.row_index,
            __row_index: item.row_index,
            __dataset_id: result.dataset_id,
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
        setErr(null);
        const aggregatePayload = payload as AggregatePayload;

        const operationLabel =
          aggregatePayload.operation.charAt(0).toUpperCase() + aggregatePayload.operation.slice(1);
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
        setResultSubtitle(filterParts ? `Filters: ${filterParts}` : "");

        const cols: string[] = [];
        if (result.group_by_column) cols.push(result.group_by_column);
        cols.push(metricColLabel);
        setColumns(cols);

        const remapped: TableRow[] = result.rowsResult.map((row) => {
          const source = row as Record<string, unknown>;
          const groupValue = source.group_value;
          const nextRow: TableRow = {};
          if (result.group_by_column) {
            nextRow[result.group_by_column] = row.group_value;
          }
          nextRow[metricColLabel] = formatAggregateValue(
            row.aggregate_value,
            result.metric_currency ?? null,
            result.metric_unit ?? null,
          );
          nextRow.__dataset_id = result.dataset_id;

          const drilldownFilters = [...(aggregatePayload.filters || [])];
          if (result.group_by_column) {
            if (groupValue === null || groupValue === undefined) {
              drilldownFilters.push({
                column: result.group_by_column,
                operator: "IS NULL",
              });
              nextRow.__drilldown_label = "NULL";
            } else {
              drilldownFilters.push({
                column: result.group_by_column,
                operator: "=",
                value: String(groupValue),
              });
              nextRow.__drilldown_label = String(groupValue);
            }
          } else {
            nextRow.__drilldown_label = "All matching rows";
          }
          nextRow.__drilldown_filters = drilldownFilters;
          return nextRow;
        });
        setRows(remapped);
      })
      .catch((error: unknown) => setErr(getErrorMessage(error)));
  }, [parsedQuery]);

  const searchQuery = searchState.key === location.search ? searchState.value : "";
  const normalizedSearch = searchQuery.trim().toLowerCase();
  const filtered = useMemo(() => {
    if (!normalizedSearch) {
      return {
        rows,
        rowIndices: rows.map((row, index) => {
          if (typeof row.__row_index === "number") {
            return row.__row_index;
          }
          if (typeof row.row_index === "number") {
            return Number(row.row_index);
          }
          return index;
        }),
      };
    }

    const nextRows: TableRow[] = [];
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
      if (typeof row.__row_index === "number") {
        nextRowIndices.push(row.__row_index);
      } else if (typeof row.row_index === "number") {
        nextRowIndices.push(Number(row.row_index));
      } else {
        nextRowIndices.push(i);
      }
    }

    return { rows: nextRows, rowIndices: nextRowIndices };
  }, [rows, normalizedSearch]);

  const hasRowDrilldown = useMemo(
    () =>
      filtered.rows.some((row) => {
        const sourceDataset =
          typeof row.__dataset_id === "number"
            ? row.__dataset_id
            : null;
        const hasSingleRow =
          typeof row.__row_index === "number"
          || typeof row.row_index === "number";
        const hasMultiRow = Array.isArray(row.__drilldown_filters);
        return sourceDataset !== null && (hasSingleRow || hasMultiRow);
      }),
    [filtered.rows],
  );

  const displayError = parsedQuery.error || err;
  if (displayError) {
    return (
      <div className="page-stack">
        <p className="error" role="alert">
          {displayError}
        </p>
      </div>
    );
  }

  return (
    <div className="page-stack virtual-results-page">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row virtual-results-header-row">
          <div className="virtual-results-header-main">
            <div className="result-title">{resultTitle}</div>
            {resultSubtitle ? <div className="result-subtitle">{resultSubtitle}</div> : null}
            <div className="small">
              Showing {filtered.rows.length.toLocaleString()} of {rows.length.toLocaleString()} row(s)
            </div>
          </div>
          <div className="table-view-tools virtual-results-tools">
            <input
              type="text"
              className="table-view-search"
              value={searchQuery}
              onChange={(event) =>
                setSearchState({
                  key: location.search,
                  value: event.target.value,
                })}
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
              hasRowDrilldown
                ? ({ row }) => {
                  const sourceDataset =
                    typeof row.__dataset_id === "number"
                      ? row.__dataset_id
                      : null;
                  const sourceRow =
                    typeof row.__row_index === "number"
                      ? row.__row_index
                      : typeof row.row_index === "number"
                        ? Number(row.row_index)
                        : null;

                  if (sourceDataset !== null && Array.isArray(row.__drilldown_filters)) {
                    const label = String(row.__drilldown_label || "All matching rows");
                    const spec = encodePayload({
                      dataset_id: sourceDataset,
                      filters: row.__drilldown_filters,
                      label,
                      max_rows: MAX_MULTI_HIGHLIGHT_ROWS,
                    });
                    return (
                      <Link
                        className="result-row-open-link"
                        to={`/tables/${sourceDataset}?highlight_mode=multi&highlight_spec=${encodeURIComponent(spec)}&return_to=${encodeURIComponent(`${location.pathname}${location.search}`)}`}
                        aria-label={`Open ${label} rows in full table`}
                        title="Open in full table"
                      >
                        <img src={openIcon} alt="" aria-hidden="true" />
                      </Link>
                    );
                  }

                  if (sourceDataset !== null && sourceRow !== null) {
                    return (
                      <Link
                        className="result-row-open-link"
                        to={`/tables/${sourceDataset}?highlight_row=${sourceRow}&return_to=${encodeURIComponent(`${location.pathname}${location.search}`)}`}
                        aria-label={`Open row ${sourceRow} in full table`}
                        title="Open in full table"
                      >
                        <img src={openIcon} alt="" aria-hidden="true" />
                      </Link>
                    );
                  }
                  return null;
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
