import { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import {
  aggregate,
  filterRows,
  type AggregateResponse,
  type FilterResponse,
} from "../api";
import DataTable from "../components/DataTable";

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export default function VirtualTableView() {
  const location = useLocation();
  const [err, setErr] = useState<string | null>(null);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);
  const [resultTitle, setResultTitle] = useState<string>("Result");
  const [resultSubtitle, setResultSubtitle] = useState<string>("");

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
      setErr("This URL is not valid or no longer valid");
      return;
    }

    let payload: AggregatePayload | FilterPayload;

    try {
      payload = decodePayload(encoded);
    } catch {
      setErr("This URL is not valid or no longer valid");
      return;
    }

    if ("mode" in payload && payload.mode === "filter") {
      filterRows(payload)
        .then((result: FilterResponse) => {
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
            ...(item.row_data || {}),
          }));
          setRows(mappedRows);
        })
        .catch((error: unknown) => setErr(getErrorMessage(error)));
      return;
    }

    aggregate(payload)
      .then((result: AggregateResponse) => {
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
  }, [location.search]);

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
        <div className="result-title">{resultTitle}</div>
        <div className="result-subtitle">{resultSubtitle}</div>
      </div>

      {rows.length > 0 && (
        <div className="table-area">
          <DataTable
            columns={columns}
            rows={rows}
          />
        </div>
      )}
    </div>
  );
}
