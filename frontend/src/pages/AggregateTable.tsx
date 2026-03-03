import { useEffect, useState } from "react";
import { useLocation } from "react-router-dom";
import { aggregate, type AggregateResponse } from "../api";
import DataTable from "../components/DataTable";

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

export default function VirtualTableView() {
  const location = useLocation();
  const [data, setData] = useState<AggregateResponse | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [highlightIndex, setHighlightIndex] = useState<number>(0);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Record<string, unknown>[]>([]);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const encoded = params.get("q");
    if (!encoded) {
      setErr("Missing query parameter.");
      return;
    }

    let payload: {
      dataset_id: number;
      operation: string;
      metric_column?: string;
      group_by?: string;
      filters?: unknown[];
      highlight_index?: number;
      limit?: number;
    };

    try {
      payload = JSON.parse(atob(encoded));
    } catch {
      setErr("Invalid query parameter.");
      return;
    }

    setHighlightIndex(payload.highlight_index ?? 0);

    aggregate(payload)
      .then((result: AggregateResponse) => {
        setData(result);

        // Build columns
        const cols: string[] = [];
        if (result.group_by_column) cols.push(result.group_by_column);
        cols.push(result.metric_column ?? "aggregate_value");
        setColumns(cols);

        // Build rows — remap group_value/aggregate_value to real column names
        const remapped = result.rowsResult.map((row) => {
          const r: Record<string, unknown> = {};
          if (result.group_by_column) r[result.group_by_column] = row.group_value;
          r[result.metric_column ?? "aggregate_value"] = row.aggregate_value;
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
    <div className="page-stack">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="mono">Aggregate Result</div>
        <div className="small">
          {data?.group_by_column
            ? `${data.metric_column} by ${data.group_by_column}`
            : data?.metric_column}
        </div>
      </div>

      {rows.length > 0 && (
        <div className="table-area">
          <DataTable
            columns={columns}
            rows={rows}
            highlight={{ rows: [highlightIndex], cols: columns }}
          />
        </div>
      )}
    </div>
  );
}