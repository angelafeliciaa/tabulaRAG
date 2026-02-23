import { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { getSlice, listTables, type TableSlice, type TableSummary } from "../api";
import DataTable from "../components/DataTable";

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}

export default function TableView() {
  const { datasetId } = useParams();
  const numericDatasetId = Number(datasetId);

  const [data, setData] = useState<TableSlice | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [tableName, setTableName] = useState<string | null>(null);

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    getSlice(numericDatasetId, 0, 100)
      .then(setData)
      .catch((error: unknown) => setErr(getErrorMessage(error)));
  }, [numericDatasetId]);

  useEffect(() => {
    if (!Number.isFinite(numericDatasetId) || numericDatasetId <= 0) {
      return;
    }

    listTables()
      .then((tables: TableSummary[]) => {
        const table = tables.find((row) => row.dataset_id === numericDatasetId);
        setTableName(table?.name || null);
      })
      .catch(() => setTableName(null));
  }, [numericDatasetId]);

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
    <div className="page-stack">
      <div className="card" style={{ marginBottom: 12 }}>
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div>
            <div className="mono">{tableName || "Table"}</div>
            <div className="small">Showing first 100 rows.</div>
          </div>
        </div>
      </div>

      {err && <p className="error">{err}</p>}
      {data && (
        <div className="table-area">
          <DataTable columns={data.columns} rows={data.rows} />
        </div>
      )}
    </div>
  );
}
