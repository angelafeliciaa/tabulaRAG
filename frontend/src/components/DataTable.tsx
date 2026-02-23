type DataTableProps = {
  columns: string[];
  rows: Record<string, unknown>[];
  highlight?: { rows: number[]; cols: string[] };
  rowOffset?: number;
};

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

export default function DataTable({
  columns,
  rows,
  highlight,
  rowOffset = 0,
}: DataTableProps) {
  const highlightedRows = new Set((highlight?.rows || []).map((row) => row - rowOffset));
  const highlightedCols = new Set(highlight?.cols || []);

  return (
    <div className="card table-card">
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th className="mono">#</th>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => {
              const isHighlightedRow = highlightedRows.has(rowIndex);
              const absoluteRowIndex = rowIndex + rowOffset;

              return (
                <tr key={absoluteRowIndex} data-row-index={absoluteRowIndex}>
                  <td className={`mono ${isHighlightedRow ? "hl" : ""}`}>{absoluteRowIndex}</td>
                  {columns.map((column) => {
                    const isHighlightedCell =
                      isHighlightedRow && highlightedCols.has(column);
                    return (
                      <td key={`${absoluteRowIndex}:${column}`} className={isHighlightedCell ? "hl" : ""}>
                        {formatValue(row[column])}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
