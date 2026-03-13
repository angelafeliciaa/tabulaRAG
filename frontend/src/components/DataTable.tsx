import { type MouseEvent, useMemo, useState } from "react";

type DataTableProps = {
  columns: string[];
  rows: Record<string, unknown>[];
  caption?: string;
  highlight?: { rows: number[]; cols: string[] };
  rowOffset?: number;
  rowIndices?: number[];
  sortable?: boolean;
  formatCellValue?: (column: string, value: unknown) => string;
  onCellContextMenu?: (
    event: MouseEvent<HTMLTableCellElement>,
    payload: { column: string; value: unknown; rowIndex: number },
  ) => void;
};

type SortDirection = "asc" | "desc";
type SortKind = "number" | "date" | "text";

function parseNumberLike(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  const normalizedNumeric = text
    .replace(/\((.*)\)/, "-$1")
    .replace(/[$€£¥₹,\s]/g, "")
    .replace(/[^0-9.+-]/g, "");
  const numericCandidate = Number(normalizedNumeric);
  if (
    Number.isFinite(numericCandidate)
    && /[0-9]/.test(normalizedNumeric)
    && /^[-+]?\d*\.?\d+$/.test(normalizedNumeric)
  ) {
    return numericCandidate;
  }
  return null;
}

function parseDateToEpoch(value: string): number | null {
  const text = value.trim();
  if (!text) {
    return null;
  }

  // YYYY-MM-DD (or YYYY/MM/DD, YYYY.MM.DD) with optional time.
  const isoMatch = text.match(
    /^(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
  );
  if (isoMatch) {
    const year = Number(isoMatch[1]);
    const month = Number(isoMatch[2]);
    const day = Number(isoMatch[3]);
    const epoch = Date.UTC(year, month - 1, day);
    if (!Number.isNaN(epoch)) {
      return epoch;
    }
  }

  // DD/MM/YYYY or MM/DD/YYYY (delimiter can be / . -). If ambiguous, prefer DD/MM.
  const dmyOrMdyMatch = text.match(
    /^(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})(?:[ T]\d{1,2}:\d{2}(?::\d{2})?)?$/,
  );
  if (dmyOrMdyMatch) {
    const a = Number(dmyOrMdyMatch[1]);
    const b = Number(dmyOrMdyMatch[2]);
    const yearRaw = Number(dmyOrMdyMatch[3]);
    const year = yearRaw < 100 ? 2000 + yearRaw : yearRaw;

    let day = a;
    let month = b;
    if (a <= 12 && b > 12) {
      month = a;
      day = b;
    } else if (a <= 12 && b <= 12) {
      // Ambiguous, choose D/M to match common CSV exports in this project.
      day = a;
      month = b;
    }

    if (month >= 1 && month <= 12 && day >= 1 && day <= 31) {
      const epoch = Date.UTC(year, month - 1, day);
      if (!Number.isNaN(epoch)) {
        return epoch;
      }
    }
  }

  // Fallback for textual month formats only when the input looks date-like.
  if (/[a-zA-Z]/.test(text)) {
    const parsed = Date.parse(text);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  return null;
}

function defaultFormatValue(value: unknown): string {
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

function getSortableValue(value: unknown): { kind: "empty" | "number" | "date" | "text"; value: string | number } {
  if (value === null || value === undefined) {
    return { kind: "empty", value: "" };
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    return { kind: "number", value };
  }

  const text = String(value).trim();
  if (!text) {
    return { kind: "empty", value: "" };
  }

  const numericCandidate = parseNumberLike(text);
  if (numericCandidate !== null) {
    return { kind: "number", value: numericCandidate };
  }

  const parsedDateEpoch = parseDateToEpoch(text);
  if (parsedDateEpoch !== null) {
    return { kind: "date", value: parsedDateEpoch };
  }

  return { kind: "text", value: text.toLowerCase() };
}

function inferSortKind(rows: Record<string, unknown>[], column: string): SortKind {
  const sample = rows.slice(0, 300);
  let nonEmpty = 0;
  let numericHits = 0;
  let dateHits = 0;
  for (const row of sample) {
    const value = row[column];
    const text = value === null || value === undefined ? "" : String(value).trim();
    if (!text) {
      continue;
    }
    nonEmpty += 1;
    if (parseNumberLike(text) !== null) {
      numericHits += 1;
      continue;
    }
    if (parseDateToEpoch(text) !== null) {
      dateHits += 1;
    }
  }

  if (nonEmpty === 0) {
    return "text";
  }

  const numericRatio = numericHits / nonEmpty;
  const dateRatio = dateHits / nonEmpty;
  if (numericRatio >= 0.6) {
    return "number";
  }
  if (dateRatio >= 0.6) {
    return "date";
  }
  return "text";
}

export default function DataTable({
  columns,
  rows,
  caption,
  highlight,
  rowOffset = 0,
  rowIndices,
  sortable = false,
  formatCellValue,
  onCellContextMenu,
}: DataTableProps) {
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>("asc");
  const highlightedRows = new Set(highlight?.rows || []);
  const highlightedCols = new Set(highlight?.cols || []);

  const displayRows = useMemo(() => {
    const entries = rows.map((row, index) => ({
      row,
      absoluteRowIndex:
        typeof rowIndices?.[index] === "number"
          ? Number(rowIndices[index])
          : rowOffset + index,
      originalIndex: index,
    }));

    if (!sortable || !sortColumn) {
      return entries;
    }

    const sign = sortDirection === "asc" ? 1 : -1;
    const sortKind = inferSortKind(rows, sortColumn);
    return [...entries].sort((left, right) => {
      const leftValue = getSortableValue(left.row[sortColumn]);
      const rightValue = getSortableValue(right.row[sortColumn]);

      if (leftValue.kind === "empty" && rightValue.kind === "empty") {
        return left.originalIndex - right.originalIndex;
      }
      if (leftValue.kind === "empty") {
        return 1;
      }
      if (rightValue.kind === "empty") {
        return -1;
      }

      if (sortKind === "number" && leftValue.kind === "number" && rightValue.kind === "number") {
        const delta = Number(leftValue.value) - Number(rightValue.value);
        if (delta !== 0) {
          return delta * sign;
        }
      } else if (sortKind === "date" && leftValue.kind === "date" && rightValue.kind === "date") {
        const delta = Number(leftValue.value) - Number(rightValue.value);
        if (delta !== 0) {
          return delta * sign;
        }
      } else {
        const delta = String(leftValue.value).localeCompare(String(rightValue.value), undefined, {
          sensitivity: "base",
          numeric: true,
        });
        if (delta !== 0) {
          return delta * sign;
        }
      }

      return left.originalIndex - right.originalIndex;
    });
  }, [rowIndices, rowOffset, rows, sortColumn, sortDirection, sortable]);

  function toggleSort(column: string) {
    if (sortColumn !== column) {
      setSortColumn(column);
      setSortDirection("asc");
      return;
    }
    if (sortDirection === "asc") {
      setSortDirection("desc");
      return;
    }
    setSortColumn(null);
    setSortDirection("asc");
  }

  return (
    <div className="card table-card">
      <div className="table-scroll">
        <table>
          <caption className="sr-only">
            {caption || `Data table with ${displayRows.length} rows and ${columns.length} columns.`}
          </caption>
          <thead>
            <tr>
              <th scope="col" className="mono" aria-label="Row number">
                #
              </th>
              {columns.map((column) => {
                if (!sortable) {
                  return (
                    <th key={column} scope="col">
                      {column}
                    </th>
                  );
                }
                return (
                  <th
                    key={column}
                    scope="col"
                    aria-sort={
                      sortColumn === column
                        ? sortDirection === "asc"
                          ? "ascending"
                          : "descending"
                        : "none"
                    }
                  >
                    <button
                      type="button"
                      className="table-sort-button"
                      onClick={() => toggleSort(column)}
                      title={`Sort by ${column}`}
                    >
                      <span>{column}</span>
                      <span className="table-sort-arrow" aria-hidden="true">
                        {sortColumn === column ? (sortDirection === "asc" ? "▲" : "▼") : "▴"}
                      </span>
                    </button>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {displayRows.map(({ row, absoluteRowIndex }) => {
              const isHighlightedRow = highlightedRows.has(absoluteRowIndex);

              return (
                <tr
                  key={absoluteRowIndex}
                  data-row-index={absoluteRowIndex}
                  className={isHighlightedRow ? "table-row-highlighted" : undefined}
                >
                  <th
                    scope="row"
                    className={`mono ${isHighlightedRow ? "hl" : ""}`}
                  >
                    {absoluteRowIndex}
                  </th>
                  {columns.map((column) => {
                    const isHighlightedCell =
                      isHighlightedRow && highlightedCols.has(column);
                    const rawValue = row[column];
                    return (
                      <td
                        key={`${absoluteRowIndex}:${column}`}
                        className={isHighlightedCell ? "hl" : ""}
                        onContextMenu={(event) =>
                          onCellContextMenu?.(event, {
                            column,
                            value: rawValue,
                            rowIndex: absoluteRowIndex,
                          })
                        }
                      >
                        {formatCellValue
                          ? formatCellValue(column, rawValue)
                          : defaultFormatValue(rawValue)}
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
