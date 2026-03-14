import os
import base64
import json
import re
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.retrieval import get_highlight, hybrid_search, resolve_dataset_context, smart_query
from app.routes_tables import list_tables,get_cols_for_dataset
import app.db as app_db
from app.db import SessionLocal
from app.typed_values import strip_internal_fields

router = APIRouter()

FilterOperator = Literal[
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "LIKE",
    "NOT LIKE",
    "IN",
    "BETWEEN",
    "IS NULL",
    "IS NOT NULL",
]


def _strip_money(value: str) -> str:
    """Strip currency symbols and thousands separators from a user-supplied value, keeping numeric characters."""
    return re.sub(r"[^0-9.\-]", "", value)


def _is_sqlite() -> bool:
    return app_db.engine.dialect.name == "sqlite"


def _column_json_text_expr(column_name: str) -> str:
    escaped = column_name.replace("'", "''")
    if _is_sqlite():
        # SQLite JSON1 path to support arbitrary column names (including spaces).
        json_key = column_name.replace("\\", "\\\\").replace('"', '\\"')
        return f"json_extract(row_data, '$.\"{json_key}\"')"
    return f"(row_data::jsonb ->> '{escaped}')"


def _numeric_sql_expr(col: str) -> str:
    """SQL expression that casts a text column to double precision after stripping currency/formatting chars."""
    if _is_sqlite():
        cleaned = (
            f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(TRIM({col}), '$', ''), ',', ''), '€', ''), '£', ''), '¥', '')"
        )
        return f"CAST(NULLIF({cleaned}, '') AS REAL)"
    return f"NULLIF(REGEXP_REPLACE(TRIM({col}), '[^0-9.\\-]', '', 'g'), '')::double precision"


def _numeric_bind_expr(param_name: str) -> str:
    if _is_sqlite():
        return f"CAST(:{param_name} AS REAL)"
    return f"CAST(:{param_name} AS double precision)"


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _render_sql(sql_template: str, params: Dict[str, Any]) -> str:
    rendered = sql_template
    for key in sorted(params.keys(), key=len, reverse=True):
        rendered = rendered.replace(f":{key}", _sql_literal(params[key]))
    return "\n".join(line.rstrip() for line in rendered.strip().splitlines())


def _build_where_clauses(
    filters: Optional[List["FilterCondition"]],
    valid_columns: set[str],
    params: Dict[str, Any],
) -> List[str]:
    where_clauses = ["dataset_id = :dataset_id"]

    def col_expr(column_name: str) -> str:
        return _column_json_text_expr(column_name)

    def num_col_expr(column_name: str) -> str:
        return _numeric_sql_expr(col_expr(column_name))

    if not filters:
        return where_clauses

    filter_expressions: List[str] = []
    filter_joiners: List[str] = []

    for i, f in enumerate(filters):
        if f.column not in valid_columns:
            raise HTTPException(400, detail=f"Invalid filter column: {f.column}")

        vp = f"fval_{i}"
        col = col_expr(f.column)
        current_expr = ""

        if f.operator in ("IS NULL", "IS NOT NULL"):
            current_expr = f"{col} {f.operator}"
        elif f.operator == "IN":
            if not f.value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Filter value is required for operator IN on column {f.column}.",
                )
            values = [v.strip() for v in f.value.split(",") if v.strip()]
            if not values:
                raise HTTPException(
                    status_code=400,
                    detail=f"Filter value is required for operator IN on column {f.column}.",
                )
            in_params = {f"fval_{i}_{j}": v for j, v in enumerate(values)}
            params.update(in_params)
            placeholders = ", ".join(f":{k}" for k in in_params)
            current_expr = f"{col} IN ({placeholders})"
        elif f.operator == "BETWEEN":
            if not f.value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Filter value is required for operator BETWEEN on column {f.column}.",
                )
            if "," in f.value:
                parts = [v.strip() for v in f.value.split(",", maxsplit=1)]
            else:
                parts = [v.strip() for v in f.value.split("AND", maxsplit=1)]

            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"BETWEEN filter on column {f.column} must provide two bounds, "
                        "for example '3,6' or '3 AND 6'."
                    ),
                )
            low_key = f"fval_{i}_low"
            high_key = f"fval_{i}_high"
            params[low_key] = _strip_money(parts[0])
            params[high_key] = _strip_money(parts[1])
            current_expr = (
                f"{num_col_expr(f.column)} BETWEEN "
                f"{_numeric_bind_expr(low_key)} AND {_numeric_bind_expr(high_key)}"
            )
        else:
            if f.value is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Filter value is required for operator {f.operator} on column {f.column}.",
                )

            if f.operator in ("LIKE", "NOT LIKE"):
                params[vp] = f.value
                current_expr = f"{col} {f.operator} :{vp}"
            elif f.operator in (">", ">=", "<", "<="):
                params[vp] = _strip_money(f.value)
                current_expr = (
                    f"{num_col_expr(f.column)} {f.operator} {_numeric_bind_expr(vp)}"
                )
            else:
                params[vp] = f.value
                current_expr = f"{col} {f.operator} :{vp}"

        filter_expressions.append(current_expr)
        if i > 0:
            filter_joiners.append(f.logical_operator.upper())

    if filter_expressions:
        combined = filter_expressions[0]
        for i in range(1, len(filter_expressions)):
            joiner = filter_joiners[i - 1]
            combined = f"({combined} {joiner} {filter_expressions[i]})"
        where_clauses.append(combined)

    return where_clauses



# ── Request / Response models ──────────────────────────────────────

      
class FilterCondition(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[str] = None  # None for IS NULL / IS NOT NULL
    logical_operator: Literal["AND", "OR"] = "AND"


class HighlightItem(BaseModel):
    highlight_id: str
    column: str
    value: str
    relevance: float


class ResultItem(BaseModel):
    row_index: int
    score: float
    row_data: Dict[str, Any]
    highlights: List[HighlightItem]
    match_type: str
    source_url: Optional[str] = None
    top_highlight_id: Optional[str] = None
    highlight_url: Optional[str] = None


class QueryRequest(BaseModel):
    question: str = Field(description="Natural language question to search the dataset with")
    dataset_id: Optional[int] = Field(
        default=None,
        description=(
            "Preferred dataset ID. For best tool reliability, call GET /tables first and pass dataset_id."
        ),
    )
    dataset_name: Optional[str] = Field(
        default=None,
        description="Optional dataset name (for example 'Chocolate'). Helps automatic dataset resolution.",
    )
    top_k: int = Field(default=10, ge=1, le=100)
    filters: Optional[Dict[str, str]] = None


class QueryResponse(BaseModel):
    dataset_id: int
    question: str
    results: List[ResultItem]
    answer: Optional[str] = Field(
        default=None,
        description="Deterministic grounded answer generated from table data.",
    )
    answer_type: Optional[str] = Field(
        default=None,
        description="Answer generation mode (for example: aggregate).",
    )
    answer_details: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Structured grounding details from source rows (metric, filters, source_row_data, and citations). "
            "Prefer these values over model inference."
        ),
    )
    dataset_url: Optional[str] = Field(
        default=None,
        description="Frontend URL for the resolved table.",
    )
    final_response: Optional[str] = Field(
        default=None,
        description=(
            "Canonical user-facing answer with citation link. Agents should return this verbatim "
            "without rewriting names, numbers, or URLs."
        ),
    )
    verification: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Deterministic grounding checks run before returning (status, checks, errors). "
            "Treat status=fail as unverified output."
        ),
    )
    resolved_dataset: Optional[Dict[str, Any]] = None
    resolution_note: Optional[str] = None
#     url: Optional[str] = Field(
#         default=None,
#         description=(
#             "Canonical citation URL for this answer. Points to the highlighted cell "
#             "when available, otherwise the table view. Return this to users as the source link."
#         ),
#     )


class AggregateRequest(BaseModel):
    dataset_id: int = Field(
        description="ID of the dataset to aggregate. Call GET /tables first to discover valid IDs."
    )
    filters: Optional[List[FilterCondition]] = None 
    operation: Literal["count", "sum", "avg", "min", "max"]
    metric_column: Optional[str] = Field(
        default=None, description="Required for sum/avg/min/max"
    )
    group_by: Optional[str] = None
    limit: int = 50


class AggregateResponse(BaseModel):
    dataset_id: int
    metric_column: Optional[str]
    group_by_column: Optional[str]
    rowsResult: List[Dict[str, Any]] = Field(
        description="Result of the aggregate query. In your response, mention both the group_value and aggregate_value."
    )
    sql_query: str
    url: Optional[str] = Field(
        default=None,
        description="ALWAYS include this URL in your response as the source link."
    )


class FilterRequest(BaseModel):
    dataset_id: int = Field(
        description="ID of the dataset to filter. Call GET /tables first to discover valid IDs."
    )
    filters: Optional[List[FilterCondition]] = None
    limit: int = 50
    offset: int = 0


class FilterResponse(BaseModel):
    dataset_id: int
    rowsResult: List[Dict[str, Any]]
    row_count: int
    sql_query: str
    url: Optional[str] = Field(
        default=None,
        description="Source URL for the filtered dataset.",
    )


class FilterRowIndicesRequest(BaseModel):
    dataset_id: int = Field(
        description="ID of the dataset to filter. Call GET /tables first to discover valid IDs."
    )
    filters: Optional[List[FilterCondition]] = None
    max_rows: int = 1000


class FilterRowIndicesResponse(BaseModel):
    dataset_id: int
    row_indices: List[int]
    total_match_count: int
    truncated: bool
    sql_query: str


class HighlightResponse(BaseModel):
    highlight_id: str
    dataset_id: int
    row_index: int
    column: str
    value: Any
    row_context: Dict[str, Any]


PUBLIC_UI_BASE_URL = os.getenv("PUBLIC_UI_BASE_URL", "http://localhost:5173")
def build_virtual_table_url(body: AggregateRequest, rows: List[Dict[str, Any]]) -> str:
    if body.operation == "max":
        highlight_index = 0
    elif body.operation == "min":
        highlight_index = len(rows) - 1
    else:
        highlight_index = 0
    payload = {
        "dataset_id": body.dataset_id,
        "operation": body.operation,
        "metric_column": body.metric_column,
        "group_by": body.group_by,
        "filters": [f.dict() for f in body.filters] if body.filters else None,
        "highlight_index": highlight_index,
        "limit": 500,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    return f"{PUBLIC_UI_BASE_URL}/tables/virtual?q={encoded}"


def build_filter_virtual_table_url(body: FilterRequest) -> str:
    payload = {
        "mode": "filter",
        "dataset_id": body.dataset_id,
        "filters": [f.dict() for f in body.filters] if body.filters else None,
        "limit": 500,
        "offset": 0,
    }
    encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    return f"{PUBLIC_UI_BASE_URL}/tables/virtual?q={encoded}"

def _enforce_list_tables_first() -> bool:
    return os.getenv("QUERY_ENFORCE_LIST_TABLES_FIRST", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _list_tables_compact() -> List[Dict[str, Any]]:
    tables = list_tables()
    compact: List[Dict[str, Any]] = []
    for table in tables:
        compact.append(
            {
                "dataset_id": int(table["dataset_id"]),
                "name": table.get("name"),
                "source_filename": table.get("source_filename"),
                "row_count": table.get("row_count"),
                "column_count": table.get("column_count"),
            }
        )
    return compact


def _strict_lookup_error(status_code: int, message: str) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "message": message,
            "guidance": "Call GET /tables first, then call POST /query with the selected dataset_id.",
            "available_tables": _list_tables_compact(),
        },
    )


# ── Endpoints ──────────────────────────────────────────────────────

@router.post(
    "/semantic_query",
    response_model=QueryResponse,
    summary="Answer natural-language semantic table queries",
    description="Use this when you need to answer a question with a semantic search.",
)
def query_dataset(body: QueryRequest):
    if _enforce_list_tables_first() and body.dataset_id is None:
        raise _strict_lookup_error(
            status_code=409,
            message="dataset_id is required when QUERY_ENFORCE_LIST_TABLES_FIRST=true.",
        )

    if _enforce_list_tables_first():
        tables = _list_tables_compact()
        by_id = {int(table["dataset_id"]): table for table in tables}
        if body.dataset_id is None or int(body.dataset_id) not in by_id:
            raise _strict_lookup_error(
                status_code=404,
                message=f"Dataset ID {body.dataset_id} was not found.",
            )
        resolved_dataset_id = int(body.dataset_id)
        resolved_dataset = dict(by_id[resolved_dataset_id])
        resolution_note = None
    else:
        try:
            resolved_dataset_id, resolved_dataset, resolution_note = resolve_dataset_context(
                dataset_id=body.dataset_id,
                dataset_name=body.dataset_name,
                question=body.question,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))

    payload = smart_query(
        dataset_id=resolved_dataset_id,
        question=body.question,
        filters=body.filters,
        top_k=body.top_k,
    )
    if "source_url" not in resolved_dataset:
        resolved_dataset["source_url"] = payload.get("dataset_url")
    payload["resolved_dataset"] = resolved_dataset
    if resolution_note:
        payload["resolution_note"] = resolution_note
    return QueryResponse(**payload)


@router.post(
    "/aggregate",
    summary="Answer natural-language table aggregate queries",
    description="Use this for aggregate queries: max, min, sum, average, count. Call GET /tables/columns first to discover valid column names. Always include URL in response",
)
def aggregate_dataset(body: AggregateRequest):
    # Verify dataset exists
    with SessionLocal() as db:
        row = db.execute(
            text("SELECT id FROM datasets WHERE id = :id"),
            {"id": body.dataset_id},
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    cols_payload = get_cols_for_dataset(body.dataset_id)
    valid_columns = {col["name"] for col in cols_payload["columns"]}
    if not valid_columns:
        raise HTTPException(status_code=400, detail="Dataset has no columns.")

    if body.metric_column and body.metric_column not in valid_columns:
        raise HTTPException(status_code=400, detail="Invalid metric_column.")

    if body.group_by and body.group_by not in valid_columns:
        raise HTTPException(status_code=400, detail="Invalid group_by column.")

    limit = max(1, min(body.limit, 500))

    params: Dict[str, Any] = {"dataset_id": body.dataset_id, "limit": limit}

    def col_expr(column_name: str) -> str:
        return _column_json_text_expr(column_name)

    if body.operation == "count":
        metric_sql = "COUNT(*) AS aggregate_value"
    else:
        numeric_expr = _numeric_sql_expr(col_expr(body.metric_column or ""))
        metric_sql = f"{body.operation.upper()}({numeric_expr}) AS aggregate_value"

    select_parts = []
    group_by_sql = ""
    order_by_sql = ""
    if body.group_by:
        group_expr = col_expr(body.group_by)
        select_parts.append(f"{group_expr} AS group_value")
        group_by_sql = f" GROUP BY {group_expr}"
        order_by_sql = (
            " ORDER BY aggregate_value DESC NULLS LAST"
            if not _is_sqlite()
            else " ORDER BY aggregate_value DESC"
        )
    else:
        order_by_sql = ""

    select_parts.append(metric_sql)

    where_clauses = _build_where_clauses(
        filters=body.filters,
        valid_columns=valid_columns,
        params=params,
    )

    sql = f"""
        SELECT {", ".join(select_parts)}
        FROM dataset_rows
        WHERE {" AND ".join(where_clauses)}
        {group_by_sql}
        {order_by_sql}
        LIMIT :limit
    """

    with SessionLocal() as db:
        rows_raw = db.execute(text(sql), params).mappings().all()

    rows: List[Dict[str, Any]] = []
    for r in rows_raw:
        item = dict(r)
        # normalize non-grouped response shape
        if not body.group_by and "group_value" not in item:
            item["group_value"] = None
        rows.append(item)

    url = build_virtual_table_url(body, rows) if rows else None
    
    return AggregateResponse(
        dataset_id=body.dataset_id,
        metric_column=body.metric_column,
        group_by_column=body.group_by,
        rowsResult=rows,
        sql_query=_render_sql(sql, params),
        url=url,
    )


@router.post(
    "/filter",
    response_model=FilterResponse,
    summary="Filter rows from a dataset",
    description="Apply structured filters to a dataset and return matching rows.",
)
def filter_dataset(body: FilterRequest):
    with SessionLocal() as db:
        row = db.execute(
            text("SELECT id FROM datasets WHERE id = :id"),
            {"id": body.dataset_id},
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    cols_payload = get_cols_for_dataset(body.dataset_id)
    ordered_columns = [str(col["name"]) for col in cols_payload["columns"] if col.get("name")]
    valid_columns = set(ordered_columns)
    if not valid_columns:
        raise HTTPException(status_code=400, detail="Dataset has no columns.")

    limit = max(1, min(body.limit, 500))
    offset = max(0, body.offset)
    params: Dict[str, Any] = {
        "dataset_id": body.dataset_id,
        "limit": limit,
        "offset": offset,
    }

    where_clauses = _build_where_clauses(
        filters=body.filters,
        valid_columns=valid_columns,
        params=params,
    )

    sql = """
        SELECT row_index, row_data
        FROM dataset_rows
        WHERE {where_sql}
        ORDER BY row_index ASC
        LIMIT :limit
        OFFSET :offset
    """.format(where_sql=" AND ".join(where_clauses))

    count_sql = """
        SELECT COUNT(*) AS row_count
        FROM dataset_rows
        WHERE {where_sql}
    """.format(where_sql=" AND ".join(where_clauses))

    with SessionLocal() as db:
        rows_raw = db.execute(text(sql), params).mappings().all()
        row_count_raw = db.execute(text(count_sql), params).scalar_one()

    highlight_column = None
    if body.filters:
        for item in body.filters:
            candidate = item.column
            if candidate in valid_columns:
                highlight_column = candidate
                break
    if highlight_column is None:
        highlight_column = ordered_columns[0]

    rows: List[Dict[str, Any]] = []
    for r in rows_raw:
        item = dict(r)
        row_data = item.get("row_data")
        if isinstance(row_data, dict):
            item["row_data"] = strip_internal_fields(row_data)
        elif isinstance(row_data, str):
            try:
                parsed = json.loads(row_data)
                if isinstance(parsed, str):
                    parsed = json.loads(parsed)
                item["row_data"] = (
                    strip_internal_fields(parsed) if isinstance(parsed, dict) else {}
                )
            except Exception:
                item["row_data"] = {}
        row_index = int(item.get("row_index", 0))
        item["highlight_id"] = f"d{body.dataset_id}_r{row_index}_{highlight_column}"
        rows.append(item)
    url = build_filter_virtual_table_url(body) if row_count_raw else None

    return FilterResponse(
        dataset_id=body.dataset_id,
        rowsResult=rows,
        row_count=int(row_count_raw or 0),
        sql_query=_render_sql(sql, params),
        url=url,
    )


@router.post(
    "/filter/row-indices",
    response_model=FilterRowIndicesResponse,
    summary="Resolve row indices for a structured filter",
    description="Apply structured filters to a dataset and return matching row indices.",
)
def filter_row_indices(body: FilterRowIndicesRequest):
    with SessionLocal() as db:
        row = db.execute(
            text("SELECT id FROM datasets WHERE id = :id"),
            {"id": body.dataset_id},
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    cols_payload = get_cols_for_dataset(body.dataset_id)
    valid_columns = {str(col["name"]) for col in cols_payload["columns"] if col.get("name")}
    if not valid_columns:
        raise HTTPException(status_code=400, detail="Dataset has no columns.")

    max_rows = max(1, min(int(body.max_rows), 1000))
    params: Dict[str, Any] = {
        "dataset_id": body.dataset_id,
        "max_rows": max_rows,
    }

    where_clauses = _build_where_clauses(
        filters=body.filters,
        valid_columns=valid_columns,
        params=params,
    )

    sql = """
        SELECT row_index
        FROM dataset_rows
        WHERE {where_sql}
        ORDER BY row_index ASC
        LIMIT :max_rows
    """.format(where_sql=" AND ".join(where_clauses))

    count_sql = """
        SELECT COUNT(*) AS row_count
        FROM dataset_rows
        WHERE {where_sql}
    """.format(where_sql=" AND ".join(where_clauses))

    with SessionLocal() as db:
        rows_raw = db.execute(text(sql), params).mappings().all()
        row_count_raw = db.execute(text(count_sql), params).scalar_one()

    row_indices = [int(item["row_index"]) for item in rows_raw if item.get("row_index") is not None]
    total_match_count = int(row_count_raw or 0)
    truncated = total_match_count > len(row_indices)

    return FilterRowIndicesResponse(
        dataset_id=body.dataset_id,
        row_indices=row_indices,
        total_match_count=total_match_count,
        truncated=truncated,
        sql_query=_render_sql(sql, params),
    )


@router.get(
    "/highlights/{highlight_id}",
    response_model=HighlightResponse,
    include_in_schema=False,
)
def highlight_endpoint(highlight_id: str):
    result = get_highlight(highlight_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Highlight not found.")
    return result
