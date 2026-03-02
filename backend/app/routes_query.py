from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.db import SessionLocal
from app.retrieval import get_highlight, hybrid_search
from app.routes_tables import get_cols_for_dataset

router = APIRouter()

FilterOperator = Literal["=", "!=", ">", ">=", "<", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"]


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


import base64
import json
import os



# ── Request / Response models ──────────────────────────────────────


class FilterCondition(BaseModel):
    column: str
    operator: FilterOperator
    value: Optional[str] = None  # None for IS NULL / IS NOT NULL


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


class QueryRequest(BaseModel):
    question: str = Field(
        description="Natural language question to search the dataset with"
    )
    dataset_id: int = Field(
        description="ID of the dataset to query. Call GET /tables first to discover valid IDs."
    )
    top_k: int
    filters: Optional[Dict[str, str]] = None


class QueryResponse(BaseModel):
    dataset_id: int
    question: str
    results: List[ResultItem]
    url: Optional[str] = Field(
        default=None,
        description=(
            "Canonical citation URL for this answer. Points to the highlighted cell "
            "when available, otherwise the table view. Return this to users as the source link."
        ),
    )


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


class HighlightResponse(BaseModel):
    highlight_id: str
    dataset_id: int
    row_index: int
    column: str
    value: Any
    row_context: Dict[str, Any]

PUBLIC_UI_BASE_URL = os.getenv("PUBLIC_UI_BASE_URL", "http://localhost:5173")
def build_virtual_table_url(body: AggregateRequest, highlight_index: int = 0) -> str:
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

# ── Endpoints ──────────────────────────────────────────────────────


@router.post(
    "/query",
    response_model=QueryResponse,
    summary="Answer natural-language row queries",
    description="Use this instead of row-slice tools for row query and precise citations. Always include URL from response when answering the user",
)
def query_dataset(body: QueryRequest):
    # Verify dataset exists
    with SessionLocal() as db:
        row = db.execute(
            text("SELECT id FROM datasets WHERE id = :id"),
            {"id": body.dataset_id},
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")

    results = hybrid_search(
        dataset_id=body.dataset_id,
        question=body.question,
        filters=body.filters,
        top_k=10,
    )

    return QueryResponse(
        dataset_id=body.dataset_id,
        question=body.question,
        results=results,
    )


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

    def col_expr(param_name: str) -> str:
        return f"(row_data::jsonb ->> :{param_name})"

    if body.operation == "count":
        metric_sql = "COUNT(*)::bigint AS aggregate_value"
    else:
        params["metric_column"] = body.metric_column
        numeric_from_num_json = f"NULLIF(TRIM((row_data_num::jsonb ->> :metric_column)), '')::double precision"
        numeric_from_text_json = (
            "NULLIF("
            f"REGEXP_REPLACE(TRIM({col_expr('metric_column')}), '[^0-9.\\-]', '', 'g')"
            ", '')::double precision"
        )
        numeric_expr = f"COALESCE({numeric_from_num_json}, {numeric_from_text_json})"
        metric_sql = f"{body.operation.upper()}({numeric_expr})::double precision AS aggregate_value"

    select_parts = []
    group_by_sql = ""
    order_by_sql = ""
    if body.group_by:
        params["group_by"] = body.group_by
        group_expr = col_expr("group_by")
        select_parts.append(f"{group_expr} AS group_value")
        group_by_sql = f" GROUP BY {group_expr}"
        order_by_sql = " ORDER BY aggregate_value DESC NULLS LAST"
    else:
        order_by_sql = ""

    select_parts.append(metric_sql)

    where_clauses = ["dataset_id = :dataset_id"]
    if body.filters:
        for i, f in enumerate(body.filters):
            if f.column not in valid_columns:
                raise HTTPException(400, detail=f"Invalid filter column: {f.column}")
    
            kp = f"fcol_{i}"
            vp = f"fval_{i}"
            params[kp] = f.column
            col = col_expr(kp)

            if f.operator in ("IS NULL", "IS NOT NULL"):
                where_clauses.append(f"{col} {f.operator}")
            elif f.operator == "IN":
                # expect value to be comma-separated
                values = [v.strip() for v in f.value.split(",")]
                in_params = {f"fval_{i}_{j}": v for j, v in enumerate(values)}
                params.update(in_params)
                placeholders = ", ".join(f":{k}" for k in in_params)
                where_clauses.append(f"{col} IN ({placeholders})")
            elif f.operator == "LIKE":
                params[vp] = f.value
                where_clauses.append(f"{col} LIKE :{vp}")
            elif f.operator in (">", ">=", "<", "<="):
                params[vp] = f.value
                # cast to numeric for amount-style columns
                where_clauses.append(
                    f"NULLIF(TRIM({col}), '')::double precision {f.operator} :{vp}::double precision"
                )
            else:  # "=" and "!="
                params[vp] = f.value
                where_clauses.append(f"{col} {f.operator} :{vp}")

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

    url = build_virtual_table_url(body, highlight_index=0) if rows else None

    return AggregateResponse(
        dataset_id=body.dataset_id,
        metric_column=body.metric_column,
        group_by_column=body.group_by,
        rowsResult=rows,
        sql_query=_render_sql(sql, params),
        url=url,
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
