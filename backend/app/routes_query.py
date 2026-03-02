from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.db import SessionLocal
from app.retrieval import get_highlight, hybrid_search

router = APIRouter()


# ── Request / Response models ──────────────────────────────────────


class QueryRequest(BaseModel):
    question: str = Field(
        description="Natural language question to search the dataset with"
    )
    dataset_id: int = Field(
        description="ID of the dataset to query. Call GET /tables first to discover valid IDs."
    )
    top_k: int
    filters: Optional[Dict[str, str]] = None


class AggregateRequest(BaseModel):
    question: str = Field(
        description="Natural language question to aggregate the dataset with"
    )
    dataset_id: int = Field(
        description="ID of the dataset to aggregate. Call GET /tables first to discover valid IDs."
    )
    top_k: int
    filters: Optional[Dict[str, str]] = None
    operation: Literal["count", "sum", "avg", "min", "max"]


class AggregateResponse(BaseModel):
    dataset_id: int
    operation: str
    metric_column: Optional[str]
    group_by_column: Optional[str]
    question: str
    rows: List[Dict[str, Any]]
    url: Optional[str] = Field(
        default=None,
        description=(
            "Canonical citation URL for this answer. Points to the highlighted cell "
            "when available, otherwise the table view. Return this to users as the source link."
        ),
    )


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


class HighlightResponse(BaseModel):
    highlight_id: str
    dataset_id: int
    row_index: int
    column: str
    value: Any
    row_context: Dict[str, Any]


# ── Endpoints ──────────────────────────────────────────────────────


@router.post(
    "/query",
    response_model=QueryResponse,
    summary="Answer natural-language table queries",
    description="Primary analytics endpoint. Use this instead of row-slice tools for sums/counts/top-N and precise citations. Always return URL from response",
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
    description="Use this for aggregate queries. Always return URL from response",
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
