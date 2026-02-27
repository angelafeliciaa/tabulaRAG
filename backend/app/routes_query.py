from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.retrieval import get_highlight, resolve_dataset_context, smart_query

router = APIRouter()


# ── Request / Response models ──────────────────────────────────────

class QueryRequest(BaseModel):
    question: str = Field(description="Natural language question to search the dataset with")
    dataset_id: Optional[int] = Field(
        default=None,
        description="Optional dataset ID. If omitted (or invalid), the backend resolves from dataset_name/question.",
    )
    dataset_name: Optional[str] = Field(
        default=None,
        description="Optional dataset name (for example 'Chocolate'). Helps automatic dataset resolution.",
    )
    top_k: int = Field(default=10, ge=1, le=100)
    filters: Optional[Dict[str, str]] = None


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


class QueryResponse(BaseModel):
    dataset_id: int
    question: str
    results: List[ResultItem]
    answer: Optional[str] = None
    answer_type: Optional[str] = None
    answer_details: Optional[Dict[str, Any]] = None
    dataset_url: Optional[str] = None
    final_response: Optional[str] = None
    resolved_dataset: Optional[Dict[str, Any]] = None
    resolution_note: Optional[str] = None

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
    description="Primary analytics endpoint. Use this instead of row-slice tools for sums/counts/top-N and precise citations.",
)
def query_dataset(body: QueryRequest):
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
    payload["resolved_dataset"] = resolved_dataset
    if resolution_note:
        payload["resolution_note"] = resolution_note
    return QueryResponse(**payload)


@router.get("/highlights/{highlight_id}", response_model=HighlightResponse)
def get_highlight_endpoint(highlight_id: str):
    result = get_highlight(highlight_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Highlight not found.")
    return result
