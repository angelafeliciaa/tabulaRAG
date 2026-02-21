import json
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from app.db import SessionLocal
from app.embeddings import embed_texts
from app.qdrant_client import search_vectors


def _deserialize_row_data(raw: object) -> Dict[str, Any]:
    """Deserialize row_data, handling potential double-serialization.

    The ingestion code stores row_data via json.dumps() into a SQLAlchemy JSON
    column, which may cause double-serialization depending on the DB backend.
    """
    if isinstance(raw, dict):
        return raw
    data = json.loads(raw) if isinstance(raw, str) else raw
    if isinstance(data, str):
        data = json.loads(data)
    return data


def semantic_search(
    dataset_id: int, question: str, top_k: int = 10
) -> List[Dict[str, Any]]:
    """Embed the question and search Qdrant for similar rows."""
    query_vector = embed_texts([question])[0]
    hits = search_vectors(dataset_id, query_vector, limit=top_k)

    results = []
    for hit in hits:
        row_data = hit["payload"].get("row_data", {})
        row_index = hit["id"]
        score = hit["score"]
        highlights = generate_highlights(dataset_id, row_index, row_data, question)
        results.append({
            "row_index": row_index,
            "score": score,
            "row_data": row_data,
            "highlights": highlights,
            "match_type": "semantic",
        })
    return results


def exact_search(
    dataset_id: int, filters: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Exact match on row_data fields. Fetches rows and filters in Python
    to handle double-serialized JSON and work across DB backends."""
    if not filters:
        return []

    with SessionLocal() as db:
        result = db.execute(
            text(
                "SELECT row_index, row_data FROM dataset_rows "
                "WHERE dataset_id = :dataset_id ORDER BY row_index"
            ),
            {"dataset_id": dataset_id},
        )
        rows = result.fetchall()

    results = []
    for row in rows:
        row_index = row[0]
        row_data = _deserialize_row_data(row[1])
        if all(row_data.get(col) == val for col, val in filters.items()):
            results.append({
                "row_index": row_index,
                "score": 1.0,
                "row_data": row_data,
                "highlights": [],
                "match_type": "exact",
            })
    return results


def hybrid_search(
    dataset_id: int,
    question: str,
    filters: Optional[Dict[str, str]] = None,
    top_k: int = 10,
) -> List[Dict[str, Any]]:
    """Combine exact matches (score 1.0) with semantic results, deduplicated."""
    seen_indices = set()
    results = []

    # Exact matches first
    if filters:
        for r in exact_search(dataset_id, filters):
            if r["row_index"] not in seen_indices:
                seen_indices.add(r["row_index"])
                results.append(r)

    # Then semantic results
    for r in semantic_search(dataset_id, question, top_k=top_k):
        if r["row_index"] not in seen_indices:
            seen_indices.add(r["row_index"])
            results.append(r)

    return results


def generate_highlights(
    dataset_id: int,
    row_index: int,
    row_data: Dict[str, Any],
    question: str,
) -> List[Dict[str, Any]]:
    """Generate cell-level citation highlights based on keyword overlap."""
    question_tokens = set(question.lower().split())
    highlights = []

    for col, val in row_data.items():
        if val is None or val == "":
            continue
        val_str = str(val).lower()
        val_tokens = set(val_str.split())
        overlap = question_tokens & val_tokens
        if overlap:
            highlight_id = f"d{dataset_id}_r{row_index}_{col}"
            highlights.append({
                "highlight_id": highlight_id,
                "column": col,
                "value": str(val),
                "relevance": len(overlap) / max(len(question_tokens), 1),
            })

    highlights.sort(key=lambda h: h["relevance"], reverse=True)
    return highlights


def get_highlight(highlight_id: str) -> Optional[Dict[str, Any]]:
    """Parse a highlight ID and look up the cell value from PG.

    Highlight ID format: d{dataset_id}_r{row_index}_{column}
    Uses split("_", 2) on the remainder after "d" to handle underscores in column names.
    """
    if not highlight_id.startswith("d"):
        return None

    try:
        rest = highlight_id[1:]  # strip leading "d"
        parts = rest.split("_", 2)  # ["<dataset_id>", "r<row_index>", "<column>"]
        if len(parts) != 3:
            return None
        dataset_id = int(parts[0])
        row_index = int(parts[1][1:])  # strip leading "r"
        column = parts[2]
    except (ValueError, IndexError):
        return None

    with SessionLocal() as db:
        result = db.execute(
            text(
                "SELECT row_data FROM dataset_rows "
                "WHERE dataset_id = :dataset_id AND row_index = :row_index"
            ),
            {"dataset_id": dataset_id, "row_index": row_index},
        )
        row = result.fetchone()

    if row is None:
        return None

    row_data = _deserialize_row_data(row[0])

    if column not in row_data:
        return None

    return {
        "highlight_id": highlight_id,
        "dataset_id": dataset_id,
        "row_index": row_index,
        "column": column,
        "value": row_data[column],
        "row_context": row_data,
    }
