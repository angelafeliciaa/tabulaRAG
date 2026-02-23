import json
import re
from typing import Any, Dict, List, Optional, Set

from qdrant_client import models
from sqlalchemy import text

from app.db import SessionLocal
from app.embeddings import embed_texts
from app.qdrant_client import search_vectors


# Common English stop words to exclude from keyword filters
_STOP_WORDS: Set[str] = {
    "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "need", "dare", "ought",
    "used", "to", "of", "in", "for", "on", "with", "at", "by", "from",
    "as", "into", "through", "during", "before", "after", "above", "below",
    "between", "out", "off", "over", "under", "again", "further", "then",
    "once", "here", "there", "when", "where", "why", "how", "all", "each",
    "every", "both", "few", "more", "most", "other", "some", "such", "no",
    "nor", "not", "only", "own", "same", "so", "than", "too", "very",
    "just", "because", "but", "and", "or", "if", "while", "about",
    "what", "which", "who", "whom", "this", "that", "these", "those",
    "am", "it", "its", "i", "me", "my", "myself", "we", "our", "ours",
    "you", "your", "yours", "he", "him", "his", "she", "her", "hers",
    "they", "them", "their", "theirs", "tell", "show", "find", "get",
    "give", "know", "think", "say", "make", "go", "see", "come", "take",
    "want", "look", "use", "many", "much",
}


def extract_keywords(question: str) -> List[str]:
    """Extract meaningful keywords from a question by stripping stop words and punctuation."""
    # Remove punctuation, keep only alphanumeric and spaces
    cleaned = re.sub(r"[^\w\s]", " ", question.lower())
    tokens = cleaned.split()
    # Filter out stop words and very short tokens
    keywords = [t for t in tokens if t not in _STOP_WORDS and len(t) >= 2]
    return keywords


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


def _build_keyword_filter(keywords: List[str]) -> Optional[models.Filter]:
    """Build a Qdrant filter that matches rows containing any of the keywords."""
    if not keywords:
        return None
    conditions = [
        models.FieldCondition(
            key="text",
            match=models.MatchText(text=kw),
        )
        for kw in keywords
    ]
    return models.Filter(should=conditions)


def _hits_to_results(
    hits: List[Dict],
    dataset_id: int,
    question: str,
    match_type: str,
) -> List[Dict[str, Any]]:
    """Convert raw search hits into result dicts."""
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
            "match_type": match_type,
        })
    return results


def semantic_search(
    dataset_id: int, question: str, top_k: int = 10
) -> List[Dict[str, Any]]:
    """Two-pass semantic search: keyword-filtered first, then unfiltered fallback."""
    query_vector = embed_texts([question])[0]
    keywords = extract_keywords(question)
    keyword_filter = _build_keyword_filter(keywords)

    seen_ids: set = set()
    results: List[Dict[str, Any]] = []

    # Pass 1: filtered search (only if we have keywords)
    if keyword_filter is not None:
        try:
            filtered_hits = search_vectors(
                dataset_id, query_vector, limit=top_k, query_filter=keyword_filter
            )
        except Exception:
            filtered_hits = []
        for r in _hits_to_results(filtered_hits, dataset_id, question, "semantic"):
            seen_ids.add(r["row_index"])
            results.append(r)

    # Pass 2: unfiltered fallback to fill remaining slots
    remaining = top_k - len(results)
    if remaining > 0:
        fallback_hits = search_vectors(dataset_id, query_vector, limit=top_k)
        for r in _hits_to_results(fallback_hits, dataset_id, question, "semantic"):
            if r["row_index"] not in seen_ids:
                seen_ids.add(r["row_index"])
                results.append(r)
                if len(results) >= top_k:
                    break

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
    """Generate cell-level citation highlights with keyword-aware weighting.

    Keyword matches (non-stop-word) are weighted 2x, stop-word matches 0.5x.
    """
    question_tokens = set(question.lower().split())
    keywords = set(extract_keywords(question))
    highlights = []

    for col, val in row_data.items():
        if val is None or val == "":
            continue
        val_str = str(val).lower()
        val_tokens = set(val_str.split())
        overlap = question_tokens & val_tokens
        if overlap:
            # Weight keyword matches 2x, stop-word matches 0.5x
            score = sum(
                2.0 if tok in keywords else 0.5
                for tok in overlap
            )
            max_possible = max(len(question_tokens), 1)
            highlight_id = f"d{dataset_id}_r{row_index}_{col}"
            highlights.append({
                "highlight_id": highlight_id,
                "column": col,
                "value": str(val),
                "relevance": score / max_possible,
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
