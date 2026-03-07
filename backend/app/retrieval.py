import json
import os
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote, urlencode, urlparse, unquote

from qdrant_client import models
from sqlalchemy import text

from app.db import SessionLocal
from app.embeddings import embed_texts
from app.qdrant_client import search_vectors
from app.typed_values import (
    get_numeric_value,
    is_internal_key,
    parse_number as _typed_parse_number,
    strip_internal_fields,
)


# Common English stop words to exclude from keyword filters
_STOP_WORDS: Set[str] = {
    "a",
    "an",
    "the",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "being",
    "have",
    "has",
    "had",
    "do",
    "does",
    "did",
    "will",
    "would",
    "could",
    "should",
    "may",
    "might",
    "shall",
    "can",
    "need",
    "dare",
    "ought",
    "used",
    "to",
    "of",
    "in",
    "for",
    "on",
    "with",
    "at",
    "by",
    "from",
    "as",
    "into",
    "through",
    "during",
    "before",
    "after",
    "above",
    "below",
    "between",
    "out",
    "off",
    "over",
    "under",
    "again",
    "further",
    "then",
    "once",
    "here",
    "there",
    "when",
    "where",
    "why",
    "how",
    "all",
    "each",
    "every",
    "both",
    "few",
    "more",
    "most",
    "other",
    "some",
    "such",
    "no",
    "nor",
    "not",
    "only",
    "own",
    "same",
    "so",
    "than",
    "too",
    "very",
    "just",
    "because",
    "but",
    "and",
    "or",
    "if",
    "while",
    "about",
    "what",
    "which",
    "who",
    "whom",
    "this",
    "that",
    "these",
    "those",
    "am",
    "it",
    "its",
    "i",
    "me",
    "my",
    "myself",
    "we",
    "our",
    "ours",
    "you",
    "your",
    "yours",
    "he",
    "him",
    "his",
    "she",
    "her",
    "hers",
    "they",
    "them",
    "their",
    "theirs",
    "tell",
    "show",
    "find",
    "get",
    "give",
    "know",
    "think",
    "say",
    "make",
    "go",
    "see",
    "come",
    "take",
    "want",
    "look",
    "use",
    "many",
    "much",
}

_SUPERLATIVE_MAX_TOKENS: Set[str] = {"most", "highest", "max", "maximum", "largest", "top"}
_SUPERLATIVE_MIN_TOKENS: Set[str] = {"least", "lowest", "min", "minimum", "smallest", "fewest"}
_AGGREGATE_HINT_TOKENS: Set[str] = {"total", "sum", "average", "avg", "mean", "amount", "number"}
_SUM_TOKENS: Set[str] = {"sum", "total"}
_AVERAGE_TOKENS: Set[str] = {"average", "avg", "mean"}
_COUNT_HINT_TOKENS: Set[str] = {"count", "many", "number"}
_ANALYTIC_TRIGGER_TOKENS: Set[str] = (
    _SUPERLATIVE_MAX_TOKENS
    | _SUPERLATIVE_MIN_TOKENS
    | _SUM_TOKENS
    | _AVERAGE_TOKENS
    | _COUNT_HINT_TOKENS
)
_METRIC_KEYWORD_TOKENS: Set[str] = {
    "amount",
    "sale",
    "total",
    "box",
    "ship",
    "qty",
    "quantity",
    "count",
    "number",
    "price",
    "cost",
    "revenue",
    "volume",
}
_SINGLE_ROW_HINT_RE = re.compile(
    r"\b(one|single)\s+(deal|transaction|row|entry|order|sale|shipment)\b"
)
_SINGLE_DAY_HINT_RE = re.compile(r"\b(?:in|on)\s+(?:a|an|one)\s+day\b")
_DAY_GROUPING_HINT_RE = re.compile(
    r"\b(?:by|per|each)\s+day\b|\b(?:what|which)\s+day\b|\bdate\s+with\b"
)
_EXPLICIT_AGGREGATE_HINT_RE = re.compile(
    r"\b(total|sum|average|avg|mean|combined|overall|all)\b"
)
_QUESTION_ROLE_COLUMN_KEYWORDS: Dict[str, Set[str]] = {
    "person": {
        "sale",
        "sales",
        "salesperson",
        "seller",
        "person",
        "name",
        "rep",
        "agent",
        "employee",
        "staff",
        "owner",
    },
    "product": {"product", "item", "sku", "brand", "flavor"},
    "location": {"country", "region", "market", "city", "state", "location"},
    "date": {"date", "day", "month", "year", "time"},
}
_NUMBER_CLEAN_RE = re.compile(r"[$€£¥₹]")
_DIGIT_ONLY_RE = re.compile(r"^[0-9./\\-]+$")


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


def _public_api_base_url() -> str:
    base = (
        os.getenv("PUBLIC_API_BASE_URL")
        or os.getenv("API_PUBLIC_BASE_URL")
        or os.getenv("BACKEND_PUBLIC_URL")
        or "http://localhost:8000"
    ).strip()
    return base.rstrip("/")


def _verification_enabled() -> bool:
    return os.getenv("QUERY_ENABLE_VERIFICATION", "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _fail_closed_on_verification_error() -> bool:
    return os.getenv("QUERY_FAIL_CLOSED_ON_VERIFY_ERROR", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _public_ui_base_url() -> str:
    base = (
        os.getenv("PUBLIC_UI_BASE_URL")
        or os.getenv("UI_PUBLIC_BASE_URL")
        or os.getenv("FRONTEND_PUBLIC_URL")
        or "http://localhost:5173"
    ).strip()
    return base.rstrip("/")


def _table_ui_url(dataset_id: int) -> str:
    return f"{_public_ui_base_url()}/tables/{dataset_id}"


def _normalize_dataset_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _list_dataset_summaries() -> List[Dict[str, Any]]:
    with SessionLocal() as db:
        rows = db.execute(
            text(
                "SELECT id, name, source_filename, row_count, column_count, created_at "
                "FROM datasets ORDER BY id DESC"
            )
        ).fetchall()

    summaries: List[Dict[str, Any]] = []
    for row in rows:
        dataset_id = int(row[0])
        created_at = row[5]
        summaries.append(
            {
                "dataset_id": dataset_id,
                "name": row[1],
                "source_filename": row[2],
                "row_count": int(row[3] or 0),
                "column_count": int(row[4] or 0),
                "created_at": (
                    created_at.isoformat()
                    if hasattr(created_at, "isoformat")
                    else str(created_at)
                ),
                "source_url": _table_ui_url(dataset_id),
            }
        )
    return summaries


def _score_dataset_match(
    summary: Dict[str, Any],
    dataset_name: str,
    question: str,
) -> float:
    score = 0.0
    normalized_name = _normalize_dataset_name(str(summary.get("name") or ""))
    source_filename = str(summary.get("source_filename") or "")
    normalized_file = _normalize_dataset_name(source_filename.rsplit(".", 1)[0])
    normalized_dataset_name = _normalize_dataset_name(dataset_name)
    normalized_question = _normalize_dataset_name(question)

    if normalized_dataset_name:
        if normalized_dataset_name == normalized_name:
            score += 300.0
        elif normalized_dataset_name == normalized_file:
            score += 280.0
        elif normalized_dataset_name in normalized_name:
            score += 210.0
        elif normalized_dataset_name in normalized_file:
            score += 190.0

    if normalized_question:
        if normalized_name and normalized_name in normalized_question:
            score += 120.0
        if normalized_file and normalized_file in normalized_question:
            score += 100.0

    score += float(summary.get("row_count") or 0) * 0.01
    score += float(summary.get("dataset_id") or 0) * 0.001
    return score


def resolve_dataset_context(
    dataset_id: Optional[int],
    dataset_name: Optional[str],
    question: str,
) -> Tuple[int, Dict[str, Any], Optional[str]]:
    summaries = _list_dataset_summaries()
    if not summaries:
        raise ValueError("No ingested tables found. Upload a CSV/TSV first.")

    by_id = {int(item["dataset_id"]): item for item in summaries}
    if dataset_id is not None:
        wanted_id = int(dataset_id)
        if wanted_id in by_id:
            return wanted_id, by_id[wanted_id], None

    ranked = sorted(
        summaries,
        key=lambda item: _score_dataset_match(item, dataset_name or "", question),
        reverse=True,
    )
    best = ranked[0]
    best_id = int(best["dataset_id"])
    best_score = _score_dataset_match(best, dataset_name or "", question)

    normalized_dataset_name = _normalize_dataset_name(dataset_name or "")
    question_norm = _normalize_dataset_name(question)
    has_hint = bool(normalized_dataset_name)
    if not has_hint:
        for item in summaries:
            name_norm = _normalize_dataset_name(str(item.get("name") or ""))
            file_norm = _normalize_dataset_name(
                str(item.get("source_filename") or "").rsplit(".", 1)[0]
            )
            if (name_norm and name_norm in question_norm) or (
                file_norm and file_norm in question_norm
            ):
                has_hint = True
                break

    if dataset_id is None and len(summaries) > 1 and not has_hint:
        raise ValueError(
            "Multiple datasets are available. Provide dataset_id, or mention the dataset name in the question."
        )

    if dataset_id is not None and not has_hint and best_score < 50.0:
        raise ValueError(
            f"Dataset ID {dataset_id} was not found. Call GET /tables and use a valid dataset_id."
        )

    if dataset_id is not None:
        note = (
            f"Dataset ID {dataset_id} was not found; resolved to dataset_id={best_id} "
            f"({best.get('name')})."
        )
    elif normalized_dataset_name:
        note = (
            f"Resolved dataset_name '{dataset_name}' to dataset_id={best_id} "
            f"({best.get('name')})."
        )
    else:
        note = f"Resolved dataset_id={best_id} ({best.get('name')})."

    return best_id, best, note


def _highlight_source_url(
    highlight_id: str,
    question: Optional[str] = None,
    additional_targets: Optional[List[str]] = None,
) -> str:
    path = f"{_public_ui_base_url()}/highlight/{quote(highlight_id, safe='')}"
    params: Dict[str, str] = {}

    if additional_targets:
        ordered_targets = []
        seen = set([highlight_id])
        for target in additional_targets:
            if not target or target in seen:
                continue
            seen.add(target)
            ordered_targets.append(target)
        if ordered_targets:
            params["targets"] = ",".join(ordered_targets)

    if question and question.strip():
        params["q"] = question.strip()

    if not params:
        return path
    return f"{path}?{urlencode(params)}"


def _fallback_highlight(
    dataset_id: int,
    row_index: int,
    row_data: Dict[str, Any],
    question: str,
) -> Optional[Dict[str, Any]]:
    question_tokens = set(_tokenize(question))
    best_col: Optional[str] = None
    best_score = -1

    for col, val in row_data.items():
        if val is None or val == "":
            continue
        col_tokens = set(_tokenize(col))
        val_tokens = set(_tokenize(str(val)))
        score = len(col_tokens & question_tokens) * 2 + len(val_tokens & question_tokens)
        if score > best_score:
            best_score = score
            best_col = col

    if best_col is None:
        return None

    value = row_data.get(best_col)
    return {
        "highlight_id": f"d{dataset_id}_r{row_index}_{best_col}",
        "column": best_col,
        "value": "" if value is None else str(value),
        "relevance": 1.0,
    }


def _build_result_item(
    dataset_id: int,
    row_index: int,
    row_data: Dict[str, Any],
    score: float,
    question: str,
    match_type: str,
) -> Dict[str, Any]:
    public_row_data = strip_internal_fields(row_data)
    highlights = generate_highlights(dataset_id, row_index, public_row_data, question)
    if not highlights:
        fallback = _fallback_highlight(dataset_id, row_index, public_row_data, question)
        if fallback is not None:
            highlights = [fallback]
    top_highlight_id = highlights[0]["highlight_id"] if highlights else None
    highlight_ui_url = (
        _highlight_source_url(top_highlight_id, question=question)
        if top_highlight_id
        else _table_ui_url(dataset_id)
    )

    result: Dict[str, Any] = {
        "row_index": row_index,
        "score": score,
        "row_data": public_row_data,
        "highlights": highlights,
        "match_type": match_type,
        "source_url": highlight_ui_url,
        "top_highlight_id": top_highlight_id,
        "highlight_url": highlight_ui_url,
    }
    return result


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
        results.append(
            _build_result_item(
                dataset_id=dataset_id,
                row_index=row_index,
                row_data=row_data,
                score=score,
                question=question,
                match_type=match_type,
            )
        )
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
    dataset_id: int,
    filters: Dict[str, str],
    question: str = "",
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
            results.append(
                _build_result_item(
                    dataset_id=dataset_id,
                    row_index=int(row_index),
                    row_data=row_data,
                    score=1.0,
                    question=question,
                    match_type="exact",
                )
            )
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
        for r in exact_search(dataset_id, filters, question=question):
            if r["row_index"] not in seen_indices:
                seen_indices.add(r["row_index"])
                results.append(r)

    # Then semantic results
    for r in semantic_search(dataset_id, question, top_k=top_k):
        if r["row_index"] not in seen_indices:
            seen_indices.add(r["row_index"])
            results.append(r)

    return results


def _normalize_token(token: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "", token.lower())
    if cleaned.endswith("ies") and len(cleaned) > 4:
        cleaned = cleaned[:-3] + "y"
    for suffix in ("ing", "ed", "es", "s"):
        if cleaned.endswith(suffix) and len(cleaned) > len(suffix) + 2:
            cleaned = cleaned[: -len(suffix)]
            break
    return cleaned


def _tokenize(text_value: str) -> List[str]:
    raw_tokens = re.split(r"[^a-zA-Z0-9]+", text_value.lower())
    normalized = [_normalize_token(tok) for tok in raw_tokens if tok]
    return [tok for tok in normalized if tok]


def _normalize_text(text_value: Any) -> str:
    lowered = str(text_value or "").lower()
    cleaned = re.sub(r"[^a-z0-9% ]+", " ", lowered)
    return " ".join(cleaned.split())


def _contains_phrase(haystack: str, needle: str) -> bool:
    if not haystack or not needle:
        return False
    return f" {needle} " in f" {haystack} "


def _extract_top_n(question: str) -> int:
    match = re.search(r"\btop\s+(\d{1,2})\b", question.lower())
    if not match:
        return 1
    return max(1, min(int(match.group(1)), 20))


def _likely_analytic_query(question: str) -> bool:
    lowered = question.lower()
    if "how many" in lowered:
        return True
    if "group by" in lowered:
        return True
    tokens = set(_tokenize(question))
    return bool(tokens & _ANALYTIC_TRIGGER_TOKENS)


def _value_matches_filter(row_value: Any, filter_value: str) -> bool:
    if row_value is None:
        return False
    row_norm = _normalize_text(row_value)
    filter_norm = _normalize_text(filter_value)
    if not row_norm or not filter_norm:
        return False
    return row_norm == filter_norm or _contains_phrase(row_norm, filter_norm)


def _infer_filters(
    question: str,
    rows: List[Tuple[int, Dict[str, Any]]],
    columns: List[str],
    numeric_columns: Set[str],
) -> List[Tuple[str, str]]:
    question_norm = _normalize_text(question)
    question_tokens = set(_tokenize(question))
    if not question_norm:
        return []

    candidates: List[Tuple[str, str, float]] = []

    # Column-explicit filters: "country is india", "product = dark bars"
    for col in columns:
        col_norm = _normalize_text(col)
        if not col_norm:
            continue
        pattern = (
            rf"\b{re.escape(col_norm)}\b\s*"
            r"(?:=|is|equals|equal to)\s*"
            r"([a-z0-9%&/ .,'\-]{1,60})"
        )
        match = re.search(pattern, question_norm)
        if not match:
            continue
        candidate = match.group(1)
        candidate = re.split(
            r"\b(?:and|or|by|with|where|from|for|that|who|which|what)\b",
            candidate,
            maxsplit=1,
        )[0].strip(" ,.'\"")
        if candidate:
            candidates.append((col, candidate, 100.0 + len(candidate)))

    # Value-driven filters: detect known categorical values inside the question.
    value_to_columns: Dict[str, Dict[str, str]] = defaultdict(dict)
    for _, row_data in rows:
        for col, val in row_data.items():
            if col in numeric_columns:
                continue
            if val is None:
                continue
            value_norm = _normalize_text(val)
            if len(value_norm) < 2:
                continue
            if value_norm in _STOP_WORDS:
                continue
            if _DIGIT_ONLY_RE.fullmatch(value_norm):
                continue
            if len(value_to_columns[value_norm]) >= 3:
                continue
            value_to_columns[value_norm][col] = str(val)

    for value_norm, column_map in value_to_columns.items():
        if not _contains_phrase(question_norm, value_norm):
            continue
        for col, raw_value in column_map.items():
            score = float(len(value_norm.split()) * 5 + len(value_norm))
            col_tokens = set(_tokenize(col))
            if col_tokens & question_tokens:
                score += 3.0
            candidates.append((col, raw_value, score))

    by_column: Dict[str, Tuple[str, float]] = {}
    for col, value, score in sorted(candidates, key=lambda item: item[2], reverse=True):
        if col in by_column and by_column[col][1] >= score:
            continue
        by_column[col] = (value, score)

    ordered = sorted(
        ((col, value, score) for col, (value, score) in by_column.items()),
        key=lambda item: item[2],
        reverse=True,
    )
    return [(col, value) for col, value, _ in ordered[:3]]


def _apply_filters(
    rows: List[Tuple[int, Dict[str, Any]]],
    filters: List[Tuple[str, str]],
) -> List[Tuple[int, Dict[str, Any]]]:
    if not filters:
        return rows

    filtered: List[Tuple[int, Dict[str, Any]]] = []
    for row_index, row_data in rows:
        if all(_value_matches_filter(row_data.get(col), val) for col, val in filters):
            filtered.append((row_index, row_data))
    return filtered


def _sql_escape_literal(value: str) -> str:
    return value.replace("'", "''")


def _sql_metric_expr(metric_column: str) -> str:
    escaped = _sql_escape_literal(metric_column)
    return (
        "NULLIF(regexp_replace(COALESCE(row_data->>'"
        + escaped
        + "', ''), '[^0-9.\\-]', '', 'g'), '')::double precision"
    )


def _sql_filter_clauses(dataset_id: int, filters: List[Tuple[str, str]]) -> str:
    clauses = [f"dataset_id = {int(dataset_id)}"]
    for col, value in filters:
        col_escaped = _sql_escape_literal(col)
        val_escaped = _sql_escape_literal(value)
        clauses.append(
            "LOWER(COALESCE(row_data->>'"
            + col_escaped
            + "', '')) = LOWER('"
            + val_escaped
            + "')"
        )
    return " AND ".join(clauses)


def _sql_equivalent_query(
    dataset_id: int,
    mode: str,
    filters: List[Tuple[str, str]],
    metric_column: Optional[str],
    group_column: Optional[str],
    operator: Optional[str],
    top_n: int,
) -> Optional[str]:
    where_sql = _sql_filter_clauses(dataset_id, filters)

    if mode == "count" and not group_column:
        return f"SELECT COUNT(*) AS metric_value FROM dataset_rows WHERE {where_sql};"

    if mode in {"sum", "avg", "rank"} and not metric_column:
        return None

    metric_expr = _sql_metric_expr(metric_column or "")
    if group_column:
        group_escaped = _sql_escape_literal(group_column)
        if mode == "count":
            metric_sql = "COUNT(*)"
        elif mode == "avg":
            metric_sql = f"AVG({metric_expr})"
        else:
            metric_sql = f"SUM({metric_expr})"
        direction = "ASC" if operator == "min" else "DESC"
        return (
            "SELECT row_data->>'"
            + group_escaped
            + "' AS group_value, "
            + metric_sql
            + " AS metric_value "
            + "FROM dataset_rows "
            + "WHERE "
            + where_sql
            + " GROUP BY 1 ORDER BY metric_value "
            + direction
            + f" LIMIT {int(top_n)};"
        )

    if mode == "count":
        return f"SELECT COUNT(*) AS metric_value FROM dataset_rows WHERE {where_sql};"

    if mode == "sum":
        return (
            "SELECT SUM("
            + metric_expr
            + ") AS metric_value "
            + "FROM dataset_rows WHERE "
            + where_sql
            + ";"
        )
    if mode == "avg":
        return (
            "SELECT AVG("
            + metric_expr
            + ") AS metric_value "
            + "FROM dataset_rows WHERE "
            + where_sql
            + ";"
        )

    direction = "ASC" if operator == "min" else "DESC"
    return (
        "SELECT row_index, row_data "
        + "FROM dataset_rows WHERE "
        + where_sql
        + " AND "
        + metric_expr
        + " IS NOT NULL ORDER BY "
        + metric_expr
        + " "
        + direction
        + " LIMIT 1;"
    )


def _detect_analytic_mode(
    question: str,
    metric_column: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (mode, operator, rank_aggregation)."""
    tokens = set(_tokenize(question))
    lowered = question.lower()

    if tokens & _SUPERLATIVE_MAX_TOKENS:
        rank_aggregation = "avg" if tokens & _AVERAGE_TOKENS else "sum"
        return "rank", "max", rank_aggregation
    if tokens & _SUPERLATIVE_MIN_TOKENS:
        rank_aggregation = "avg" if tokens & _AVERAGE_TOKENS else "sum"
        return "rank", "min", rank_aggregation
    if tokens & _AVERAGE_TOKENS:
        return "avg", None, None
    if tokens & _SUM_TOKENS:
        return "sum", None, None
    if "how many" in lowered or tokens & _COUNT_HINT_TOKENS:
        # Questions like "how many boxes shipped" usually expect SUM(metric), not row count.
        if metric_column:
            metric_tokens = set(_tokenize(metric_column))
            asked_metric = bool(metric_tokens & tokens) or bool(tokens & _METRIC_KEYWORD_TOKENS)
            if asked_metric:
                return "sum", None, None
        return "count", None, None
    return None, None, None


def _parse_number(value: Any) -> Optional[float]:
    return _typed_parse_number(value)
# Commented out on merge conflict just in case it's actually needed
#     if value is None:
#         return None
#     if isinstance(value, bool):
#         return None
#     if isinstance(value, (int, float)):
#         return float(value)

#     raw = str(value).strip()
#     if not raw:
#         return None

#     lowered = raw.lower()
#     if lowered in {"na", "n/a", "nan", "none", "null"}:
#         return None

#     is_negative_parentheses = raw.startswith("(") and raw.endswith(")")
#     if is_negative_parentheses:
#         raw = raw[1:-1].strip()

#     raw = raw.replace(",", "").replace(" ", "")
#     raw = _NUMBER_CLEAN_RE.sub("", raw)

#     multiplier = 1.0
#     if raw and raw[-1].lower() in {"k", "m", "b"}:
#         suffix = raw[-1].lower()
#         raw = raw[:-1]
#         if suffix == "k":
#             multiplier = 1_000.0
#         elif suffix == "m":
#             multiplier = 1_000_000.0
#         else:
#             multiplier = 1_000_000_000.0

#     if raw.endswith("%"):
#         raw = raw[:-1]

#     try:
#         parsed = float(raw) * multiplier
#     except ValueError:
#         return None

#     if is_negative_parentheses:
#         parsed *= -1.0
#     return parsed


def _format_number(value: float) -> str:
    rounded = round(value)
    if abs(value - rounded) < 1e-9:
        return f"{int(rounded):,}"
    return f"{value:,.2f}"


def _is_superlative_query(question: str) -> bool:
    tokens = set(_tokenize(question))
    return bool(tokens & (_SUPERLATIVE_MAX_TOKENS | _SUPERLATIVE_MIN_TOKENS))


def _load_dataset_rows(dataset_id: int) -> List[Tuple[int, Dict[str, Any]]]:
    with SessionLocal() as db:
        result = db.execute(
            text(
                "SELECT row_index, row_data FROM dataset_rows "
                "WHERE dataset_id = :dataset_id ORDER BY row_index"
            ),
            {"dataset_id": dataset_id},
        )
        return [
            (int(row_index), _deserialize_row_data(row_data))
            for row_index, row_data in result.fetchall()
        ]


def _collect_columns(rows: List[Tuple[int, Dict[str, Any]]]) -> List[str]:
    ordered: List[str] = []
    seen: Set[str] = set()
    for _, row_data in rows[:300]:
        for column in row_data.keys():
            if is_internal_key(str(column)):
                continue
            if column not in seen:
                seen.add(column)
                ordered.append(column)
    return ordered


def _detect_numeric_columns(rows: List[Tuple[int, Dict[str, Any]]]) -> Set[str]:
    sampled = rows[:400]
    if not sampled:
        return set()

    numeric_counts: Dict[str, int] = defaultdict(int)
    for _, row_data in sampled:
        for col in row_data.keys():
            if is_internal_key(str(col)):
                continue
            if get_numeric_value(row_data, col) is not None:
                numeric_counts[col] += 1

    min_hits = max(2, int(len(sampled) * 0.40))
    return {
        col for col, count in numeric_counts.items()
        if count >= min_hits
    }


def _question_roles(question: str) -> Set[str]:
    tokens = set(_tokenize(question))
    roles: Set[str] = set()

    if "who" in tokens or tokens & {"seller", "sold", "sale", "salesperson", "sales", "person", "name"}:
        roles.add("person")
    if tokens & {"product", "item", "sku", "brand", "flavor"}:
        roles.add("product")
    if "where" in tokens or tokens & {"country", "region", "market", "city", "state", "location"}:
        roles.add("location")
    if "when" in tokens or tokens & {"date", "day", "month", "year", "time"}:
        roles.add("date")
    return roles


def _column_role_bonus(column: str, roles: Set[str]) -> float:
    if not roles:
        return 0.0
    col_tokens = set(_tokenize(column))
    if not col_tokens:
        return 0.0

    bonus = 0.0
    for role in roles:
        keywords = _QUESTION_ROLE_COLUMN_KEYWORDS.get(role)
        if keywords and col_tokens & keywords:
            bonus += 5.0
    return bonus


def _looks_like_single_row_rank_query(question: str) -> bool:
    lowered = question.lower()
    if _SINGLE_ROW_HINT_RE.search(lowered):
        return True
    if re.search(r"\bin\s+one\s+(deal|transaction|row|entry|order|sale|shipment)\b", lowered):
        return True

    # "in a day" is commonly intended as a single daily record, not grouped totals.
    if _SINGLE_DAY_HINT_RE.search(lowered):
        if not _DAY_GROUPING_HINT_RE.search(lowered) and not _EXPLICIT_AGGREGATE_HINT_RE.search(lowered):
            return True
    return False


def _extract_group_hint(question: str) -> Optional[str]:
    lowered = question.lower()
    patterns = (
        r"\b(?:what|which)\s+([a-z0-9_ ]{1,48}?)\s+"
        r"(?:has|have|had|with|is|are|sold|ship|shipped)\b",
        r"\bby\s+([a-z0-9_ ]{1,48}?)(?:[\s?.!,]|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, lowered)
        if not match:
            continue
        candidate = " ".join(match.group(1).split())
        if candidate:
            return candidate
    return None


def _column_overlap_score(column: str, token_set: Set[str], normalized_phrase: str) -> float:
    col_tokens = set(_tokenize(column))
    if not col_tokens:
        return 0.0

    overlap = col_tokens & token_set
    if not overlap:
        return 0.0

    score = float(len(overlap) * 3)
    normalized_column = " ".join(_tokenize(column))
    if normalized_column and normalized_column in normalized_phrase:
        score += 2.0
    if col_tokens & _METRIC_KEYWORD_TOKENS:
        score += 0.4
    return score


def _pick_metric_column(
    columns: List[str],
    numeric_columns: Set[str],
    question: str,
) -> Optional[str]:
    question_tokens = set(_tokenize(question))
    normalized_question = " ".join(_tokenize(question))

    best_col: Optional[str] = None
    best_score = 0.0
    for column in columns:
        if column not in numeric_columns:
            continue
        score = _column_overlap_score(column, question_tokens, normalized_question)
        if score > best_score:
            best_score = score
            best_col = column

    if best_col is not None:
        return best_col

    keyword_sorted = sorted(
        numeric_columns,
        key=lambda col: (
            len(set(_tokenize(col)) & _METRIC_KEYWORD_TOKENS),
            col.lower(),
        ),
        reverse=True,
    )
    return keyword_sorted[0] if keyword_sorted else None


def _pick_group_column(
    columns: List[str],
    numeric_columns: Set[str],
    metric_column: Optional[str],
    question: str,
) -> Optional[str]:
    candidate_columns = [
        col for col in columns if col not in numeric_columns and col != metric_column
    ]
    if not candidate_columns:
        return None

    hint = _extract_group_hint(question)
    if hint:
        hint_tokens = set(_tokenize(hint))
        normalized_hint = " ".join(_tokenize(hint))
        best_col: Optional[str] = None
        best_score = 0.0
        for column in candidate_columns:
            score = _column_overlap_score(column, hint_tokens, normalized_hint)
            if score > best_score:
                best_score = score
                best_col = column
        if best_col:
            return best_col

    question_tokens = set(_tokenize(question))
    normalized_question = " ".join(_tokenize(question))
    roles = _question_roles(question)
    best_col = None
    best_score = 0.0
    for column in candidate_columns:
        score = _column_overlap_score(column, question_tokens, normalized_question)
        score += _column_role_bonus(column, roles)
        if score > best_score:
            best_score = score
            best_col = column
    return best_col


def _pick_row_answer_column(
    columns: List[str],
    numeric_columns: Set[str],
    metric_column: Optional[str],
    question: str,
) -> Optional[str]:
    candidate_columns = [
        col for col in columns if col not in numeric_columns and col != metric_column
    ]
    if not candidate_columns:
        return None

    question_tokens = set(_tokenize(question))
    normalized_question = " ".join(_tokenize(question))
    roles = _question_roles(question)

    best_col: Optional[str] = None
    best_score = 0.0
    for column in candidate_columns:
        score = _column_overlap_score(column, question_tokens, normalized_question)
        score += _column_role_bonus(column, roles)
        if score > best_score:
            best_score = score
            best_col = column

    if best_col:
        return best_col
    return candidate_columns[0]


def _infer_aggregate_answer(
    dataset_id: int,
    question: str,
) -> Optional[Dict[str, Any]]:
    if not _likely_analytic_query(question):
        return None

    rows = _load_dataset_rows(dataset_id)
    if not rows:
        return None

    columns = _collect_columns(rows)
    if not columns:
        return None

    numeric_columns = _detect_numeric_columns(rows)
    metric_column = (
        _pick_metric_column(columns, numeric_columns, question)
        if numeric_columns
        else None
    )

    mode, operator, rank_aggregation = _detect_analytic_mode(question, metric_column)
    if not mode:
        return None
    metric_for_mode = metric_column if mode in {"sum", "avg", "rank"} else None

    inferred_filters = _infer_filters(question, rows, columns, numeric_columns)
    filtered_rows = _apply_filters(rows, inferred_filters)
    if not filtered_rows:
        filter_pairs = {col: val for col, val in inferred_filters}
        sql_mode = rank_aggregation if mode == "rank" and rank_aggregation else mode
        return {
            "answer": "No rows matched the requested filters.",
            "answer_type": "aggregate",
            "answer_details": {
                "operation": mode,
                "filters": filter_pairs,
                "matched_rows": 0,
                "sql_query": _sql_equivalent_query(
                    dataset_id=dataset_id,
                    mode=sql_mode,
                    filters=inferred_filters,
                    metric_column=metric_for_mode,
                    group_column=None,
                    operator=operator,
                    top_n=1,
                ),
            },
        }

    if mode in {"sum", "avg", "rank"} and metric_for_mode is None:
        return None

    top_n = _extract_top_n(question)
    group_column = _pick_group_column(
        columns, numeric_columns, metric_for_mode, question
    )
    has_group_phrase = bool(
        re.search(r"\b(by|per|each|group by)\b", question.lower())
        or _extract_group_hint(question)
    )
    use_grouping = bool(
        group_column
        and (
            (mode == "rank" and not _looks_like_single_row_rank_query(question))
            or has_group_phrase
            or top_n > 1
        )
    )

    filter_pairs = {col: val for col, val in inferred_filters}
    sql_mode = rank_aggregation if mode == "rank" and rank_aggregation else mode

    if use_grouping and group_column:
        grouped: Dict[str, Dict[str, Any]] = {}
        for row_index, row_data in filtered_rows:
            metric_value = (
                get_numeric_value(row_data, metric_for_mode) if metric_for_mode else None
            )
            if mode in {"sum", "avg", "rank"} and metric_value is None:
                continue

            group_raw = row_data.get(group_column)
            group_value = str(group_raw).strip() if group_raw is not None else ""
            if not group_value:
                group_value = "(blank)"

            stat = grouped.get(group_value)
            if stat is None:
                grouped[group_value] = {
                    "total": float(metric_value or 0.0),
                    "count": 1,
                    "row_index": row_index,
                    "row_data": row_data,
                    "row_metric": metric_value,
                }
                continue

            stat["count"] += 1
            if metric_value is not None:
                stat["total"] += metric_value
                if stat["row_metric"] is None:
                    stat["row_metric"] = metric_value
                    stat["row_index"] = row_index
                    stat["row_data"] = row_data
                elif (operator == "min" and metric_value < stat["row_metric"]) or (
                    operator != "min" and metric_value > stat["row_metric"]
                ):
                    stat["row_metric"] = metric_value
                    stat["row_index"] = row_index
                    stat["row_data"] = row_data

        if not grouped:
            return None

        ranking: List[Tuple[str, Dict[str, Any], float]] = []
        for key, stat in grouped.items():
            if mode == "count":
                metric_value = float(stat["count"])
            elif mode == "avg" or (mode == "rank" and rank_aggregation == "avg"):
                metric_value = stat["total"] / max(int(stat["count"]), 1)
            else:
                metric_value = stat["total"]
            ranking.append((key, stat, metric_value))

        reverse = not (mode == "rank" and operator == "min")
        ranking.sort(key=lambda item: item[2], reverse=reverse)
        top_entries = ranking[:top_n]
        if not top_entries:
            return None

        group_value, stat, metric_value = top_entries[0]
        source_row_index = int(stat["row_index"])
        source_row_data = stat["row_data"]
        source_result = _build_result_item(
            dataset_id=dataset_id,
            row_index=source_row_index,
            row_data=source_row_data,
            score=1.0,
            question=question,
            match_type="aggregate",
        )

        if top_n > 1:
            metric_label = (
                "average" if mode == "avg" or (mode == "rank" and rank_aggregation == "avg")
                else "count" if mode == "count"
                else "total"
            )
            metric_name = "rows" if mode == "count" else metric_for_mode
            preview = "; ".join(
                f"{idx + 1}) {entry_key} ({_format_number(entry_metric)})"
                for idx, (entry_key, _, entry_metric) in enumerate(top_entries)
            )
            answer = (
                f"Top {len(top_entries)} {group_column} by {metric_label} {metric_name}: "
                f"{preview}."
            )
        elif mode == "rank":
            direction = "lowest" if operator == "min" else "highest"
            aggregate_label = "average" if rank_aggregation == "avg" else "total"
            answer = (
                f"The {group_column} with the {direction} {aggregate_label} {metric_for_mode} "
                f"is {group_value} ({_format_number(metric_value)})."
            )
        elif mode == "count":
            answer = (
                f"The {group_column} with the highest count is "
                f"{group_value} ({_format_number(metric_value)} rows)."
            )
        elif mode == "avg":
            answer = (
                f"The {group_column} with the highest average {metric_for_mode} is "
                f"{group_value} ({_format_number(metric_value)})."
            )
        else:
            answer = (
                f"The {group_column} with the highest total {metric_for_mode} is "
                f"{group_value} ({_format_number(metric_value)})."
            )

        return {
            "answer": answer,
            "answer_type": "aggregate",
            "answer_details": {
                "operation": mode,
                "group_by_column": group_column,
                "group_value": group_value,
                "metric_column": metric_for_mode,
                "metric_value": metric_value,
                "metric_value_display": _format_number(metric_value),
                "aggregation": rank_aggregation if mode == "rank" else mode,
                "operator": operator,
                "top_n": top_n,
                "filters": filter_pairs,
                "matched_rows": len(filtered_rows),
                "source_row_index": source_row_index,
                "source_url": source_result["source_url"],
                "top_highlight_id": source_result["top_highlight_id"],
                "highlight_url": source_result["highlight_url"],
                "sql_query": _sql_equivalent_query(
                    dataset_id=dataset_id,
                    mode=sql_mode,
                    filters=inferred_filters,
                    metric_column=metric_for_mode,
                    group_column=group_column,
                    operator=operator,
                    top_n=top_n,
                ),
                "top_groups": [
                    {"group_value": key, "metric_value": value}
                    for key, _, value in top_entries
                ],
            },
            "source_result": source_result,
        }

    source_row_index: Optional[int] = None
    source_row_data: Optional[Dict[str, Any]] = None
    metric_value: Optional[float] = None
    answer_column: Optional[str] = None
    answer_value: Optional[str] = None

    if mode == "count":
        metric_value = float(len(filtered_rows))
        source_row_index = int(filtered_rows[0][0]) if filtered_rows else None
        source_row_data = filtered_rows[0][1] if filtered_rows else None
        answer = f"There are {_format_number(metric_value)} matching rows."
    elif mode in {"sum", "avg"}:
        values: List[float] = []
        best_metric: Optional[float] = None
        for row_index, row_data in filtered_rows:
            parsed = get_numeric_value(row_data, metric_for_mode) if metric_for_mode else None
            if parsed is None:
                continue
            values.append(parsed)
            if best_metric is None or parsed > best_metric:
                best_metric = parsed
                source_row_index = row_index
                source_row_data = row_data
        if not values:
            return None
        metric_value = sum(values) if mode == "sum" else (sum(values) / len(values))
        if source_row_index is None and filtered_rows:
            source_row_index = filtered_rows[0][0]
            source_row_data = filtered_rows[0][1]
        metric_label = "total" if mode == "sum" else "average"
        answer = f"The {metric_label} {metric_for_mode} is {_format_number(metric_value)}."
    else:
        best_metric: Optional[float] = None
        for row_index, row_data in filtered_rows:
            parsed = get_numeric_value(row_data, metric_for_mode) if metric_for_mode else None
            if parsed is None:
                continue
            if best_metric is None:
                best_metric = parsed
                source_row_index = row_index
                source_row_data = row_data
                continue
            is_better = parsed < best_metric if operator == "min" else parsed > best_metric
            if is_better:
                best_metric = parsed
                source_row_index = row_index
                source_row_data = row_data
        if source_row_index is None or source_row_data is None or best_metric is None:
            return None
        metric_value = best_metric
        direction = "lowest" if operator == "min" else "highest"
        answer_column = _pick_row_answer_column(
            columns=columns,
            numeric_columns=numeric_columns,
            metric_column=metric_for_mode,
            question=question,
        )
        if answer_column:
            raw_value = source_row_data.get(answer_column)
            if raw_value is not None and str(raw_value).strip():
                answer_value = str(raw_value).strip()

        if answer_column and answer_value:
            answer = (
                f"The {direction} {metric_for_mode} in a single row is {_format_number(metric_value)} "
                f"by {answer_column} {answer_value} (row {source_row_index})."
            )
        else:
            answer = (
                f"The {direction} {metric_for_mode} is {_format_number(metric_value)} "
                f"(row {source_row_index})."
            )

    source_result = None
    if source_row_index is not None and source_row_data is not None:
        source_result = _build_result_item(
            dataset_id=dataset_id,
            row_index=int(source_row_index),
            row_data=source_row_data,
            score=1.0,
            question=question,
            match_type="aggregate",
        )

    answer_details: Dict[str, Any] = {
        "operation": mode,
        "metric_column": metric_for_mode,
        "metric_value": metric_value,
        "metric_value_display": _format_number(metric_value or 0.0),
        "operator": operator,
        "filters": filter_pairs,
        "matched_rows": len(filtered_rows),
        "sql_query": _sql_equivalent_query(
            dataset_id=dataset_id,
            mode=sql_mode,
            filters=inferred_filters,
            metric_column=metric_for_mode,
            group_column=None,
            operator=operator,
            top_n=1,
        ),
    }
    if source_result:
        answer_details.update(
            {
                "source_row_index": source_result["row_index"],
                "source_url": source_result["source_url"],
                "top_highlight_id": source_result["top_highlight_id"],
                "highlight_url": source_result["highlight_url"],
                "source_row_data": source_result.get("row_data"),
            }
        )
    if answer_column:
        answer_details["answer_column"] = answer_column
    if answer_value:
        answer_details["answer_value"] = answer_value

    return {
        "answer": answer,
        "answer_type": "aggregate",
        "answer_details": answer_details,
        "source_result": source_result,
    }


def _build_final_response(payload: Dict[str, Any]) -> str:
    answer = str(payload.get("answer") or "").strip()
    details = payload.get("answer_details") or {}
    ui_link = details.get("highlight_url") or details.get("source_url")

    if answer:
        lines = [answer]
        if ui_link:
            lines.append(f"Link: {ui_link}")
        return "\n".join(lines)

    results = payload.get("results") or []
    if not results:
        return "No matching rows found."

    top = results[0]
    row_index = top.get("row_index")
    highlights = top.get("highlights") or []
    if highlights:
        h = highlights[0]
        lead = f"Best match: {h.get('column')} = {h.get('value')} (row {row_index})."
    else:
        lead = f"Best matching row is {row_index}."

    lines = [lead]
    ui_link = top.get("highlight_url") or top.get("source_url")
    if ui_link:
        lines.append(f"Link: {ui_link}")
    return "\n".join(lines)


def _extract_highlight_id_from_url(value: Any) -> Optional[str]:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = urlparse(value)
        if "/highlight/" not in parsed.path:
            return None
        tail = parsed.path.split("/highlight/", 1)[1].strip("/")
        if not tail:
            return None
        return unquote(tail)
    except Exception:
        return None


def _verify_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []
    errors: List[str] = []
    details = payload.get("answer_details") or {}

    source_row_index = details.get("source_row_index")
    if source_row_index is not None:
        results = payload.get("results") or []
        present = any(
            int(item.get("row_index")) == int(source_row_index)
            for item in results
            if isinstance(item, dict) and item.get("row_index") is not None
        )
        checks.append(
            {
                "name": "source_row_present_in_results",
                "passed": present,
                "source_row_index": int(source_row_index),
            }
        )
        if not present:
            errors.append("source_row_index not present in results.")

    answer_column = details.get("answer_column")
    answer_value = details.get("answer_value")
    source_row_data = details.get("source_row_data") or {}
    if answer_column and answer_value is not None and isinstance(source_row_data, dict):
        source_value = source_row_data.get(answer_column)
        is_match = source_value is not None and str(source_value).strip() == str(answer_value).strip()
        checks.append(
            {
                "name": "answer_value_matches_source_row",
                "passed": is_match,
                "answer_column": answer_column,
            }
        )
        if not is_match:
            errors.append("answer_value does not match source_row_data for answer_column.")

    op = str(details.get("operation") or "").lower()
    group_column = details.get("group_by_column")
    metric_column = details.get("metric_column")
    metric_value = details.get("metric_value")
    if (
        op == "rank"
        and not group_column
        and isinstance(source_row_data, dict)
        and metric_column
        and metric_value is not None
    ):
        source_metric = _parse_number(source_row_data.get(metric_column))
        metric_ok = source_metric is not None and abs(float(source_metric) - float(metric_value)) < 1e-9
        checks.append(
            {
                "name": "rank_metric_matches_source_row",
                "passed": metric_ok,
                "metric_column": metric_column,
            }
        )
        if not metric_ok:
            errors.append("rank metric_value does not match source row metric.")

    highlight_id = details.get("top_highlight_id")
    if not highlight_id:
        highlight_id = _extract_highlight_id_from_url(details.get("highlight_url") or details.get("source_url"))
    if not highlight_id:
        results = payload.get("results") or []
        if results and isinstance(results[0], dict):
            highlight_id = results[0].get("top_highlight_id")
            if not highlight_id:
                highlight_id = _extract_highlight_id_from_url(
                    results[0].get("highlight_url") or results[0].get("source_url")
                )
    if highlight_id:
        highlight_exists = get_highlight(str(highlight_id)) is not None
        checks.append(
            {
                "name": "highlight_exists",
                "passed": highlight_exists,
                "highlight_id": str(highlight_id),
            }
        )
        if not highlight_exists:
            errors.append("highlight reference is missing.")

    status = "pass" if not errors else "fail"
    return {"status": status, "checks": checks, "errors": errors}


def smart_query(
    dataset_id: int,
    question: str,
    filters: Optional[Dict[str, str]] = None,
    top_k: int = 10,
) -> Dict[str, Any]:
    top_k = max(1, top_k)
    results = hybrid_search(
        dataset_id=dataset_id,
        question=question,
        filters=filters,
        top_k=top_k,
    )

    response: Dict[str, Any] = {
        "dataset_id": dataset_id,
        "question": question,
        "results": results,
        "dataset_url": _table_ui_url(dataset_id),
    }

    aggregate_answer = _infer_aggregate_answer(dataset_id, question)
    if not aggregate_answer:
        response["final_response"] = _build_final_response(response)
        if _verification_enabled():
            verification = _verify_response(response)
            response["verification"] = verification
            if verification["status"] != "pass" and _fail_closed_on_verification_error():
                response["final_response"] = "I could not verify this answer against source rows."
        return response

    source_result = aggregate_answer.get("source_result")
    if source_result:
        source_row_index = source_result.get("row_index")
        if source_row_index not in {r["row_index"] for r in results}:
            response["results"] = [source_result] + results

    response["answer"] = aggregate_answer["answer"]
    response["answer_type"] = aggregate_answer["answer_type"]
    response["answer_details"] = aggregate_answer["answer_details"]
    response["final_response"] = _build_final_response(response)
    if _verification_enabled():
        verification = _verify_response(response)
        response["verification"] = verification
        if verification["status"] != "pass" and _fail_closed_on_verification_error():
            fallback_lines = [
                "I could not verify this answer against source rows.",
                f"Open table: {response.get('dataset_url')}",
            ]
            response["final_response"] = "\n".join(fallback_lines)
    return response


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
            score = sum(2.0 if tok in keywords else 0.5 for tok in overlap)
            max_possible = max(len(question_tokens), 1)
            highlight_id = f"d{dataset_id}_r{row_index}_{col}"
            highlights.append(
                {
                    "highlight_id": highlight_id,
                    "column": col,
                    "value": str(val),
                    "relevance": score / max_possible,
                }
            )

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
    public_row_data = strip_internal_fields(row_data)

    if column not in public_row_data:
        return None

    return {
        "highlight_id": highlight_id,
        "dataset_id": dataset_id,
        "row_index": row_index,
        "column": column,
        "value": public_row_data[column],
        "row_context": public_row_data,
    }


# ── Multi-table JOIN support ──────────────────────────────────────


def find_common_columns(left_dataset_id: int, right_dataset_id: int) -> List[str]:
    """Return column names that appear in both datasets."""
    left_rows = _load_dataset_rows(left_dataset_id)
    right_rows = _load_dataset_rows(right_dataset_id)
    left_cols = set(_collect_columns(left_rows))
    right_cols = set(_collect_columns(right_rows))
    return sorted(left_cols & right_cols)


def join_datasets(
    left_dataset_id: int,
    right_dataset_id: int,
    left_column: str,
    right_column: str,
    limit: int = 100,
) -> Dict[str, Any]:
    """Perform an inner JOIN between two datasets on the specified columns.

    Returns a dict with ``columns``, ``rows``, and ``row_count``.
    Each result row is a merged dict with columns prefixed by the dataset name
    when there would be a collision (excluding the join key itself).
    """
    left_rows = _load_dataset_rows(left_dataset_id)
    right_rows = _load_dataset_rows(right_dataset_id)

    left_cols = _collect_columns(left_rows)
    right_cols = _collect_columns(right_rows)

    if left_column not in left_cols:
        raise ValueError(
            f"Column '{left_column}' not found in dataset {left_dataset_id}."
        )
    if right_column not in right_cols:
        raise ValueError(
            f"Column '{right_column}' not found in dataset {right_dataset_id}."
        )

    # Fetch dataset names for prefixing colliding columns
    left_name = _dataset_display_name(left_dataset_id)
    right_name = _dataset_display_name(right_dataset_id)

    # Build lookup from the right side keyed by normalized join value
    right_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for _ri, row_data in right_rows:
        key = _normalize_text(row_data.get(right_column))
        if key:
            right_index[key].append(strip_internal_fields(row_data))

    # Determine colliding column names (excluding the join key itself)
    left_col_set = set(left_cols)
    right_col_set = set(right_cols)
    colliding = (left_col_set & right_col_set) - {left_column, right_column}

    # Perform the join
    joined_rows: List[Dict[str, Any]] = []
    for _li, left_data in left_rows:
        key = _normalize_text(left_data.get(left_column))
        if not key or key not in right_index:
            continue
        clean_left = strip_internal_fields(left_data)
        for right_data in right_index[key]:
            merged: Dict[str, Any] = {}
            for col, val in clean_left.items():
                col_name = f"{left_name}.{col}" if col in colliding else col
                merged[col_name] = val
            for col, val in right_data.items():
                if col == right_column and left_column == right_column:
                    continue  # already included from left side
                col_name = f"{right_name}.{col}" if col in colliding else col
                merged[col_name] = val
            joined_rows.append(merged)
            if len(joined_rows) >= limit:
                break
        if len(joined_rows) >= limit:
            break

    # Collect output column order (sample first rows to determine column set)
    output_columns: List[str] = []
    seen: Set[str] = set()
    for row in joined_rows[:50]:
        for col in row:
            if col not in seen:
                seen.add(col)
                output_columns.append(col)

    return {
        "columns": output_columns,
        "rows": joined_rows,
        "row_count": len(joined_rows),
    }


def _dataset_display_name(dataset_id: int) -> str:
    """Return the user-visible name for a dataset, falling back to its ID."""
    with SessionLocal() as db:
        result = db.execute(
            text("SELECT name FROM datasets WHERE id = :id"),
            {"id": dataset_id},
        )
        row = result.fetchone()
    if row and row[0]:
        return str(row[0])
    return f"dataset_{dataset_id}"
