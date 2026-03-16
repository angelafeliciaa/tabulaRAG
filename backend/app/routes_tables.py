import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import delete, select, text

from app.db import SessionLocal, engine
from app.auth import require_auth, get_user_id_from_auth
from app.index_jobs import clear_index_job, get_index_jobs
from app.models import Dataset, DatasetColumn, DatasetRow
from app.qdrant_client import delete_collection, get_collection_point_count
from app.normalization import strip_internal_fields
from app.name_guard import normalize_dataset_name_or_raise

router = APIRouter()


class RenameRequest(BaseModel):
    name: str


def _slice_column_expr(column_name: str) -> str:
    """SQL expression for normalized value of a column from row_data (for ORDER BY in slice)."""
    escaped = column_name.replace("'", "''")
    if engine.dialect.name == "sqlite":
        json_key = column_name.replace("\\", "\\\\").replace('"', '\\"')
        return f"COALESCE(json_extract(row_data, '$.\"{json_key}\".normalized'), json_extract(row_data, '$.\"{json_key}\"'))"
    return f"COALESCE((row_data::jsonb -> '{escaped}') ->> 'normalized', row_data::jsonb ->> '{escaped}')"


def _slice_order_by_clause(sort_column: str, sort_direction: str) -> str:
    """ORDER BY clause for slice: numeric sort when value looks like a number, else text (so dates like 2022-01-04 sort correctly)."""
    col = _slice_column_expr(sort_column)
    dir_upper = sort_direction.upper()
    nulls = "NULLS LAST" if dir_upper == "ASC" else "NULLS FIRST"
    if engine.dialect.name == "sqlite":
        # SQLite: order by text only to avoid casting dates (e.g. 2022-01-04) to real
        return f"{col} {dir_upper}"
    # PostgreSQL: only cast to double when value matches numeric pattern; else NULL so we sort by text (dates, text)
    numeric_expr = f"(CASE WHEN TRIM({col}) ~ '^[-+]?[0-9]*\\.?[0-9]+$' THEN ({col}::double precision) ELSE NULL END)"
    return f"{numeric_expr} {dir_upper} {nulls}, {col} {dir_upper}"


def _normalize_row_data(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return strip_internal_fields(raw)
    if isinstance(raw, str):
        try:
            parsed: Any = json.loads(raw)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, dict):
                return strip_internal_fields(parsed)
        except Exception:
            return {}
    return {}


def _delete_collection_safe(dataset_id: int) -> None:
    try:
        delete_collection(dataset_id)
    except Exception:
        # Collection cleanup is best-effort and should not block API delete.
        pass


@router.get(
    "/tables",
    summary="List all datasets",
    description="Returns indexed datasets with their IDs, names, and metadata. Pending uploads are omitted unless include_pending=true.",
)
def list_tables(include_pending: bool = False, auth: dict = Depends(require_auth)):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        query = select(Dataset).order_by(Dataset.id.desc())
        if not include_pending:
            query = query.where(Dataset.is_index_ready.is_(True))
        if user_id is not None:
            query = query.where(Dataset.user_id == user_id)
        datasets = db.execute(query).scalars().all()
        return [
            {
                "dataset_id": d.id,
                "name": d.name,
                "source_filename": d.source_filename,
                "row_count": d.row_count,
                "column_count": d.column_count,
                "created_at": d.created_at.isoformat(),
            }
            for d in datasets
        ]


def list_tables_internal(user_id: int | None = None, include_pending: bool = False) -> list:
    """Internal helper to list datasets without auth dependency."""
    with SessionLocal() as db:
        query = select(Dataset).order_by(Dataset.id.desc())
        if not include_pending:
            query = query.where(Dataset.is_index_ready.is_(True))
        if user_id is not None:
            query = query.where(Dataset.user_id == user_id)
        datasets = db.execute(query).scalars().all()
        return [
            {
                "dataset_id": d.id,
                "name": d.name,
                "source_filename": d.source_filename,
                "row_count": d.row_count,
                "column_count": d.column_count,
                "created_at": d.created_at.isoformat(),
            }
            for d in datasets
        ]


def get_columns_internal(dataset_id: int) -> dict:
    """Internal helper to get columns without auth (caller must verify access first)."""
    with SessionLocal() as db:
        columns = (
            db.execute(
                select(DatasetColumn)
                .where(DatasetColumn.dataset_id == dataset_id)
                .order_by(DatasetColumn.column_index)
            )
            .scalars()
            .all()
        )
    return {
        "dataset_id": dataset_id,
        "columns": [
            {
                "column_index": c.column_index,
                "original_name": c.original_name,
                "normalized_name": c.normalized_name,
            }
            for c in columns
        ],
    }


def _check_dataset_access(db, dataset_id: int, user_id: int | None) -> Dataset:
    """Load dataset and verify ownership. Returns the dataset or raises 404."""
    dataset = db.execute(
        select(Dataset).where(Dataset.id == dataset_id)
    ).scalar_one_or_none()
    if dataset is None:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    if user_id is not None and dataset.user_id != user_id:
        raise HTTPException(status_code=404, detail="Dataset not found.")
    return dataset


@router.get(
    "/tables/{dataset_id}/columns",
    summary="List all columns for a dataset",
    description="Returns column names and indexes for a dataset. Always call this to understand the data structure and actual column names before querying.",
)
def get_cols_for_dataset(dataset_id: int, auth: dict = Depends(require_auth)):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        _check_dataset_access(db, dataset_id, user_id)

        columns = (
            db.execute(
                select(DatasetColumn)
                .where(DatasetColumn.dataset_id == dataset_id)
                .order_by(DatasetColumn.column_index)
            )
            .scalars()
            .all()
        )

    return {
        "dataset_id": dataset_id,
        "columns": [
            {
                "column_index": c.column_index,
                "original_name": c.original_name,
                "normalized_name": c.normalized_name,
            }
            for c in columns
        ],
    }


@router.get(
    "/tables/{dataset_id}/slice",
    summary="Browse raw rows from a dataset",
    description="Returns rows in order by row index (or by sort_column when provided). Use sort_column + sort_direction for multipage sorting.",
)
def get_table_slice(
    dataset_id: int,
    offset: int = Query(
        default=0,
        description="Number of rows to skip. Use 0 to start from the beginning.",
    ),
    limit: int = Query(
        default=30, description="Number of rows to return. Default is 30."
    ),
    sort_column: Optional[str] = Query(
        default=None,
        description="Normalized column name to sort by. When set, rows are ordered by this column (multipage sort).",
    ),
    sort_direction: Optional[str] = Query(
        default="asc",
        description="Sort direction: 'asc' or 'desc'. Used only when sort_column is set.",
    ),
    auth: dict = Depends(require_auth),
):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        dataset = _check_dataset_access(db, dataset_id, user_id)

        if sort_column and sort_direction:
            # Validate sort_column exists
            col_match = (
                db.execute(
                    select(DatasetColumn)
                    .where(DatasetColumn.dataset_id == dataset_id)
                    .where(DatasetColumn.normalized_name == sort_column)
                )
                .scalars()
                .first()
            )
            if not col_match:
                raise HTTPException(status_code=400, detail=f"Unknown sort_column: {sort_column}")
            dir_norm = sort_direction.lower() in ("desc", "descending") and "desc" or "asc"
            order_sql = _slice_order_by_clause(sort_column, dir_norm)
            rows = (
                db.execute(
                    select(DatasetRow)
                    .where(DatasetRow.dataset_id == dataset_id)
                    .order_by(text(order_sql))
                    .offset(offset)
                    .limit(limit)
                )
                .scalars()
                .all()
            )
        else:
            rows = (
                db.execute(
                    select(DatasetRow)
                    .where(DatasetRow.dataset_id == dataset_id)
                    .order_by(DatasetRow.row_index)
                    .offset(offset)
                    .limit(limit)
                )
                .scalars()
                .all()
            )

        columns = (
            db.execute(
                select(DatasetColumn)
                .where(DatasetColumn.dataset_id == dataset_id)
                .order_by(DatasetColumn.column_index)
            )
            .scalars()
            .all()
        )

        return {
            "dataset_id": dataset_id,
            "offset": offset,
            "limit": limit,
            "row_count": dataset.row_count,
            "column_count": dataset.column_count,
            "has_header": dataset.has_header,
            "rows": [
                {
                    "row_index": r.row_index,
                    "data": strip_internal_fields(r.row_data),
                }
                for r in rows
            ],
            "columns": [c.normalized_name for c in columns],
            "columns_meta": [
                {"original_name": c.original_name, "normalized_name": c.normalized_name}
                for c in columns
            ],
        }


@router.get("/tables/index-status", include_in_schema=False)
def list_index_status(dataset_id: Optional[List[int]] = Query(default=None), auth: dict = Depends(require_auth)):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        query = select(Dataset.id, Dataset.row_count)
        if dataset_id:
            query = query.where(Dataset.id.in_(dataset_id))
        if user_id is not None:
            query = query.where(Dataset.user_id == user_id)
        dataset_rows = db.execute(query.order_by(Dataset.id.desc())).all()

    dataset_ids = [int(row[0]) for row in dataset_rows]
    tracked_statuses = get_index_jobs(dataset_ids)

    response = []
    for raw_dataset_id, raw_row_count in dataset_rows:
        current_dataset_id = int(raw_dataset_id)
        current_row_count = int(raw_row_count or 0)
        tracked = tracked_statuses.get(current_dataset_id)

        if tracked:
            item = dict(tracked)
            if int(item.get("total_rows", 0)) <= 0 and current_row_count > 0:
                item["total_rows"] = current_row_count
            response.append(item)
            continue

        point_count: Optional[int] = None
        try:
            point_count = get_collection_point_count(current_dataset_id)
        except Exception:
            point_count = None

        if (
            point_count is not None
            and current_row_count > 0
            and int(point_count) < current_row_count
        ):
            progress = float(point_count) / float(current_row_count) * 100.0
            response.append(
                {
                    "dataset_id": current_dataset_id,
                    "state": "indexing",
                    "progress": progress,
                    "processed_rows": int(point_count),
                    "total_rows": current_row_count,
                    "message": "Indexing vectors...",
                    "started_at": None,
                    "updated_at": None,
                    "finished_at": None,
                }
            )
            continue

        response.append(
            {
                "dataset_id": current_dataset_id,
                "state": "ready",
                "progress": 100.0,
                "processed_rows": current_row_count,
                "total_rows": current_row_count,
                "message": "Vector index is ready.",
                "started_at": None,
                "updated_at": None,
                "finished_at": None,
            }
        )

    return response


@router.delete("/tables/{dataset_id}", include_in_schema=False)
def delete_table(dataset_id: int, background_tasks: BackgroundTasks, auth: dict = Depends(require_auth)):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        _check_dataset_access(db, dataset_id, user_id)
        # Use direct SQL deletes to avoid expensive ORM cascade object loading.
        db.execute(delete(DatasetRow).where(DatasetRow.dataset_id == dataset_id))
        db.execute(delete(DatasetColumn).where(DatasetColumn.dataset_id == dataset_id))
        db.execute(delete(Dataset).where(Dataset.id == dataset_id))
        db.commit()
    clear_index_job(dataset_id)
    background_tasks.add_task(_delete_collection_safe, dataset_id)
    return {"deleted": dataset_id}


@router.patch("/tables/{dataset_id}", include_in_schema=False)
def rename_table(dataset_id: int, body: RenameRequest, auth: dict = Depends(require_auth)):
    user_id = get_user_id_from_auth(auth)
    with SessionLocal() as db:
        dataset = _check_dataset_access(db, dataset_id, user_id)
        dataset.name = normalize_dataset_name_or_raise(body.name)
        db.commit()
        return {"name": dataset.name}
