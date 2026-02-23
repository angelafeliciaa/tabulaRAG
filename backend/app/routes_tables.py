import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from sqlalchemy import delete, select

from app.db import SessionLocal
from app.index_jobs import clear_index_job, get_index_jobs
from app.models import Dataset, DatasetColumn, DatasetRow
from app.qdrant_client import delete_collection

router = APIRouter()


class RenameRequest(BaseModel):
    name: str


def _normalize_row_data(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed: Any = json.loads(raw)
            if isinstance(parsed, str):
                parsed = json.loads(parsed)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _delete_collection_safe(dataset_id: int) -> None:
    try:
        delete_collection(dataset_id)
    except Exception:
        # Collection cleanup is best-effort and should not block API delete.
        pass


@router.get("/tables")
def list_tables():
    with SessionLocal() as db:
        datasets = (
            db.execute(select(Dataset).order_by(Dataset.id.desc())).scalars().all()
        )
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


@router.get("/tables/index-status")
def list_index_status(dataset_id: Optional[List[int]] = Query(default=None)):
    with SessionLocal() as db:
        query = select(Dataset.id, Dataset.row_count)
        if dataset_id:
            query = query.where(Dataset.id.in_(dataset_id))
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


@router.get("/tables/{dataset_id}/slice")
def get_table_slice(
    dataset_id: int,
    offset: int = 0,
    limit: int = 30,
):
    with SessionLocal() as db:
        dataset = db.get(Dataset, dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Table not found")

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
                select(DatasetColumn.name)
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
                {"row_index": r.row_index, "data": _normalize_row_data(r.row_data)}
                for r in rows
            ],
            "columns": columns,
        }


@router.delete("/tables/{dataset_id}")
def delete_table(dataset_id: int, background_tasks: BackgroundTasks):
    with SessionLocal() as db:
        exists = db.execute(
            select(Dataset.id).where(Dataset.id == dataset_id)
        ).first()
        if not exists:
            raise HTTPException(status_code=404, detail="Table not found")
        # Use direct SQL deletes to avoid expensive ORM cascade object loading.
        db.execute(delete(DatasetRow).where(DatasetRow.dataset_id == dataset_id))
        db.execute(delete(DatasetColumn).where(DatasetColumn.dataset_id == dataset_id))
        db.execute(delete(Dataset).where(Dataset.id == dataset_id))
        db.commit()
    clear_index_job(dataset_id)
    background_tasks.add_task(_delete_collection_safe, dataset_id)
    return {"deleted": dataset_id}


@router.patch("/tables/{dataset_id}")
def rename_table(dataset_id: int, body: RenameRequest):
    with SessionLocal() as db:
        dataset = db.get(Dataset, dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Table not found")
        if not body.name.strip():
            raise HTTPException(status_code=400, detail="Name cannot be empty")
        dataset.name = body.name.strip()
        db.commit()
        return {"name": dataset.name}
