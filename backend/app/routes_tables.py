from typing import List
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Dataset, DatasetColumn, DatasetRow

router = APIRouter()


class RenameRequest(BaseModel):
    name: str


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
            "rows": [{"row_index": r.row_index, "data": r.row_data} for r in rows],
            "columns": columns,
        }


@router.delete("/tables/{dataset_id}")
def delete_table(dataset_id: int):
    with SessionLocal() as db:
        dataset = db.get(Dataset, dataset_id)
        if not dataset:
            raise HTTPException(status_code=404, detail="Table not found")
        db.delete(
            dataset
        )  # SQLAlchemy handles deleting the related DatasetColumn and DatasetRow records automatically
        db.commit()
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
