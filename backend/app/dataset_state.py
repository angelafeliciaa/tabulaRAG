from __future__ import annotations

from sqlalchemy import inspect, text, update

import app.db as app_db
from app.models import Dataset


def ensure_dataset_index_ready_column() -> None:
    inspector = inspect(app_db.engine)
    if "datasets" not in inspector.get_table_names():
        return

    column_names = {column["name"] for column in inspector.get_columns("datasets")}
    if "is_index_ready" in column_names:
        return

    with app_db.engine.begin() as conn:
        conn.execute(
            text(
                "ALTER TABLE datasets "
                "ADD COLUMN is_index_ready BOOLEAN NOT NULL DEFAULT FALSE"
            )
        )


def set_dataset_index_ready(dataset_id: int, is_ready: bool) -> None:
    with app_db.SessionLocal() as db:
        db.execute(
            update(Dataset)
            .where(Dataset.id == dataset_id)
            .values(is_index_ready=bool(is_ready))
        )
        db.commit()
