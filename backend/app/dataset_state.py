from __future__ import annotations

from sqlalchemy import inspect, text, update

import app.db as app_db
from app.models import Dataset


def ensure_dataset_columns_normalized_columns() -> None:
    """Migrate dataset_columns from name to original_name + normalized_name if needed."""
    inspector = inspect(app_db.engine)
    if "dataset_columns" not in inspector.get_table_names():
        return
    column_names = {c["name"] for c in inspector.get_columns("dataset_columns")}
    if "normalized_name" in column_names:
        return
    if "name" not in column_names:
        return

    dialect = app_db.engine.dialect.name
    if dialect == "postgresql":
        with app_db.engine.begin() as conn:
            conn.execute(text("ALTER TABLE dataset_columns ADD COLUMN original_name VARCHAR(512)"))
            conn.execute(text("ALTER TABLE dataset_columns ADD COLUMN normalized_name VARCHAR(255)"))
            conn.execute(
                text("UPDATE dataset_columns SET original_name = name, normalized_name = name")
            )
            conn.execute(
                text("ALTER TABLE dataset_columns ALTER COLUMN normalized_name SET NOT NULL")
            )
            conn.execute(
                text("ALTER TABLE dataset_columns DROP CONSTRAINT uq_dataset_columns_name")
            )
            conn.execute(text("ALTER TABLE dataset_columns DROP COLUMN name"))
            conn.execute(
                text(
                    "ALTER TABLE dataset_columns "
                    "ADD CONSTRAINT uq_dataset_columns_name UNIQUE (dataset_id, normalized_name)"
                )
            )
    elif dialect == "sqlite":
        with app_db.engine.begin() as conn:
            conn.execute(text("ALTER TABLE dataset_columns ADD COLUMN original_name VARCHAR(512)"))
            conn.execute(text("ALTER TABLE dataset_columns ADD COLUMN normalized_name VARCHAR(255)"))
            conn.execute(
                text("UPDATE dataset_columns SET original_name = name, normalized_name = name")
            )
            # SQLite: recreate table to add NOT NULL and drop name (no DROP CONSTRAINT/DROP COLUMN in older SQLite)
            conn.execute(text(
                "CREATE TABLE dataset_columns_new ("
                "id INTEGER NOT NULL PRIMARY KEY,"
                "dataset_id INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,"
                "column_index INTEGER NOT NULL,"
                "original_name VARCHAR(512),"
                "normalized_name VARCHAR(255) NOT NULL,"
                "UNIQUE (dataset_id, column_index),"
                "UNIQUE (dataset_id, normalized_name)"
                ")"
            ))
            conn.execute(
                text(
                    "INSERT INTO dataset_columns_new (id, dataset_id, column_index, original_name, normalized_name) "
                    "SELECT id, dataset_id, column_index, original_name, normalized_name FROM dataset_columns"
                )
            )
            conn.execute(text("DROP TABLE dataset_columns"))
            conn.execute(text("ALTER TABLE dataset_columns_new RENAME TO dataset_columns"))
            conn.execute(
                text(
                    "CREATE INDEX ix_dataset_columns_dataset_id ON dataset_columns (dataset_id)"
                )
            )


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
