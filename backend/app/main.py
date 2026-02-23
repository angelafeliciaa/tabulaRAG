import csv
import io
import json
import os
from typing import Iterable, List, Tuple
import unicodedata
import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import insert, text, select
from contextlib import asynccontextmanager
from app.db import SessionLocal, engine
from app.index_jobs import (
    mark_index_job_error,
    mark_index_job_ready,
    queue_index_job,
    start_index_job,
    update_index_job,
)
from app.indexing import index_dataset
from app.index_worker import IndexWorker
from app.models import Base, Dataset, DatasetColumn, DatasetRow
from app.mcp_server import mcp
from app.qdrant_client import get_collection_point_count
from app.routes_tables import router as tables_router
from app.routes_query import router as query_router


_index_worker: IndexWorker | None = None
INDEX_WORKER_CONCURRENCY = max(1, int(os.getenv("INDEX_WORKER_CONCURRENCY", "4")))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _index_worker
    Base.metadata.create_all(bind=engine)
    try:
        from app.embeddings import get_model
        get_model()
    except Exception:
        pass

    _index_worker = IndexWorker(
        _index_dataset_safe,
        worker_count=INDEX_WORKER_CONCURRENCY,
    )
    _index_worker.start()
    _resume_incomplete_index_jobs()

    async with mcp.session_manager.run():
        try:
            yield
        finally:
            if _index_worker is not None:
                _index_worker.stop()


app = FastAPI(title="TabulaRAG API", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(tables_router)
app.include_router(query_router)
app.mount("/mcp", mcp.streamable_http_app())


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/deps")
def health_deps():
    postgres_ok = False
    qdrant_ok = False

    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        postgres_ok = True
    except Exception:
        postgres_ok = False

    qdrant_url = os.getenv("QDRANT_URL", "http://qdrant:6333")
    try:
        response = httpx.get(f"{qdrant_url}/healthz", timeout=2.0)
        qdrant_ok = response.status_code == 200
    except Exception:
        qdrant_ok = False

    all_ok = postgres_ok and qdrant_ok
    return {
        "status": "ok" if all_ok else "degraded",
        "postgres": "ok" if postgres_ok else "down",
        "qdrant": "ok" if qdrant_ok else "down",
    }


@app.get("/mcp-status")
def mcp_status():
    return {"status": "ok", "endpoint": "/mcp"}


# checks if file is a csv or tsv based on file extension, raises HTTPException if not
def validate_filename(filename: str) -> None:
    if not filename.lower().endswith((".csv", ".tsv")):
        raise HTTPException(
            status_code=400, detail="File must have a .csv or .tsv extension."
        )


# normalizes header names by stripping whitespace, replacing empty names with col_{index}, and ensuring uniqueness by appending _{count} to duplicates
def _normalize_headers(headers: List[str]) -> List[str]:
    seen = {}
    normalized = []
    for idx, header in enumerate(headers):
        base = (header or "").strip()
        if not base:
            base = f"col_{idx + 1}"
        key = base
        if key in seen:
            seen[key] += 1
            key = f"{base}_{seen[base]}"
        else:
            seen[key] = 1
        normalized.append(key)
    return normalized


NULL_VALUES = {"null", "none", "na", "n/a", "nan", "-", ""}
ROW_INSERT_BATCH_SIZE = int(os.getenv("ROW_INSERT_BATCH_SIZE", "20000"))
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "100"))
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024


def _normalize_value(value: str) -> str | None:
    if value is None:
        return None
    value = unicodedata.normalize("NFC", value)
    value = value.replace("\xa0", " ")
    value = " ".join(value.split())  # collapse extra whitespace
    if value.lower() in NULL_VALUES:
        return None
    return value


def validate_upload_size(upload: UploadFile) -> None:
    try:
        upload.file.seek(0, os.SEEK_END)
        size_bytes = upload.file.tell()
        upload.file.seek(0)
    except Exception:
        # Fallback to best effort if stream size is unavailable.
        return

    if size_bytes > MAX_UPLOAD_SIZE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File size exceeds {MAX_UPLOAD_SIZE_MB} MB limit.",
        )


def _detect_delimiter(filename: str | None) -> str:
    if filename and filename.lower().endswith(".tsv"):
        return "\t"
    if filename and filename.lower().endswith(".csv"):
        return ","
    return ","


def _iter_rows(
    upload: UploadFile,
    has_header: bool,
) -> Tuple[List[str], Iterable[List[str]], str]:
    validate_filename(upload.filename or "")
    detected_delimiter = _detect_delimiter(upload.filename)
    if detected_delimiter not in [",", "\t"]:
        raise HTTPException(status_code=400, detail="Delimiter must be comma or tab.")

    # Use TextIOWrapper to read the uploaded file as text with UTF-8 encoding, which allows csv.reader to process it correctly.
    text_stream = io.TextIOWrapper(upload.file, encoding="utf-8-sig", newline="")
    reader = csv.reader(text_stream, delimiter=detected_delimiter)

    try:
        first_row = next(
            reader
        )  # gets the first row of the file to determine headers and column count
    except StopIteration:
        raise HTTPException(status_code=400, detail="Empty file.")

    if has_header:
        headers = _normalize_headers(first_row)
        rows_iter = reader
    else:
        headers = _normalize_headers([f"col_{i + 1}" for i in range(len(first_row))])

        def row_iter() -> Iterable[List[str]]:
            yield first_row
            yield from reader

        rows_iter = row_iter()

    return headers, rows_iter, detected_delimiter


def _build_row_obj(headers: List[str], row: List[str]) -> dict:
    return {
        headers[i]: _normalize_value(row[i] if i < len(row) else None)
        for i in range(len(headers))
    }


def _insert_rows_postgres_copy(
    dataset_id: int,
    headers: List[str],
    rows_iter: Iterable[List[str]],
) -> int:
    """Fast path for PostgreSQL ingestion using COPY."""
    row_count = 0
    raw_connection = engine.raw_connection()
    try:
        with raw_connection.cursor() as cursor:
            with cursor.copy(
                "COPY dataset_rows (dataset_id, row_index, row_data) FROM STDIN"
            ) as copy:
                for row_index, row in enumerate(rows_iter):
                    row_obj = _build_row_obj(headers, row)
                    copy.write_row(
                        (
                            dataset_id,
                            row_index,
                            json.dumps(row_obj, ensure_ascii=False),
                        )
                    )
                    row_count += 1
        raw_connection.commit()
        return row_count
    except Exception:
        raw_connection.rollback()
        raise
    finally:
        raw_connection.close()


def _insert_rows_batched(
    dataset_id: int,
    headers: List[str],
    rows_iter: Iterable[List[str]],
) -> int:
    """Fallback ingestion path (works for SQLite and non-Postgres)."""
    row_count = 0
    with SessionLocal() as db:
        batch_rows = []
        for row_index, row in enumerate(rows_iter):
            row_obj = _build_row_obj(headers, row)
            batch_rows.append(
                {
                    "dataset_id": dataset_id,
                    "row_index": row_index,
                    "row_data": row_obj,
                }
            )
            if len(batch_rows) >= ROW_INSERT_BATCH_SIZE:
                db.execute(insert(DatasetRow), batch_rows)
                row_count += len(batch_rows)
                batch_rows.clear()

        if batch_rows:
            db.execute(insert(DatasetRow), batch_rows)
            row_count += len(batch_rows)
        db.commit()
    return row_count


def _index_dataset_safe(dataset_id: int, total_rows: int) -> None:
    start_index_job(dataset_id, total_rows)

    try:
        index_dataset(
            dataset_id,
            progress_callback=lambda processed, total: update_index_job(
                dataset_id, processed, total
            ),
            expected_total_rows=total_rows,
        )
        mark_index_job_ready(dataset_id, total_rows)
    except Exception as exc:
        mark_index_job_error(dataset_id, total_rows, f"Indexing failed: {exc}")


def _enqueue_index_job(dataset_id: int, total_rows: int) -> None:
    if _index_worker is None:
        _index_dataset_safe(dataset_id, total_rows)
        return
    _index_worker.enqueue(dataset_id, total_rows)


def _resume_incomplete_index_jobs() -> None:
    if _index_worker is None:
        return

    with SessionLocal() as db:
        datasets = db.execute(
            select(Dataset.id, Dataset.row_count)
            .where(Dataset.row_count > 0)
            .order_by(Dataset.id.asc())
        ).all()

    for dataset_id_raw, row_count_raw in datasets:
        dataset_id = int(dataset_id_raw)
        row_count = int(row_count_raw or 0)
        if row_count <= 0:
            continue

        try:
            point_count = get_collection_point_count(dataset_id)
        except Exception:
            point_count = None

        if point_count is not None and int(point_count) >= row_count:
            continue

        queue_index_job(dataset_id, row_count)
        _index_worker.enqueue(dataset_id, row_count)


@app.post("/ingest")
def ingest_table(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(None),
    has_header: bool = Form(True),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")
    validate_filename(file.filename)
    validate_upload_size(file)

    headers, rows_iter, detected_delimiter = _iter_rows(file, has_header)

    dataset_display_name = dataset_name or os.path.splitext(file.filename)[0]

    with SessionLocal() as db:
        dataset = Dataset(
            name=dataset_display_name,
            source_filename=file.filename,
            delimiter=detected_delimiter,
            has_header=has_header,
            column_count=len(headers),
        )
        db.add(
            dataset
        )  # adds the new dataset to the session, which assigns it an ID after flush() is called
        db.flush()  # sends it to the database to get the generated ID, but does not commit yet so it can be rolled back if needed

        db.add_all(
            [
                DatasetColumn(dataset_id=dataset.id, column_index=i, name=col_name)
                for i, col_name in enumerate(headers)
            ]
        )  # creates a DatasetColumn object for each header and adds them to the session, associating them with the dataset by dataset_id
        db.commit()  # commits the dataset to the database
        dataset_id = dataset.id
        dataset_name_value = dataset.name
        dataset_delimiter = dataset.delimiter
        dataset_has_header = dataset.has_header

    row_count = 0
    try:
        if engine.dialect.name == "postgresql":
            row_count = _insert_rows_postgres_copy(dataset_id, headers, rows_iter)
        else:
            row_count = _insert_rows_batched(dataset_id, headers, rows_iter)
    except Exception as exc:
        with SessionLocal() as db:
            db.execute(text("DELETE FROM datasets WHERE id = :id"), {"id": dataset_id})
            db.commit()
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc
    # update the row count for the dataset after all rows have been inserted, using a raw SQL UPDATE statement for efficiency
    # look at database in __init__.py
    with SessionLocal() as db:
        db.execute(
            text("UPDATE datasets SET row_count = :row_count WHERE id = :id"),
            {"row_count": row_count, "id": dataset_id},
        )
        db.commit()

    # Start vector indexing after response so uploads don't block on embedding/Qdrant upserts.
    queue_index_job(dataset_id, row_count)
    _enqueue_index_job(dataset_id, row_count)

    return {
        "dataset_id": dataset_id,
        "name": dataset_name_value,
        "rows": row_count,
        "columns": len(headers),
        "delimiter": dataset_delimiter,
        "has_header": dataset_has_header,
    }
