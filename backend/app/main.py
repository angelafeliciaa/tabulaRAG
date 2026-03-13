import csv
import io
import json
import logging
import os
from typing import Iterable, List, Optional, Tuple
import httpx
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import insert, text, select
from contextlib import asynccontextmanager
from app.db import SessionLocal, engine
from app.dataset_state import (
    ensure_dataset_columns_normalized_columns,
    ensure_dataset_index_ready_column,
    set_dataset_index_ready,
)
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
from app.qdrant_client import get_collection_point_count
from fastapi_mcp import FastApiMCP
from app.routes_tables import router as tables_router
from app.routes_query import router as query_router
from app.normalization import (
    infer_date_formats_for_columns,
    normalize_headers,
    normalize_row_obj,
)
from app.name_guard import normalize_dataset_name_or_raise
from app.auth import require_auth, exchange_github_code, create_jwt, GITHUB_CLIENT_ID


logger = logging.getLogger(__name__)
_index_worker: IndexWorker | None = None
INDEX_WORKER_CONCURRENCY = max(1, int(os.getenv("INDEX_WORKER_CONCURRENCY", "4")))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _index_worker
    Base.metadata.create_all(bind=engine)
    ensure_dataset_columns_normalized_columns()
    ensure_dataset_index_ready_column()
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

    yield


app = FastAPI(title="TabulaRAG API", lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(tables_router)
app.include_router(query_router)


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


@app.get("/health/deps", include_in_schema=False)
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


# checks if file is a csv or tsv based on file extension, raises HTTPException if not
def validate_filename(filename: str) -> None:
    if not filename.lower().endswith((".csv", ".tsv")):
        raise HTTPException(
            status_code=400, detail="File must have a .csv or .tsv extension."
        )


ROW_INSERT_BATCH_SIZE = int(os.getenv("ROW_INSERT_BATCH_SIZE", "20000"))
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(50 * 1024 * 1024)))
FILE_SNIFF_BYTES = int(os.getenv("FILE_SNIFF_BYTES", "65536"))
BLOCKED_UPLOAD_CONTENT_TYPE_PREFIXES = (
    "application/pdf",
    "application/zip",
    "application/x-zip",
    "application/x-rar",
    "application/octet-stream",
    "image/",
    "audio/",
    "video/",
)


def _detect_delimiter(filename: str | None) -> str:
    if filename and filename.lower().endswith(".tsv"):
        return "\t"
    if filename and filename.lower().endswith(".csv"):
        return ","
    return ","


def _validate_upload_content(upload: UploadFile) -> None:
    content_type = (upload.content_type or "").strip().lower()
    if any(content_type.startswith(prefix) for prefix in BLOCKED_UPLOAD_CONTENT_TYPE_PREFIXES):
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported content type '{content_type}' for CSV/TSV upload.",
        )

    # Enforce a hard file-size cap to reduce parser/DB abuse.
    upload.file.seek(0, io.SEEK_END)
    size = upload.file.tell()
    upload.file.seek(0)
    if size > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"File is too large. Maximum size is {MAX_UPLOAD_BYTES} bytes.",
        )

    head = upload.file.read(FILE_SNIFF_BYTES)
    upload.file.seek(0)

    # Empty-file handling remains in _iter_rows for existing behavior/messages.
    if not head:
        return

    if b"\x00" in head:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file appears to be binary. Please upload a valid CSV/TSV file.",
        )

    try:
        head.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(
            status_code=400,
            detail="Uploaded file must be UTF-8 encoded text.",
        ) from exc


def _iter_rows(
    upload: UploadFile,
    has_header: bool,
) -> Tuple[List[str], List[str], Iterable[List[str]], str]:
    """Return (raw_headers, normalized_headers, rows_iter, delimiter)."""
    validate_filename(upload.filename or "")
    _validate_upload_content(upload)
    detected_delimiter = _detect_delimiter(upload.filename)
    if detected_delimiter not in [",", "\t"]:
        raise HTTPException(status_code=400, detail="Delimiter must be comma or tab.")

    text_stream = io.TextIOWrapper(upload.file, encoding="utf-8-sig", newline="")
    reader = csv.reader(text_stream, delimiter=detected_delimiter)

    try:
        first_row = next(reader)
    except StopIteration:
        raise HTTPException(status_code=400, detail="Empty file.")

    if has_header:
        raw_headers = [str(h) if h is not None else "" for i, h in enumerate(first_row)]
        normalized_headers = normalize_headers(first_row)
        rows_iter = reader
    else:
        raw_headers = [f"col_{i + 1}" for i in range(len(first_row))]
        normalized_headers = normalize_headers(raw_headers)

        def row_iter() -> Iterable[List[str]]:
            yield first_row
            yield from reader

        rows_iter = row_iter()

    return raw_headers, normalized_headers, rows_iter, detected_delimiter


def _build_row_obj(
    normalized_headers: List[str],
    row: List[str],
    date_format_by_column: Optional[dict] = None,
) -> dict:
    return normalize_row_obj(
        normalized_headers,
        row,
        store_original=True,
        date_format_by_column=date_format_by_column,
    )


def _insert_rows_postgres_copy(
    dataset_id: int,
    headers: List[str],
    rows_iter: Iterable[List[str]],
    date_format_by_column: Optional[dict] = None,
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
                    row_obj = _build_row_obj(headers, row, date_format_by_column)
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
    date_format_by_column: Optional[dict] = None,
) -> int:
    """Fallback ingestion path (works for SQLite and non-Postgres)."""
    row_count = 0
    with SessionLocal() as db:
        batch_rows = []
        for row_index, row in enumerate(rows_iter):
            row_obj = _build_row_obj(headers, row, date_format_by_column)
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
        set_dataset_index_ready(dataset_id, True)
        mark_index_job_ready(dataset_id, total_rows)
    except Exception as exc:
        logger.exception("Indexing failed for dataset_id=%s", dataset_id)
        set_dataset_index_ready(dataset_id, False)
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
            select(Dataset.id, Dataset.row_count, Dataset.is_index_ready).order_by(
                Dataset.id.asc()
            )
        ).all()

    for dataset_id_raw, row_count_raw, is_index_ready_raw in datasets:
        dataset_id = int(dataset_id_raw)
        row_count = int(row_count_raw or 0)
        is_index_ready = bool(is_index_ready_raw)
        if row_count <= 0:
            if not is_index_ready:
                set_dataset_index_ready(dataset_id, True)
            continue

        try:
            point_count = get_collection_point_count(dataset_id)
        except Exception:
            point_count = None

        if point_count is not None and int(point_count) >= row_count:
            if not is_index_ready:
                set_dataset_index_ready(dataset_id, True)
            continue
        if point_count is None and is_index_ready:
            continue

        if is_index_ready:
            set_dataset_index_ready(dataset_id, False)
        queue_index_job(dataset_id, row_count)
        _index_worker.enqueue(dataset_id, row_count)


@app.post("/auth/verify", include_in_schema=False)
def auth_verify(credentials: None = Depends(require_auth)) -> dict:
    return {"valid": True}


@app.get("/auth/github", include_in_schema=False)
def auth_github_redirect():
    if not GITHUB_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GitHub OAuth not configured")
    return {"client_id": GITHUB_CLIENT_ID}


@app.post("/auth/github/callback", include_in_schema=False)
async def auth_github_callback(body: dict):
    code = body.get("code")
    if not code:
        raise HTTPException(status_code=400, detail="Missing code parameter")
    github_user = await exchange_github_code(code)
    token = create_jwt(github_user)
    return {
        "token": token,
        "user": {
            "login": github_user["login"],
            "name": github_user.get("name") or github_user["login"],
            "avatar_url": github_user.get("avatar_url", ""),
        },
    }


@app.post("/ingest", include_in_schema=False)
def ingest_table(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(None),
    has_header: bool = Form(True),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")
    validate_filename(file.filename)

    raw_headers, normalized_headers, rows_iter, detected_delimiter = _iter_rows(file, has_header)

    dataset_display_name = normalize_dataset_name_or_raise(
        dataset_name or os.path.splitext(file.filename)[0]
    )

    with SessionLocal() as db:
        dataset = Dataset(
            name=dataset_display_name,
            source_filename=file.filename,
            delimiter=detected_delimiter,
            has_header=has_header,
            column_count=len(normalized_headers),
            is_index_ready=False,
        )
        db.add(dataset)
        db.flush()

        db.add_all(
            [
                DatasetColumn(
                    dataset_id=dataset.id,
                    column_index=i,
                    original_name=raw_headers[i] or None,
                    normalized_name=normalized_headers[i],
                )
                for i in range(len(normalized_headers))
            ]
        )
        db.commit()
        dataset_id = dataset.id
        dataset_name_value = dataset.name
        dataset_delimiter = dataset.delimiter
        dataset_has_header = dataset.has_header

    rows_list = list(rows_iter)
    date_format_by_column = infer_date_formats_for_columns(normalized_headers, rows_list)
    row_count = 0
    try:
        if engine.dialect.name == "postgresql":
            row_count = _insert_rows_postgres_copy(
                dataset_id, normalized_headers, rows_list, date_format_by_column
            )
        else:
            row_count = _insert_rows_batched(
                dataset_id, normalized_headers, rows_list, date_format_by_column
            )
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

    if row_count <= 0:
        set_dataset_index_ready(dataset_id, True)
        mark_index_job_ready(dataset_id, row_count)
    else:
        # Start vector indexing after response so uploads don't block on embedding/Qdrant upserts.
        queue_index_job(dataset_id, row_count)
        _enqueue_index_job(dataset_id, row_count)

    return {
        "dataset_id": dataset_id,
        "name": dataset_name_value,
        "rows": row_count,
        "columns": len(normalized_headers),
        "delimiter": dataset_delimiter,
        "has_header": dataset_has_header,
    }



mcp = FastApiMCP(
    app,
    name="TabulaRAG",
)
mcp.mount_http()
