import csv
import io
import json
import os
from typing import Iterable, List, Tuple
import unicodedata

import httpx
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text, select
from contextlib import asynccontextmanager

from app.db import SessionLocal, engine
from app.indexing import index_dataset
from app.models import Base, Dataset, DatasetColumn, DatasetRow
from app.mcp_server import mcp
from app.routes_tables import router as tables_router
from app.routes_query import router as query_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    try:
        from app.embeddings import get_model
        get_model()
    except Exception:
        pass

    async with mcp.session_manager.run():
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


def _normalize_value(value: str) -> str | None:
    if value is None:
        return None
    value = unicodedata.normalize("NFC", value)
    value = value.replace("\xa0", " ")
    value = " ".join(value.split())  # collapse extra whitespace
    if value.lower() in NULL_VALUES:
        return None
    return value


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


@app.post("/ingest")
def ingest_table(
    file: UploadFile = File(...),
    dataset_name: str | None = Form(None),
    has_header: bool = Form(True),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename.")
    validate_filename(file.filename)

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
    # Insert rows using SQLAlchemy ORM for compatibility with both SQLite and PostgreSQL
    try:
        with SessionLocal() as db:
            for row_index, row in enumerate(rows_iter):
                row_obj = {
                    headers[i]: _normalize_value(row[i] if i < len(row) else None)
                    for i in range(len(headers))
                }
                dataset_row = DatasetRow(
                    dataset_id=dataset_id,
                    row_index=row_index,
                    row_data=json.dumps(row_obj),
                )
                db.add(dataset_row)
                row_count += 1
            db.commit()
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

    # Index dataset in Qdrant for vector search (non-blocking: failure shouldn't fail ingestion)
    try:
        index_dataset(dataset_id)
    except Exception:
        pass

    return {
        "dataset_id": dataset_id,
        "name": dataset_name_value,
        "rows": row_count,
        "columns": len(headers),
        "delimiter": dataset_delimiter,
        "has_header": dataset_has_header,
    }
