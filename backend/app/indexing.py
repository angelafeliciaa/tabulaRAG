import json
from typing import Callable, Dict, List, Optional

from qdrant_client import models
from sqlalchemy import text

from app.db import SessionLocal
from app.embeddings import embed_texts, row_to_text
from app.qdrant_client import ensure_collection, ensure_text_index, upsert_vectors

EMBED_BATCH_SIZE = 256


def index_dataset(
    dataset_id: int,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> None:
    """Read all rows for a dataset from PG, embed them, and upsert into Qdrant.

    Uses row_index as the Qdrant point ID (unique per collection).
    Stores row_data and the serialized text in the Qdrant payload so that
    search results can be returned without an extra PG round-trip.
    """
    ensure_collection(dataset_id)
    ensure_text_index(dataset_id)

    # Fetch all rows from Postgres
    with SessionLocal() as db:
        result = db.execute(
            text(
                "SELECT row_index, row_data FROM dataset_rows "
                "WHERE dataset_id = :dataset_id ORDER BY row_index"
            ),
            {"dataset_id": dataset_id},
        )
        rows = result.fetchall()

    if not rows:
        return

    # Prepare texts and metadata
    row_indices: List[int] = []
    texts: List[str] = []
    row_datas: List[Dict] = []

    for row in rows:
        row_index = row[0]
        raw = row[1]
        # Handle potential double-serialization (json.dumps into JSON column)
        row_data = json.loads(raw) if isinstance(raw, str) else raw
        if isinstance(row_data, str):
            row_data = json.loads(row_data)
        serialized = row_to_text(row_data)
        if not serialized:
            continue
        row_indices.append(row_index)
        texts.append(serialized)
        row_datas.append(row_data)

    total_rows = len(texts)
    processed_rows = 0
    if progress_callback:
        progress_callback(processed_rows, total_rows)

    # Embed and upsert in batches
    for i in range(0, len(texts), EMBED_BATCH_SIZE):
        batch_texts = texts[i : i + EMBED_BATCH_SIZE]
        batch_indices = row_indices[i : i + EMBED_BATCH_SIZE]
        batch_row_datas = row_datas[i : i + EMBED_BATCH_SIZE]

        vectors = embed_texts(batch_texts)

        points = [
            models.PointStruct(
                id=idx,
                vector=vec,
                payload={"row_data": rd, "text": txt},
            )
            for idx, vec, rd, txt in zip(
                batch_indices, vectors, batch_row_datas, batch_texts
            )
        ]
        upsert_vectors(dataset_id, points)
        processed_rows += len(batch_texts)
        if progress_callback:
            progress_callback(processed_rows, total_rows)
