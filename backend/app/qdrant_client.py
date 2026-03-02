import logging
import os
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient, models

from app.embeddings import EMBEDDING_DIM

logger = logging.getLogger(__name__)

_client: Optional[QdrantClient] = None


def _env_bool(name: str, default: str) -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes", "on"}


QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
QDRANT_TIMEOUT_SECONDS = float(os.getenv("QDRANT_TIMEOUT_SECONDS", "60"))
QDRANT_UPSERT_BATCH_SIZE = max(64, int(os.getenv("QDRANT_UPSERT_BATCH_SIZE", "768")))
QDRANT_UPSERT_PARALLEL = max(1, int(os.getenv("QDRANT_UPSERT_PARALLEL", "2")))
QDRANT_UPSERT_WAIT = _env_bool("QDRANT_UPSERT_WAIT", "false")
QDRANT_BULK_INGEST_MODE = _env_bool("QDRANT_BULK_INGEST_MODE", "true")
QDRANT_BULK_HNSW_M = int(os.getenv("QDRANT_BULK_HNSW_M", "0"))
QDRANT_BULK_INDEXING_THRESHOLD = int(os.getenv("QDRANT_BULK_INDEXING_THRESHOLD", "0"))
QDRANT_FINAL_HNSW_M = int(os.getenv("QDRANT_FINAL_HNSW_M", "16"))
QDRANT_FINAL_INDEXING_THRESHOLD = int(
    os.getenv("QDRANT_FINAL_INDEXING_THRESHOLD", "20000")
)


def get_client() -> QdrantClient:
    """Lazy singleton for the Qdrant client."""
    global _client
    if _client is None:
        # Disable strict version checks so minor version skew doesn't slow startup/log spam.
        _client = QdrantClient(
            url=QDRANT_URL,
            check_compatibility=False,
            timeout=QDRANT_TIMEOUT_SECONDS,
        )
    return _client


def collection_name(dataset_id: int) -> str:
    return f"dataset_{dataset_id}"


def ensure_collection(dataset_id: int) -> None:
    """Create a collection for the dataset if it does not already exist."""
    client = get_client()
    name = collection_name(dataset_id)
    if not client.collection_exists(name):
        create_kwargs: Dict[str, Any] = {}
        if QDRANT_BULK_INGEST_MODE:
            create_kwargs["hnsw_config"] = models.HnswConfigDiff(m=QDRANT_BULK_HNSW_M)
            create_kwargs["optimizers_config"] = models.OptimizersConfigDiff(
                indexing_threshold=QDRANT_BULK_INDEXING_THRESHOLD
            )
        client.create_collection(
            collection_name=name,
            vectors_config=models.VectorParams(
                size=EMBEDDING_DIM,
                distance=models.Distance.COSINE,
            ),
            **create_kwargs,
        )


def prepare_collection_for_bulk_ingest(dataset_id: int) -> None:
    if not QDRANT_BULK_INGEST_MODE:
        return
    client = get_client()
    client.update_collection(
        collection_name=collection_name(dataset_id),
        hnsw_config=models.HnswConfigDiff(m=QDRANT_BULK_HNSW_M),
        optimizers_config=models.OptimizersConfigDiff(
            indexing_threshold=QDRANT_BULK_INDEXING_THRESHOLD
        ),
    )


def ensure_text_index(dataset_id: int) -> None:
    """Create a full-text index on the 'text' payload field (idempotent)."""
    client = get_client()
    name = collection_name(dataset_id)
    try:
        client.create_payload_index(
            collection_name=name,
            field_name="text",
            field_schema=models.TextIndexParams(
                type=models.TextIndexType.TEXT,
                tokenizer=models.TokenizerType.WORD,
                min_token_len=2,
                max_token_len=30,
                lowercase=True,
            ),
        )
    except Exception:
        # Index already exists — Qdrant raises if it's a duplicate
        logger.debug("Text index on '%s' already exists, skipping.", name)


def finalize_collection_after_ingest(dataset_id: int) -> None:
    client = get_client()
    if QDRANT_BULK_INGEST_MODE:
        client.update_collection(
            collection_name=collection_name(dataset_id),
            hnsw_config=models.HnswConfigDiff(m=QDRANT_FINAL_HNSW_M),
            optimizers_config=models.OptimizersConfigDiff(
                indexing_threshold=QDRANT_FINAL_INDEXING_THRESHOLD
            ),
        )
    ensure_text_index(dataset_id)


def get_collection_point_count(dataset_id: int) -> Optional[int]:
    client = get_client()
    name = collection_name(dataset_id)
    if not client.collection_exists(name):
        return 0
    info = client.get_collection(collection_name=name)
    return int(info.points_count or 0)


def delete_collection(dataset_id: int) -> None:
    """Idempotent delete of a dataset collection."""
    client = get_client()
    name = collection_name(dataset_id)
    if client.collection_exists(name):
        client.delete_collection(name)


def upsert_vectors(
    dataset_id: int,
    points: List[models.PointStruct],
    batch_size: int = QDRANT_UPSERT_BATCH_SIZE,
) -> None:
    """Upsert points into the dataset collection in batches."""
    if not points:
        return

    client = get_client()
    name = collection_name(dataset_id)
    try:
        client.upload_points(
            collection_name=name,
            points=points,
            batch_size=max(1, batch_size),
            parallel=QDRANT_UPSERT_PARALLEL,
            wait=QDRANT_UPSERT_WAIT,
            max_retries=5,
        )
        return
    except Exception:
        logger.debug(
            "Falling back to basic upsert for dataset '%s'.",
            dataset_id,
            exc_info=True,
        )

    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        client.upsert(
            collection_name=name,
            points=batch,
            wait=QDRANT_UPSERT_WAIT,
        )


def search_vectors(
    dataset_id: int,
    query_vector: List[float],
    limit: int = 10,
    query_filter: Optional[models.Filter] = None,
) -> List[Dict[str, Any]]:
    """Search the dataset collection and return results with scores and payloads."""
    client = get_client()
    name = collection_name(dataset_id)
    hits = client.query_points(
        collection_name=name,
        query=query_vector,
        limit=limit,
        query_filter=query_filter,
        with_payload=True,
    )
    results = []
    for hit in hits.points:
        results.append(
            {
                "id": hit.id,
                "score": hit.score,
                "payload": hit.payload,
            }
        )
    return results
