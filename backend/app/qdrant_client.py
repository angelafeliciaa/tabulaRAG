import os
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient, models

from app.embeddings import EMBEDDING_DIM

_client: Optional[QdrantClient] = None

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")


def get_client() -> QdrantClient:
    """Lazy singleton for the Qdrant client."""
    global _client
    if _client is None:
        _client = QdrantClient(url=QDRANT_URL)
    return _client


def collection_name(dataset_id: int) -> str:
    return f"dataset_{dataset_id}"


def ensure_collection(dataset_id: int) -> None:
    """Create a collection for the dataset if it does not already exist."""
    client = get_client()
    name = collection_name(dataset_id)
    if not client.collection_exists(name):
        client.create_collection(
            collection_name=name,
            vectors_config=models.VectorParams(
                size=EMBEDDING_DIM,
                distance=models.Distance.COSINE,
            ),
        )


def delete_collection(dataset_id: int) -> None:
    """Idempotent delete of a dataset collection."""
    client = get_client()
    name = collection_name(dataset_id)
    if client.collection_exists(name):
        client.delete_collection(name)


def upsert_vectors(
    dataset_id: int,
    points: List[models.PointStruct],
    batch_size: int = 100,
) -> None:
    """Upsert points into the dataset collection in batches."""
    client = get_client()
    name = collection_name(dataset_id)
    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        client.upsert(collection_name=name, points=batch)


def search_vectors(
    dataset_id: int,
    query_vector: List[float],
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Search the dataset collection and return results with scores and payloads."""
    client = get_client()
    name = collection_name(dataset_id)
    hits = client.query_points(
        collection_name=name,
        query=query_vector,
        limit=limit,
        with_payload=True,
    )
    results = []
    for hit in hits.points:
        results.append({
            "id": hit.id,
            "score": hit.score,
            "payload": hit.payload,
        })
    return results
