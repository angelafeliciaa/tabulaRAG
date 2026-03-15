import os
from typing import Dict, List, Optional

from fastembed import TextEmbedding
from app.normalization import is_internal_key

_model: Optional[TextEmbedding] = None

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
EMBEDDING_DIM = 384
EMBEDDING_BATCH_SIZE = max(
    32,
    int(os.getenv("EMBEDDING_BATCH_SIZE", os.getenv("EMBED_BATCH_SIZE", "512"))),
)
EMBEDDING_MODEL_THREADS = max(
    1,
    int(
        os.getenv(
            "EMBEDDING_MODEL_THREADS",
            os.getenv("EMBED_MODEL_THREADS", "4"),
        )
    ),
)
_EMBEDDING_PARALLEL_RAW = os.getenv("EMBEDDING_PARALLEL")
EMBEDDING_PARALLEL = (
    int(_EMBEDDING_PARALLEL_RAW) if _EMBEDDING_PARALLEL_RAW not in (None, "") else None
)


def get_model() -> TextEmbedding:
    """Lazy singleton for the FastEmbed model."""
    global _model
    if _model is None:
        _model = TextEmbedding(
            model_name=MODEL_NAME,
            threads=EMBEDDING_MODEL_THREADS,
        )
    return _model


def row_to_text(row_data: Dict[str, object]) -> str:
    """Serialize a row dict to a pipe-separated string for embedding.

    Example: {"name": "Alice", "age": "30"} -> "name: Alice | age: 30"
    Skips keys whose values are None or empty string. Uses normalized value per column.
    """
    from app.normalization import get_normalized_value

    parts = []
    for key in row_data:
        if is_internal_key(str(key)):
            continue
        value = get_normalized_value(row_data, key)
        if value is None or value == "":
            continue
        parts.append(f"{key}: {value}")
    return " | ".join(parts)


def embed_texts(texts: List[str]) -> List[List[float]]:
    """Batch-embed a list of texts using FastEmbed.

    Returns a list of embedding vectors (each a list of floats).
    """
    if not texts:
        return []
    model = get_model()
    embed_kwargs = {"batch_size": EMBEDDING_BATCH_SIZE}
    if EMBEDDING_PARALLEL is not None:
        embed_kwargs["parallel"] = EMBEDDING_PARALLEL
    embeddings = list(model.embed(texts, **embed_kwargs))
    return [e.tolist() for e in embeddings]
