import os
from typing import Dict, List, Optional

from fastembed import TextEmbedding

_model: Optional[TextEmbedding] = None

MODEL_NAME = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")
EMBEDDING_DIM = 384


def get_model() -> TextEmbedding:
    """Lazy singleton for the FastEmbed model."""
    global _model
    if _model is None:
        _model = TextEmbedding(model_name=MODEL_NAME)
    return _model


def row_to_text(row_data: Dict[str, object]) -> str:
    """Serialize a row dict to a pipe-separated string for embedding.

    Example: {"name": "Alice", "age": "30"} -> "name: Alice | age: 30"
    Skips keys whose values are None or empty string.
    """
    parts = []
    for key, value in row_data.items():
        if value is None or value == "":
            continue
        parts.append(f"{key}: {value}")
    return " | ".join(parts)


def embed_texts(texts: List[str]) -> List[List[float]]:
    """Batch-embed a list of texts using FastEmbed.

    Returns a list of embedding vectors (each a list of floats).
    """
    model = get_model()
    embeddings = list(model.embed(texts))
    return [e.tolist() for e in embeddings]
