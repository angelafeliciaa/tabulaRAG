"""Tests for app.qdrant_client – Qdrant helper functions (all mocked)."""

from unittest.mock import patch, MagicMock

from app.qdrant_client import (
    collection_name,
    _env_bool,
)


# ── collection_name ───────────────────────────────────────────────


def test_collection_name():
    assert collection_name(1) == "dataset_1"
    assert collection_name(42) == "dataset_42"


# ── _env_bool ─────────────────────────────────────────────────────


def test_env_bool_true_values():
    for val in ["1", "true", "yes", "on", "True", "YES", "ON"]:
        with patch.dict("os.environ", {"TEST_BOOL": val}):
            assert _env_bool("TEST_BOOL", "false") is True


def test_env_bool_false_values():
    for val in ["0", "false", "no", "off", "random"]:
        with patch.dict("os.environ", {"TEST_BOOL": val}):
            assert _env_bool("TEST_BOOL", "true") is False


def test_env_bool_default():
    assert _env_bool("NONEXISTENT_VAR_XYZ", "true") is True
    assert _env_bool("NONEXISTENT_VAR_XYZ", "false") is False


# ── ensure_collection ─────────────────────────────────────────────


def test_ensure_collection_creates_when_missing():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = False

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", False):
        from app.qdrant_client import ensure_collection
        ensure_collection(5)

    mock_client.create_collection.assert_called_once()
    args = mock_client.create_collection.call_args
    assert args.kwargs["collection_name"] == "dataset_5"


def test_ensure_collection_skips_when_exists():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = True

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import ensure_collection
        ensure_collection(5)

    mock_client.create_collection.assert_not_called()


def test_ensure_collection_bulk_mode():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = False

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", True):
        from app.qdrant_client import ensure_collection
        ensure_collection(5)

    mock_client.create_collection.assert_called_once()
    kwargs = mock_client.create_collection.call_args.kwargs
    assert "hnsw_config" in kwargs
    assert "optimizers_config" in kwargs


# ── prepare_collection_for_bulk_ingest ────────────────────────────


def test_prepare_collection_noop_when_not_bulk():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", False):
        from app.qdrant_client import prepare_collection_for_bulk_ingest
        prepare_collection_for_bulk_ingest(1)

    mock_client.update_collection.assert_not_called()


def test_prepare_collection_bulk_mode():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", True):
        from app.qdrant_client import prepare_collection_for_bulk_ingest
        prepare_collection_for_bulk_ingest(1)

    mock_client.update_collection.assert_called_once()


# ── finalize_collection_after_ingest ──────────────────────────────


def test_finalize_collection_bulk_mode():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", True), \
         patch("app.qdrant_client.ensure_text_index"):
        from app.qdrant_client import finalize_collection_after_ingest
        finalize_collection_after_ingest(1)

    mock_client.update_collection.assert_called_once()


def test_finalize_collection_non_bulk():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client), \
         patch("app.qdrant_client.QDRANT_BULK_INGEST_MODE", False), \
         patch("app.qdrant_client.ensure_text_index"):
        from app.qdrant_client import finalize_collection_after_ingest
        finalize_collection_after_ingest(1)

    mock_client.update_collection.assert_not_called()


# ── ensure_text_index ─────────────────────────────────────────────


def test_ensure_text_index_success():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import ensure_text_index
        ensure_text_index(3)

    mock_client.create_payload_index.assert_called_once()


def test_ensure_text_index_duplicate_ignored():
    mock_client = MagicMock()
    mock_client.create_payload_index.side_effect = Exception("already exists")

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import ensure_text_index
        ensure_text_index(3)  # should not raise


# ── get_collection_point_count ────────────────────────────────────


def test_get_point_count_existing():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = True
    mock_info = MagicMock()
    mock_info.points_count = 42
    mock_client.get_collection.return_value = mock_info

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import get_collection_point_count
        assert get_collection_point_count(1) == 42


def test_get_point_count_missing_collection():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = False

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import get_collection_point_count
        assert get_collection_point_count(1) == 0


# ── delete_collection ─────────────────────────────────────────────


def test_delete_collection_existing():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = True

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import delete_collection
        delete_collection(1)

    mock_client.delete_collection.assert_called_once_with("dataset_1")


def test_delete_collection_missing():
    mock_client = MagicMock()
    mock_client.collection_exists.return_value = False

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import delete_collection
        delete_collection(1)

    mock_client.delete_collection.assert_not_called()


# ── upsert_vectors ────────────────────────────────────────────────


def test_upsert_vectors_empty():
    mock_client = MagicMock()

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import upsert_vectors
        upsert_vectors(1, [])

    mock_client.upload_points.assert_not_called()


def test_upsert_vectors_success():
    mock_client = MagicMock()
    points = [MagicMock()]

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import upsert_vectors
        upsert_vectors(1, points)

    mock_client.upload_points.assert_called_once()


def test_upsert_vectors_fallback():
    mock_client = MagicMock()
    mock_client.upload_points.side_effect = Exception("upload failed")
    points = [MagicMock()]

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import upsert_vectors
        upsert_vectors(1, points)

    mock_client.upsert.assert_called_once()


# ── search_vectors ────────────────────────────────────────────────


def test_search_vectors():
    mock_hit = MagicMock()
    mock_hit.id = 0
    mock_hit.score = 0.95
    mock_hit.payload = {"text": "hello"}
    mock_result = MagicMock()
    mock_result.points = [mock_hit]

    mock_client = MagicMock()
    mock_client.query_points.return_value = mock_result

    with patch("app.qdrant_client.get_client", return_value=mock_client):
        from app.qdrant_client import search_vectors
        results = search_vectors(1, [0.1] * 384, limit=5)

    assert len(results) == 1
    assert results[0]["score"] == 0.95
