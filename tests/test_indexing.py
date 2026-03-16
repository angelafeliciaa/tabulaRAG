"""Tests for app.indexing – _deserialize_row_data, _effective_embed_batch_size, and index_dataset."""

import json
from unittest.mock import patch, MagicMock, call

import pytest

from app.indexing import (
    _deserialize_row_data,
    _effective_embed_batch_size,
    EMBED_BATCH_SIZE,
    INDEX_EMBED_MIN_BATCH_SIZE,
)


# ── _deserialize_row_data ─────────────────────────────────────────


def test_deserialize_dict():
    assert _deserialize_row_data({"a": 1}) == {"a": 1}


def test_deserialize_json_string():
    assert _deserialize_row_data('{"a": 1}') == {"a": 1}


def test_deserialize_double_encoded_json():
    inner = json.dumps({"a": 1})
    outer = json.dumps(inner)
    assert _deserialize_row_data(outer) == {"a": 1}


def test_deserialize_non_dict_returns_empty():
    assert _deserialize_row_data(42) == {}
    assert _deserialize_row_data(None) == {}
    assert _deserialize_row_data([1, 2]) == {}


def test_deserialize_string_non_json():
    # Non-JSON strings raise JSONDecodeError, which is not caught by _deserialize_row_data
    import json
    with pytest.raises(json.JSONDecodeError):
        _deserialize_row_data("not json")


def test_deserialize_json_array_string():
    assert _deserialize_row_data('[1,2,3]') == {}


# ── _effective_embed_batch_size ───────────────────────────────────


def test_effective_batch_zero_rows():
    assert _effective_embed_batch_size(0) == EMBED_BATCH_SIZE


def test_effective_batch_negative_rows():
    assert _effective_embed_batch_size(-10) == EMBED_BATCH_SIZE


def test_effective_batch_small_dataset():
    # Very small dataset should clamp to min batch size
    result = _effective_embed_batch_size(10)
    assert result >= INDEX_EMBED_MIN_BATCH_SIZE


def test_effective_batch_large_dataset():
    result = _effective_embed_batch_size(1_000_000)
    assert result <= EMBED_BATCH_SIZE
    assert result >= INDEX_EMBED_MIN_BATCH_SIZE


# ── index_dataset (mocked) ───────────────────────────────────────


def test_index_dataset_empty_dataset():
    """index_dataset with no rows should still call ensure/finalize."""
    mock_db_session = MagicMock()
    mock_db_session.__enter__ = MagicMock(return_value=mock_db_session)
    mock_db_session.__exit__ = MagicMock(return_value=False)
    # scalar() returns 0 rows
    mock_db_session.execute.return_value.scalar.return_value = 0
    # Second execute for streaming returns empty iterator
    mock_result = MagicMock()
    mock_result.__iter__ = MagicMock(return_value=iter([]))
    mock_db_session.execute.return_value = mock_result

    with patch("app.indexing.ensure_collection") as mock_ensure, \
         patch("app.indexing.prepare_collection_for_bulk_ingest") as mock_prep, \
         patch("app.indexing.finalize_collection_after_ingest") as mock_final, \
         patch("app.indexing.SessionLocal", return_value=mock_db_session):
        from app.indexing import index_dataset
        index_dataset(1)

    mock_ensure.assert_called_once_with(1)
    mock_prep.assert_called_once_with(1)
    mock_final.assert_called_once_with(1)


def test_index_dataset_with_rows_and_progress():
    """index_dataset processes rows and reports progress."""
    row1 = (0, {"name": "Alice", "age": "30"})
    row2 = (1, {"name": "Bob", "age": "25"})

    mock_db_session = MagicMock()
    mock_db_session.__enter__ = MagicMock(return_value=mock_db_session)
    mock_db_session.__exit__ = MagicMock(return_value=False)

    call_count = [0]

    def mock_execute(query, params=None):
        result = MagicMock()
        call_count[0] += 1
        if call_count[0] == 1:
            # scalar query for row count
            result.scalar.return_value = 2
            return result
        # streaming query returns rows
        result.__iter__ = MagicMock(return_value=iter([row1, row2]))
        return result

    mock_db_session.execute.side_effect = mock_execute

    progress_calls = []

    with patch("app.indexing.ensure_collection"), \
         patch("app.indexing.prepare_collection_for_bulk_ingest"), \
         patch("app.indexing.finalize_collection_after_ingest"), \
         patch("app.indexing.embed_texts", return_value=[[0.1] * 384, [0.2] * 384]), \
         patch("app.indexing.upsert_vectors"), \
         patch("app.indexing.SessionLocal", return_value=mock_db_session):
        from app.indexing import index_dataset
        index_dataset(1, progress_callback=lambda p, t: progress_calls.append((p, t)))

    # Initial progress (0, 2) then final (2, 2)
    assert progress_calls[0] == (0, 2)
    assert progress_calls[-1] == (2, 2)


def test_index_dataset_skips_empty_text():
    """Rows that produce empty text from row_to_text are skipped."""
    row1 = (0, {"name": None})  # row_to_text returns "" for all-None

    mock_db_session = MagicMock()
    mock_db_session.__enter__ = MagicMock(return_value=mock_db_session)
    mock_db_session.__exit__ = MagicMock(return_value=False)

    call_count = [0]

    def mock_execute(query, params=None):
        result = MagicMock()
        call_count[0] += 1
        if call_count[0] == 1:
            result.scalar.return_value = 1
            return result
        result.__iter__ = MagicMock(return_value=iter([row1]))
        return result

    mock_db_session.execute.side_effect = mock_execute

    with patch("app.indexing.ensure_collection"), \
         patch("app.indexing.prepare_collection_for_bulk_ingest"), \
         patch("app.indexing.finalize_collection_after_ingest"), \
         patch("app.indexing.embed_texts") as mock_embed, \
         patch("app.indexing.upsert_vectors") as mock_upsert, \
         patch("app.indexing.SessionLocal", return_value=mock_db_session):
        from app.indexing import index_dataset
        index_dataset(1, expected_total_rows=1)

    mock_embed.assert_not_called()
    mock_upsert.assert_not_called()
