from unittest.mock import patch, MagicMock

import pytest


# ── Helper: ingest a small CSV so the dataset + rows exist in PG ──

def _ingest(client):
    csv_content = b"name,city,age\nAlice,London,30\nBob,Paris,25\n"
    resp = client.post(
        "/ingest",
        files={"file": ("people.csv", csv_content, "text/csv")},
    )
    assert resp.status_code == 200
    return resp.json()["dataset_id"]


# ── POST /query ───────────────────────────────────────────────────

def test_query_dataset_not_found(client):
    resp = client.post(
        "/query",
        json={"question": "Who lives in London?", "dataset_id": 9999},
    )
    assert resp.status_code == 404
    assert "Dataset not found" in resp.json()["detail"]


def test_query_missing_question(client):
    resp = client.post("/query", json={"dataset_id": 1})
    assert resp.status_code == 422


def test_query_semantic_results(client):
    dataset_id = _ingest(client)

    mock_hits = [
        {
            "id": 0,
            "score": 0.92,
            "payload": {
                "row_data": {"name": "Alice", "city": "London", "age": "30"},
                "text": "name: Alice | city: London | age: 30",
            },
        },
    ]

    with patch("app.retrieval.search_vectors", return_value=mock_hits), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert data["question"] == "Who lives in London?"
    assert len(data["results"]) >= 1
    assert data["results"][0]["row_index"] == 0
    assert data["results"][0]["match_type"] == "semantic"


def test_query_with_filters(client):
    dataset_id = _ingest(client)

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "tell me about this person",
                "dataset_id": dataset_id,
                "filters": {"city": "London"},
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    # Exact match should appear with score 1.0
    exact_results = [r for r in data["results"] if r["match_type"] == "exact"]
    assert len(exact_results) == 1
    assert exact_results[0]["row_data"]["city"] == "London"
    assert exact_results[0]["score"] == 1.0


# ── GET /highlights/{highlight_id} ────────────────────────────────

def test_highlight_found(client):
    dataset_id = _ingest(client)

    resp = client.get(f"/highlights/d{dataset_id}_r0_name")
    assert resp.status_code == 200
    data = resp.json()
    assert data["highlight_id"] == f"d{dataset_id}_r0_name"
    assert data["dataset_id"] == dataset_id
    assert data["row_index"] == 0
    assert data["column"] == "name"
    assert data["value"] == "Alice"
    assert "city" in data["row_context"]


def test_highlight_not_found(client):
    resp = client.get("/highlights/d999_r0_name")
    assert resp.status_code == 404


def test_highlight_invalid_format(client):
    resp = client.get("/highlights/invalid")
    assert resp.status_code == 404


def test_highlight_column_with_underscore(client):
    """Columns with underscores in their names should be handled correctly."""
    csv_content = b"first_name,last_name\nAlice,Smith\n"
    resp = client.post(
        "/ingest",
        files={"file": ("names.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.get(f"/highlights/d{dataset_id}_r0_first_name")
    assert resp.status_code == 200
    assert resp.json()["column"] == "first_name"
    assert resp.json()["value"] == "Alice"
