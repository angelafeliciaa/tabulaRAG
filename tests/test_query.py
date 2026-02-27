from unittest.mock import patch, MagicMock

import pytest

from app.retrieval import extract_keywords


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
    assert "ingested tables" in resp.json()["detail"].lower()


def test_query_missing_question(client):
    resp = client.post("/query", json={"dataset_id": 1})
    assert resp.status_code == 422


def test_query_top_k_optional(client):
    dataset_id = _ingest(client)

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id},
        )

    assert resp.status_code == 200
    assert resp.json()["dataset_id"] == dataset_id


def test_query_auto_resolves_dataset_from_question(client):
    csv_content = b"Product,Boxes Shipped,Country\nDark,10,UK\n"
    ingest_resp = client.post(
        "/ingest",
        files={"file": ("chocolate.csv", csv_content, "text/csv")},
        data={"dataset_name": "Chocolate"},
    )
    dataset_id = ingest_resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "From the Chocolate table, how many entries are there?"},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert data["resolved_dataset"]["dataset_id"] == dataset_id
    assert "resolved" in (data.get("resolution_note") or "").lower()


def test_query_invalid_dataset_id_resolves_from_name_hint(client):
    csv_content = b"Product,Boxes Shipped,Country\nDark,10,UK\n"
    ingest_resp = client.post(
        "/ingest",
        files={"file": ("chocolate.csv", csv_content, "text/csv")},
        data={"dataset_name": "Chocolate"},
    )
    dataset_id = ingest_resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "dataset_id": dataset_id + 999,
                "question": "From the Chocolate table, what product has the most boxes shipped?",
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert data["resolved_dataset"]["name"] == "Chocolate"
    assert "not found" in (data.get("resolution_note") or "").lower()


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
    assert data["results"][0]["source_url"].endswith(
        f"/tables/{dataset_id}/slice?offset=0&limit=1"
    )


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


def test_query_aggregate_answer_with_source_links(client):
    csv_content = (
        b"Product,Boxes Shipped,Country\n"
        b"Dark,10,UK\n"
        b"Caramel,14,US\n"
        b"Dark,30,UK\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("choco.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "What product has the most amount of boxes shipped?",
                "dataset_id": dataset_id,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["answer_type"] == "aggregate"
    assert "Dark" in data["answer"]
    assert data["answer_details"]["group_by_column"] == "Product"
    assert data["answer_details"]["metric_column"] == "Boxes Shipped"
    assert data["answer_details"]["metric_value"] == 40
    assert data["answer_details"]["source_url"].endswith(
        f"/tables/{dataset_id}/slice?offset=2&limit=1"
    )
    assert data["dataset_url"].endswith(f"/tables/{dataset_id}/slice?offset=0&limit=30")
    assert data["results"][0]["source_url"].startswith("http://localhost:8000/tables/")


def test_query_sum_with_natural_language_filter(client):
    csv_content = (
        b"Product,Boxes Shipped,Country\n"
        b"Dark,10,UK\n"
        b"Caramel,14,US\n"
        b"Dark,30,UK\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("choco.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "How many boxes shipped for Dark?",
                "dataset_id": dataset_id,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["answer_type"] == "aggregate"
    assert data["answer_details"]["operation"] == "sum"
    assert data["answer_details"]["metric_column"] == "Boxes Shipped"
    assert data["answer_details"]["metric_value"] == 40
    assert data["answer_details"]["filters"]["Product"] == "Dark"
    assert "Source URL:" in data["final_response"]


def test_query_count_with_natural_language_filter(client):
    csv_content = (
        b"Product,Boxes Shipped,Country\n"
        b"Dark,10,UK\n"
        b"Caramel,14,US\n"
        b"Dark,30,UK\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("choco.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "How many entries are in UK?",
                "dataset_id": dataset_id,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["answer_type"] == "aggregate"
    assert data["answer_details"]["operation"] == "count"
    assert data["answer_details"]["metric_value"] == 2
    assert data["answer_details"]["filters"]["Country"] == "UK"
    assert "Source URL:" in data["final_response"]


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


# ── extract_keywords tests ────────────────────────────────────────

def test_extract_keywords_basic():
    """Should extract meaningful keywords, stripping stop words."""
    keywords = extract_keywords("who is the engineer from asana")
    assert "engineer" in keywords
    assert "asana" in keywords
    # Stop words should be excluded
    assert "who" not in keywords
    assert "is" not in keywords
    assert "the" not in keywords
    assert "from" not in keywords


def test_extract_keywords_all_stop_words():
    """A query of only stop words should return an empty list."""
    keywords = extract_keywords("who is the from")
    assert keywords == []


def test_extract_keywords_punctuation():
    """Punctuation should be stripped before extracting keywords."""
    keywords = extract_keywords("what's the role at asana?")
    assert "asana" in keywords
    assert "role" in keywords
    # Punctuation artifacts should not appear
    for kw in keywords:
        assert "?" not in kw
        assert "'" not in kw


# ── Two-pass search behavior ──────────────────────────────────────

def test_two_pass_search_filtered_results_first(client):
    """Filtered (keyword-matched) results should appear before fallback results."""
    dataset_id = _ingest(client)

    # Filtered hit: contains "London" keyword
    filtered_hit = {
        "id": 0,
        "score": 0.85,
        "payload": {
            "row_data": {"name": "Alice", "city": "London", "age": "30"},
            "text": "name: Alice | city: London | age: 30",
        },
    }
    # Fallback hit: does not contain "London"
    fallback_hit = {
        "id": 1,
        "score": 0.90,
        "payload": {
            "row_data": {"name": "Bob", "city": "Paris", "age": "25"},
            "text": "name: Bob | city: Paris | age: 25",
        },
    }

    def mock_search(dataset_id, query_vector, limit=10, query_filter=None):
        if query_filter is not None:
            return [filtered_hit]
        return [fallback_hit, filtered_hit]

    with patch("app.retrieval.search_vectors", side_effect=mock_search), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id},
        )

    assert resp.status_code == 200
    data = resp.json()
    results = data["results"]
    assert len(results) >= 2
    # Filtered result (Alice/London) should come first
    assert results[0]["row_data"]["city"] == "London"
    # Fallback result (Bob/Paris) should come second (deduplicated)
    assert results[1]["row_data"]["city"] == "Paris"
