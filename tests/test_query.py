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


def test_query_strict_mode_requires_dataset_id(client, monkeypatch):
    _ingest(client)
    monkeypatch.setenv("QUERY_ENFORCE_LIST_TABLES_FIRST", "true")

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?"},
        )

    assert resp.status_code == 409
    detail = resp.json()["detail"]
    assert "dataset_id is required" in detail["message"]
    assert isinstance(detail["available_tables"], list)
    assert len(detail["available_tables"]) >= 1


def test_query_strict_mode_rejects_invalid_dataset_id(client, monkeypatch):
    dataset_id = _ingest(client)
    monkeypatch.setenv("QUERY_ENFORCE_LIST_TABLES_FIRST", "true")

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id + 9999},
        )

    assert resp.status_code == 404
    detail = resp.json()["detail"]
    assert "not found" in detail["message"].lower()
    assert isinstance(detail["available_tables"], list)
    assert any(int(item["dataset_id"]) == dataset_id for item in detail["available_tables"])


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
    assert data["results"][0]["source_url"].startswith("http://localhost:5173/highlight/")


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
    assert data["answer_details"]["source_url"].startswith("http://localhost:5173/highlight/")
    assert data["dataset_url"].endswith(f"/tables/{dataset_id}")
    assert data["results"][0]["source_url"].startswith("http://localhost:5173/highlight/")


def test_query_rank_single_row_returns_name_for_who_question(client):
    csv_content = (
        b"Sales Person,Product,Boxes Shipped,Country\n"
        b"Alice,Dark Bites,100,UK\n"
        b"Andrew,Milk Choco,620,US\n"
        b"Karlen McCaffrey,50% Dark Bites,778,Australia\n"
        b"Bob,White Choc,300,Canada\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("choco_sales.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "Who sold the most boxes in one deal?",
                "dataset_id": dataset_id,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["answer_type"] == "aggregate"
    assert data["answer_details"]["operation"] == "rank"
    assert data["answer_details"]["answer_column"] == "Sales Person"
    assert data["answer_details"]["answer_value"] == "Karlen McCaffrey"
    assert round(float(data["answer_details"]["metric_value"]), 3) == 778.0
    assert "Karlen McCaffrey" in data["answer"]
    assert "Karlen McCaffrey" in data["final_response"]
    assert data["verification"]["status"] == "pass"


def test_query_highest_amount_in_a_day_prefers_single_row_not_grouped_total(client):
    csv_content = (
        b"Date,Sales Person,Amount\n"
        b"2024-06-30,Alice,$210.00\n"
        b"2024-06-30,Bob,$220.00\n"
        b"2024-06-29,Carla,$300.00\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("day_amounts.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    with patch("app.retrieval.search_vectors", return_value=[]), \
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]):
        resp = client.post(
            "/query",
            json={
                "question": "What is the highest sales amount made in a day?",
                "dataset_id": dataset_id,
            },
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["answer_type"] == "aggregate"
    assert data["answer_details"]["operation"] == "rank"
    assert round(float(data["answer_details"]["metric_value"]), 3) == 300.0
    assert data["answer_details"]["source_row_data"]["Date"] == "2024-06-29"
    assert data["answer_details"]["source_row_data"]["Amount"] == "$300.00"
    assert "group_by_column" not in data["answer_details"]


def test_query_fail_closed_when_verification_fails(client, monkeypatch):
    dataset_id = _ingest(client)
    monkeypatch.setenv("QUERY_ENABLE_VERIFICATION", "true")
    monkeypatch.setenv("QUERY_FAIL_CLOSED_ON_VERIFY_ERROR", "true")

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
         patch("app.retrieval.embed_texts", return_value=[[0.1] * 384]), \
         patch("app.retrieval.get_highlight", return_value=None):
        resp = client.post(
            "/query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["verification"]["status"] == "fail"
    assert "I could not verify this answer against source rows." in data["final_response"]


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
    assert "Link: http://localhost:5173/highlight/" in data["final_response"]


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
    assert "Link: http://localhost:5173/highlight/" in data["final_response"]


# ── POST /filter ──────────────────────────────────────────────────

def test_filter_rows_success(client):
    dataset_id = _ingest(client)

    resp = client.post(
        "/filter",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "city", "operator": "=", "value": "London"}],
            "limit": 10,
            "offset": 0,
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert data["row_count"] == 1
    assert len(data["rowsResult"]) == 1
    assert data["rowsResult"][0]["row_data"]["city"] == "London"
    assert data["url"].startswith("http://localhost:5173/tables/virtual?q=")


def test_filter_between_numeric(client):
    csv_content = (
        b"name,number_of_rooms\n"
        b"A,2\n"
        b"B,3\n"
        b"C,5\n"
        b"D,7\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("rooms.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.post(
        "/filter",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "number_of_rooms", "operator": "BETWEEN", "value": "3,6"}],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["row_count"] == 2
    names = [row["row_data"]["name"] for row in data["rowsResult"]]
    assert names == ["B", "C"]


def test_filter_not_like(client):
    csv_content = (
        b"city\n"
        b"Paris\n"
        b"Tokyo\n"
        b"Berlin\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("cities.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.post(
        "/filter",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "city", "operator": "NOT LIKE", "value": "%is"}],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    cities = [row["row_data"]["city"] for row in data["rowsResult"]]
    assert cities == ["Tokyo", "Berlin"]


def test_filter_or_conditions(client):
    csv_content = (
        b"city,year_listed\n"
        b"Paris,2010\n"
        b"London,2014\n"
        b"Rome,2008\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("listings.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.post(
        "/filter",
        json={
            "dataset_id": dataset_id,
            "filters": [
                {"column": "city", "operator": "=", "value": "Paris"},
                {
                    "column": "year_listed",
                    "operator": ">",
                    "value": "2012",
                    "logical_operator": "OR",
                },
            ],
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    cities = [row["row_data"]["city"] for row in data["rowsResult"]]
    assert cities == ["Paris", "London"]


def test_filter_dataset_not_found(client):
    resp = client.post(
        "/filter",
        json={
            "dataset_id": 999999,
            "filters": [{"column": "city", "operator": "=", "value": "London"}],
        },
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


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
