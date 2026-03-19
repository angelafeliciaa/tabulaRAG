from unittest.mock import patch

from app.retrieval import extract_keywords


def _ingest(client):
    csv_content = b"name,city,age\nAlice,London,30\nBob,Paris,25\n"
    resp = client.post(
        "/ingest",
        files={"file": ("people.csv", csv_content, "text/csv")},
    )
    assert resp.status_code == 200
    return resp.json()["dataset_id"]


def test_semantic_query_dataset_not_found(client):
    resp = client.post(
        "/semantic_query",
        json={"question": "Who lives in London?", "dataset_id": 9999},
    )
    assert resp.status_code == 404


def test_semantic_query_semantic_results(client):
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
            "/semantic_query",
            json={"question": "Who lives in London?", "dataset_id": dataset_id},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert isinstance(data.get("dataset_url"), str)
    assert data["dataset_url"].startswith("http")


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


def test_extract_keywords_basic():
    keywords = extract_keywords("who is the engineer from asana")
    assert "engineer" in keywords
    assert "asana" in keywords
    assert "who" not in keywords
    assert "is" not in keywords
    assert "the" not in keywords
    assert "from" not in keywords


def test_extract_keywords_all_stop_words():
    keywords = extract_keywords("who is the from")
    assert keywords == []


def test_extract_keywords_punctuation():
    keywords = extract_keywords("what's the role at asana?")
    assert "asana" in keywords
    assert "role" in keywords
    for kw in keywords:
        assert "?" not in kw
        assert "'" not in kw
