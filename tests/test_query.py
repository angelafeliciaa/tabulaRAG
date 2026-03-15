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
    assert data["rowsResult"][0]["highlight_id"] == f"d{dataset_id}_r0_city"
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


def test_filter_row_indices_success(client):
    dataset_id = _ingest(client)

    resp = client.post(
        "/filter/row-indices",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "city", "operator": "=", "value": "London"}],
            "max_rows": 1000,
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["dataset_id"] == dataset_id
    assert data["row_indices"] == [0]
    assert data["total_match_count"] == 1
    assert data["truncated"] is False


def test_filter_row_indices_or_conditions(client):
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
        "/filter/row-indices",
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
            "max_rows": 1000,
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["row_indices"] == [0, 1]
    assert data["total_match_count"] == 2
    assert data["truncated"] is False


def test_filter_row_indices_truncated(client):
    csv_content = (
        b"name,city\n"
        b"A,London\n"
        b"B,London\n"
        b"C,London\n"
        b"D,Paris\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("people.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.post(
        "/filter/row-indices",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "city", "operator": "=", "value": "London"}],
            "max_rows": 2,
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["row_indices"] == [0, 1]
    assert data["total_match_count"] == 3
    assert data["truncated"] is True


def test_filter_row_indices_is_null(client):
    csv_content = (
        b"name,team\n"
        b"Alice,Alpha\n"
        b"Bob\n"
        b"Carol,Gamma\n"
    )
    resp = client.post(
        "/ingest",
        files={"file": ("teams.csv", csv_content, "text/csv")},
    )
    dataset_id = resp.json()["dataset_id"]

    resp = client.post(
        "/filter/row-indices",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "team", "operator": "IS NULL"}],
            "max_rows": 1000,
        },
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["row_indices"] == [1]
    assert data["total_match_count"] == 1
    assert data["truncated"] is False


def test_filter_row_indices_dataset_not_found(client):
    resp = client.post(
        "/filter/row-indices",
        json={
            "dataset_id": 999999,
            "filters": [{"column": "city", "operator": "=", "value": "London"}],
        },
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


def test_filter_row_indices_invalid_column(client):
    dataset_id = _ingest(client)
    resp = client.post(
        "/filter/row-indices",
        json={
            "dataset_id": dataset_id,
            "filters": [{"column": "unknown_col", "operator": "=", "value": "x"}],
        },
    )
    assert resp.status_code == 400
    assert "Invalid filter column" in resp.json()["detail"]


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


