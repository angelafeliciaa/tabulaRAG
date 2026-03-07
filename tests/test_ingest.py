import io
import json
from unittest.mock import patch

import pytest
from sqlalchemy import text


def make_csv(content: str, filename: str = "test.csv"):
    return {"file": (filename, io.BytesIO(content.encode("utf-8")), "text/csv")}


# ── Happy path ────────────────────────────────────────────────────────────────

def test_basic_csv(client):
    response = client.post(
        "/ingest",
        files=make_csv("name,age,city\nAlice,30,London\nBob,25,Paris\n"),
        data={"has_header": "true"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["rows"] == 2
    assert body["columns"] == 3
    assert body["delimiter"] == ","


def test_tsv(client):
    response = client.post(
        "/ingest",
        files={"file": ("data.tsv", io.BytesIO(b"name\tage\nAlice\t30\n"), "text/plain")},
        data={"has_header": "true"},
    )
    assert response.status_code == 200
    assert response.json()["delimiter"] == "\t"


def test_no_header(client):
    response = client.post(
        "/ingest",
        files=make_csv("Alice,30,London\nBob,25,Paris\n"),
        data={"has_header": "false"},
    )
    body = response.json()
    assert body["rows"] == 2
    assert body["columns"] == 3


def test_custom_dataset_name(client):
    response = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "my_custom_name"},
    )
    assert response.json()["name"] == "my_custom_name"


def test_dataset_name_defaults_to_filename(client):
    response = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n", filename="people.csv"),
    )
    assert response.json()["name"] == "people"


def test_bom_utf8(client):
    content = "\ufeffname,age\nAlice,30\n".encode("utf-8-sig")
    response = client.post(
        "/ingest",
        files={"file": ("bom.csv", io.BytesIO(content), "text/csv")},
    )
    assert response.status_code == 200
    assert response.json()["columns"] == 2


def test_header_only_no_data_rows(client):
    response = client.post(
        "/ingest",
        files=make_csv("name,age,city\n"),
    )
    assert response.status_code == 200
    assert response.json()["rows"] == 0


def test_jagged_rows(client):
    response = client.post(
        "/ingest",
        files=make_csv("name,age,city\nAlice,30\nBob\n"),
    )
    assert response.status_code == 200
    assert response.json()["rows"] == 2


# ── Database state verification ───────────────────────────────────────────────
# With ORM you can easily query the DB after ingestion to verify the actual stored data

def test_db_dataset_record(client, test_engine):
    client.post(
        "/ingest",
        files=make_csv("name,age\nAlice,30\nBob,25\n"),
        data={"dataset_name": "verify_test"},
    )
    with test_engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM datasets WHERE name = 'verify_test'")).fetchone()
    assert row is not None
    assert row.row_count == 2
    assert row.column_count == 2


def test_db_columns_stored(client, test_engine):
    client.post("/ingest", files=make_csv("foo,bar,baz\n1,2,3\n"))
    with test_engine.connect() as conn:
        cols = conn.execute(text("SELECT name FROM dataset_columns ORDER BY column_index")).fetchall()
    assert [c.name for c in cols] == ["foo", "bar", "baz"]


def test_db_rows_stored(client, test_engine):
    client.post("/ingest", files=make_csv("name,age\nAlice,30\n"))
    with test_engine.connect() as conn:
        rows = conn.execute(text("SELECT row_data FROM dataset_rows")).fetchall()
    assert len(rows) == 1
    data = json.loads(rows[0].row_data)
    assert data == {"name": "Alice", "age": "30"}


# ── Error cases ───────────────────────────────────────────────────────────────

def test_empty_file(client):
    response = client.post("/ingest", files=make_csv(""))
    assert response.status_code == 400
    assert "Empty file" in response.json()["detail"]


# def test_missing_filename(client):
#     response = client.post(
#         "/ingest",
#         files={"file": ("", io.BytesIO(b"a,b\n1,2"), "text/csv")},
#     )
#     assert response.status_code == 400


def test_invalid_extension(client):
    response = client.post(
        "/ingest",
        files={"file": ("data.txt", io.BytesIO(b"a,b\n1,2"), "text/plain")},
    )
    assert response.status_code == 400
    assert ".csv or .tsv" in response.json()["detail"]


# ── GET /tables filtering ────────────────────────────────────────────────────

_PATCH_ENQUEUE = "app.main._enqueue_index_job"


def test_list_tables_excludes_indexing_datasets(client):
    """GET /tables should not return datasets that are still being indexed."""
    with patch(_PATCH_ENQUEUE):
        response = client.post(
            "/ingest",
            files=make_csv("a,b\n1,2\n"),
            data={"dataset_name": "pending_test"},
        )
    assert response.status_code == 200
    dataset_id = response.json()["dataset_id"]

    # Index job is queued but not processing, so it should be hidden
    tables_response = client.get("/tables")
    dataset_ids = [t["dataset_id"] for t in tables_response.json()]
    assert dataset_id not in dataset_ids


def test_list_tables_includes_ready_datasets(client):
    """GET /tables should include datasets after indexing completes."""
    from app.index_jobs import mark_index_job_ready

    with patch(_PATCH_ENQUEUE):
        response = client.post(
            "/ingest",
            files=make_csv("a,b\n1,2\n"),
            data={"dataset_name": "ready_test"},
        )
    assert response.status_code == 200
    dataset_id = response.json()["dataset_id"]
    row_count = response.json()["rows"]

    # Not visible while queued
    tables_response = client.get("/tables")
    dataset_ids = [t["dataset_id"] for t in tables_response.json()]
    assert dataset_id not in dataset_ids

    # Mark as ready
    mark_index_job_ready(dataset_id, row_count)

    # Now visible
    tables_response = client.get("/tables")
    dataset_ids = [t["dataset_id"] for t in tables_response.json()]
    assert dataset_id in dataset_ids


def test_list_tables_includes_error_datasets(client):
    """GET /tables should include datasets with indexing errors so users can manage them."""
    from app.index_jobs import mark_index_job_error

    with patch(_PATCH_ENQUEUE):
        response = client.post(
            "/ingest",
            files=make_csv("a,b\n1,2\n"),
            data={"dataset_name": "error_test"},
        )
    assert response.status_code == 200
    dataset_id = response.json()["dataset_id"]
    row_count = response.json()["rows"]

    # Mark as error
    mark_index_job_error(dataset_id, row_count, "Indexing failed")

    # Should be visible (so user can see/delete it)
    tables_response = client.get("/tables")
    dataset_ids = [t["dataset_id"] for t in tables_response.json()]
    assert dataset_id in dataset_ids