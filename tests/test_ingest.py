import io
import json
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


def test_ingest_requires_auth():
    from fastapi.testclient import TestClient
    import app.main as app_main

    with TestClient(app_main.app) as unauthenticated:
        response = unauthenticated.post(
            "/ingest",
            files={"file": ("data.csv", io.BytesIO(b"a,b\n1,2\n"), "text/csv")},
        )
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API key"