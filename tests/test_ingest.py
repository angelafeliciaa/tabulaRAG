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


# ── Description field ────────────────────────────────────────────────────────

def test_ingest_with_description(client):
    response = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "described_table", "description": "Sales data for Q1 2024"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "described_table"
    assert body["description"] == "Sales data for Q1 2024"


def test_ingest_without_description(client):
    response = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "no_desc_table"},
    )
    assert response.status_code == 200


def test_description_stored_in_db(client, test_engine):
    client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "desc_db_test", "description": "Test description"},
    )
    with test_engine.connect() as conn:
        row = conn.execute(text("SELECT description FROM datasets WHERE name = 'desc_db_test'")).fetchone()
    assert row is not None
    assert row.description == "Test description"


def test_description_null_when_omitted(client, test_engine):
    client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "no_desc_db_test"},
    )
    with test_engine.connect() as conn:
        row = conn.execute(text("SELECT description FROM datasets WHERE name = 'no_desc_db_test'")).fetchone()
    assert row is not None
    assert row.description is None


def test_description_in_list_tables(client):
    client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "list_desc_test", "description": "A test table"},
    )
    response = client.get("/tables")
    assert response.status_code == 200
    tables = response.json()
    table = next(t for t in tables if t["name"] == "list_desc_test")
    assert table["description"] == "A test table"


def test_update_description_via_patch(client):
    resp = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "patch_desc_test"},
    )
    dataset_id = resp.json()["dataset_id"]
    patch_resp = client.patch(
        f"/tables/{dataset_id}",
        json={"description": "Updated description"},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["description"] == "Updated description"


def test_clear_description_via_patch(client):
    resp = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "clear_desc_test", "description": "Initial desc"},
    )
    dataset_id = resp.json()["dataset_id"]
    patch_resp = client.patch(
        f"/tables/{dataset_id}",
        json={"description": "  "},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["description"] is None


def test_clear_description_with_explicit_null(client):
    resp = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "null_desc_test", "description": "To be cleared"},
    )
    dataset_id = resp.json()["dataset_id"]
    patch_resp = client.patch(
        f"/tables/{dataset_id}",
        json={"description": None},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["description"] is None


def test_patch_name_only_preserves_description(client):
    resp = client.post(
        "/ingest",
        files=make_csv("a,b\n1,2\n"),
        data={"dataset_name": "preserve_desc", "description": "Keep me"},
    )
    dataset_id = resp.json()["dataset_id"]
    patch_resp = client.patch(
        f"/tables/{dataset_id}",
        json={"name": "renamed_preserve"},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["name"] == "renamed_preserve"
    assert patch_resp.json()["description"] == "Keep me"


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