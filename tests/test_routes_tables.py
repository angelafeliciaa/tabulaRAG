"""Tests for app.routes_tables – table CRUD endpoints."""

import io
import json

import pytest


def _ingest(client, name="test.csv", content=b"name,age\nAlice,30\nBob,25\n"):
    resp = client.post(
        "/ingest",
        files={"file": (name, io.BytesIO(content), "text/csv")},
        data={"has_header": "true"},
    )
    assert resp.status_code == 200
    return resp.json()["dataset_id"]


# ── GET /tables ───────────────────────────────────────────────────


def test_list_tables_empty(client):
    resp = client.get("/tables")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_tables_returns_dataset(client):
    _ingest(client)
    resp = client.get("/tables?include_pending=true")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1
    assert "dataset_id" in data[0]
    assert "name" in data[0]
    assert "row_count" in data[0]


# ── GET /tables/{id}/columns ─────────────────────────────────────


def test_get_columns(client):
    dataset_id = _ingest(client)
    resp = client.get(f"/tables/{dataset_id}/columns")
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_id"] == dataset_id
    col_names = [c["name"] for c in body["columns"]]
    assert "name" in col_names
    assert "age" in col_names


def test_get_columns_not_found(client):
    resp = client.get("/tables/99999/columns")
    assert resp.status_code == 404


# ── GET /tables/{id}/slice ───────────────────────────────────────


def test_get_slice(client):
    dataset_id = _ingest(client)
    resp = client.get(f"/tables/{dataset_id}/slice?offset=0&limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["dataset_id"] == dataset_id
    assert body["offset"] == 0
    assert len(body["rows"]) == 2
    assert "name" in body["rows"][0]["data"]


def test_get_slice_not_found(client):
    resp = client.get("/tables/99999/slice")
    assert resp.status_code == 404


def test_get_slice_with_offset(client):
    dataset_id = _ingest(client)
    resp = client.get(f"/tables/{dataset_id}/slice?offset=1&limit=10")
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 1


# ── DELETE /tables/{id} ──────────────────────────────────────────


def test_delete_table(client):
    dataset_id = _ingest(client)
    resp = client.delete(f"/tables/{dataset_id}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] == dataset_id

    # Confirm it's gone
    resp2 = client.get(f"/tables/{dataset_id}/columns")
    assert resp2.status_code == 404


def test_delete_table_not_found(client):
    resp = client.delete("/tables/99999")
    assert resp.status_code == 404


# ── PATCH /tables/{id} (rename) ──────────────────────────────────


def test_rename_table(client):
    dataset_id = _ingest(client)
    resp = client.patch(f"/tables/{dataset_id}", json={"name": "renamed"})
    assert resp.status_code == 200
    assert resp.json()["name"] == "renamed"


def test_rename_table_not_found(client):
    resp = client.patch("/tables/99999", json={"name": "nope"})
    assert resp.status_code == 404


# ── GET /tables/index-status ─────────────────────────────────────


def test_index_status(client):
    dataset_id = _ingest(client)
    resp = client.get(f"/tables/index-status?dataset_id={dataset_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) >= 1
    assert body[0]["dataset_id"] == dataset_id


def test_index_status_no_filter(client):
    _ingest(client)
    resp = client.get("/tables/index-status")
    assert resp.status_code == 200


# ── _normalize_row_data ──────────────────────────────────────────


def test_normalize_row_data_dict():
    from app.routes_tables import _normalize_row_data
    assert _normalize_row_data({"a": 1}) == {"a": 1}


def test_normalize_row_data_json_string():
    from app.routes_tables import _normalize_row_data
    assert _normalize_row_data('{"a": 1}') == {"a": 1}


def test_normalize_row_data_double_encoded():
    from app.routes_tables import _normalize_row_data
    inner = json.dumps({"a": 1})
    outer = json.dumps(inner)
    assert _normalize_row_data(outer) == {"a": 1}


def test_normalize_row_data_invalid():
    from app.routes_tables import _normalize_row_data
    assert _normalize_row_data("not json") == {}
    assert _normalize_row_data(42) == {}
    assert _normalize_row_data(None) == {}
