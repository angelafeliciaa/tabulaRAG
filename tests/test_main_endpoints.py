"""Tests for app.main – endpoint coverage for auth, health, ingest edge cases."""

import io
import os
from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from app.auth import create_jwt


# ── Health ────────────────────────────────────────────────────────


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_health_deps(client):
    resp = client.get("/health/deps")
    assert resp.status_code == 200
    body = resp.json()
    assert "postgres" in body
    assert "qdrant" in body


# ── Auth verify ───────────────────────────────────────────────────


def test_auth_verify_valid(client):
    resp = client.post("/auth/verify")
    assert resp.status_code == 200
    assert resp.json()["valid"] is True


def test_auth_verify_no_token():
    from fastapi.testclient import TestClient
    import app.main as app_main
    with TestClient(app_main.app) as c:
        resp = c.post("/auth/verify")
        assert resp.status_code == 401


def test_auth_verify_jwt(client):
    token = create_jwt({"id": 1, "login": "tester"})
    from fastapi.testclient import TestClient
    import app.main as app_main
    with TestClient(app_main.app, headers={"Authorization": f"Bearer {token}"}) as c:
        resp = c.post("/auth/verify")
        assert resp.status_code == 200


# ── Auth GitHub redirect ─────────────────────────────────────────


def test_github_redirect_not_configured(client):
    with patch("app.main.GITHUB_CLIENT_ID", ""):
        resp = client.get("/auth/github")
        assert resp.status_code == 500


def test_github_redirect_configured(client):
    with patch("app.main.GITHUB_CLIENT_ID", "my-client-id"):
        resp = client.get("/auth/github")
        assert resp.status_code == 200
        assert resp.json()["client_id"] == "my-client-id"


# ── Auth GitHub callback ─────────────────────────────────────────


def test_github_callback_missing_code(client):
    resp = client.post("/auth/github/callback", json={})
    assert resp.status_code == 400


def test_github_callback_success(client):
    mock_user = {"id": 1, "login": "octocat", "name": "Octo", "avatar_url": ""}

    with patch("app.main.exchange_github_code", new_callable=AsyncMock, return_value=mock_user):
        resp = client.post("/auth/github/callback", json={"code": "abc123"})
    assert resp.status_code == 200
    body = resp.json()
    assert "token" in body
    assert body["user"]["login"] == "octocat"


# ── Ingest edge cases ────────────────────────────────────────────


def test_ingest_missing_filename(client):
    resp = client.post(
        "/ingest",
        files={"file": ("", io.BytesIO(b"a,b\n1,2\n"), "text/csv")},
    )
    assert resp.status_code in (400, 422)


def test_ingest_invalid_extension(client):
    resp = client.post(
        "/ingest",
        files={"file": ("data.json", io.BytesIO(b'{"a":1}'), "application/json")},
    )
    assert resp.status_code == 400


# ── _normalize_headers ────────────────────────────────────────────


def test_normalize_headers():
    from app.normalization import normalize_headers
    result = normalize_headers(["Name", "", "Name", "  Age  "])
    assert result[0] == "Name"
    assert result[1] == "col_2"
    assert result[2] == "Name_2"  # deduplicated
    assert result[3] == "Age"


def test_normalize_headers_all_empty():
    from app.normalization import normalize_headers
    result = normalize_headers(["", "", ""])
    assert result == ["col_1", "col_2", "col_3"]


# ── validate_filename ─────────────────────────────────────────────


def test_validate_filename_csv():
    from app.main import validate_filename
    validate_filename("data.csv")  # should not raise


def test_validate_filename_tsv():
    from app.main import validate_filename
    validate_filename("data.tsv")  # should not raise


def test_validate_filename_invalid():
    from app.main import validate_filename
    from fastapi import HTTPException
    with pytest.raises(HTTPException):
        validate_filename("data.json")


# ── _detect_delimiter ─────────────────────────────────────────────


def test_detect_delimiter():
    from app.main import _detect_delimiter
    assert _detect_delimiter("file.csv") == ","
    assert _detect_delimiter("file.tsv") == "\t"
    assert _detect_delimiter("file.txt") == ","
    assert _detect_delimiter(None) == ","
