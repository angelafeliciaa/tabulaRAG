"""Tests for app.auth – JWT creation, decoding, require_auth, and OAuth flows."""

import os
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.auth import _decode_jwt, create_jwt, require_auth, JWT_SECRET, JWT_ALGORITHM


# ── helpers ────────────────────────────────────────────────────────


def _github_user(**kwargs) -> dict:
    """Create a GitHub user dict for testing."""
    defaults = {
        "id": 1,
        "login": "octocat",
        "name": "Octo Cat",
        "avatar_url": "https://example.com/avatar.png",
    }
    defaults.update(kwargs)
    return defaults


# ── JWT round-trip ────────────────────────────────────────────────


def test_create_and_decode_jwt():
    user = _github_user()
    token = create_jwt(user)
    claims = _decode_jwt(token)
    assert claims is not None
    assert claims["sub"] == "1"
    assert claims["login"] == "octocat"
    assert claims["name"] == "Octo Cat"
    assert claims["avatar_url"] == "https://example.com/avatar.png"


def test_create_jwt_uses_login_as_name_fallback():
    user = _github_user(name=None)
    token = create_jwt(user)
    claims = _decode_jwt(token)
    assert claims["name"] == "octocat"


def test_decode_jwt_invalid_token():
    assert _decode_jwt("not.a.valid.token") is None


def test_decode_jwt_wrong_secret():
    import jwt as pyjwt
    token = pyjwt.encode({"sub": "1"}, "wrong-secret", algorithm=JWT_ALGORITHM)
    assert _decode_jwt(token) is None


def test_decode_jwt_expired():
    import jwt as pyjwt
    from datetime import datetime, timedelta, timezone
    payload = {
        "sub": "1",
        "exp": datetime.now(timezone.utc) - timedelta(hours=1),
    }
    token = pyjwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    assert _decode_jwt(token) is None


# ── require_auth ──────────────────────────────────────────────────


def test_require_auth_missing_credentials():
    from fastapi import HTTPException
    with pytest.raises(HTTPException) as exc_info:
        require_auth(credentials=None)
    assert exc_info.value.status_code == 401
    assert "Missing" in exc_info.value.detail


def test_require_auth_valid_api_key():
    cred = MagicMock()
    cred.credentials = os.environ.get("API_KEY", "test-key")
    result = require_auth(credentials=cred)
    assert result is None  # returns None on success


def test_require_auth_valid_jwt():
    user = _github_user(id=99)
    token = create_jwt(user)
    cred = MagicMock()
    cred.credentials = token
    result = require_auth(credentials=cred)
    assert result is None  # returns None on success


def test_require_auth_invalid_token():
    from fastapi import HTTPException
    cred = MagicMock()
    cred.credentials = "totally-bogus-token"
    with pytest.raises(HTTPException) as exc_info:
        require_auth(credentials=cred)
    assert exc_info.value.status_code == 401
    assert "Invalid" in exc_info.value.detail


# ── exchange_github_code ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_exchange_github_code_not_configured():
    from app.auth import exchange_github_code
    from fastapi import HTTPException
    with patch("app.auth.GITHUB_CLIENT_ID", ""), patch("app.auth.GITHUB_CLIENT_SECRET", ""):
        with pytest.raises(HTTPException) as exc_info:
            await exchange_github_code("some-code")
        assert exc_info.value.status_code == 500
        assert "not configured" in exc_info.value.detail


@pytest.mark.asyncio
async def test_exchange_github_code_token_exchange_fails():
    from app.auth import exchange_github_code
    from fastapi import HTTPException

    mock_resp = MagicMock()
    mock_resp.status_code = 500

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.auth.GITHUB_CLIENT_ID", "id"), \
         patch("app.auth.GITHUB_CLIENT_SECRET", "secret"), \
         patch("app.auth.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(HTTPException) as exc_info:
            await exchange_github_code("bad-code")
        assert exc_info.value.status_code == 502


@pytest.mark.asyncio
async def test_exchange_github_code_no_access_token():
    from app.auth import exchange_github_code
    from fastapi import HTTPException

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"error_description": "bad code"}

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.auth.GITHUB_CLIENT_ID", "id"), \
         patch("app.auth.GITHUB_CLIENT_SECRET", "secret"), \
         patch("app.auth.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(HTTPException) as exc_info:
            await exchange_github_code("bad-code")
        assert exc_info.value.status_code == 401
        assert "bad code" in exc_info.value.detail


@pytest.mark.asyncio
async def test_exchange_github_code_user_fetch_fails():
    from app.auth import exchange_github_code
    from fastapi import HTTPException

    token_resp = MagicMock()
    token_resp.status_code = 200
    token_resp.json.return_value = {"access_token": "gho_abc123"}

    user_resp = MagicMock()
    user_resp.status_code = 403

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.auth.GITHUB_CLIENT_ID", "id"), \
         patch("app.auth.GITHUB_CLIENT_SECRET", "secret"), \
         patch("app.auth.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(HTTPException) as exc_info:
            await exchange_github_code("good-code")
        assert exc_info.value.status_code == 502


@pytest.mark.asyncio
async def test_exchange_github_code_success():
    from app.auth import exchange_github_code

    token_resp = MagicMock()
    token_resp.status_code = 200
    token_resp.json.return_value = {"access_token": "gho_abc123"}

    user_resp = MagicMock()
    user_resp.status_code = 200
    user_resp.json.return_value = {"id": 1, "login": "octocat", "name": "Octo", "avatar_url": ""}

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.auth.GITHUB_CLIENT_ID", "id"), \
         patch("app.auth.GITHUB_CLIENT_SECRET", "secret"), \
         patch("app.auth.httpx.AsyncClient", return_value=mock_client):
        result = await exchange_github_code("good-code")
        assert result["login"] == "octocat"
