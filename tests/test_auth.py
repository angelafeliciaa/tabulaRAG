"""Tests for app.auth – JWT creation, decoding, require_auth, and OAuth flows."""

import asyncio
import os
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from app.auth import _decode_jwt, create_jwt, require_auth, upsert_user, get_user_id_from_auth, JWT_SECRET, JWT_ALGORITHM
from app.models import User


# ── helpers ────────────────────────────────────────────────────────


def _make_user(**kwargs) -> User:
    """Create an in-memory User object for testing (not persisted)."""
    defaults = {
        "id": 1,
        "provider": "github",
        "provider_id": "42",
        "name": "Octo Cat",
        "email": "octo@example.com",
        "avatar_url": "https://example.com/avatar.png",
    }
    defaults.update(kwargs)
    user = User()
    for k, v in defaults.items():
        setattr(user, k, v)
    return user


# ── JWT round-trip ────────────────────────────────────────────────


def test_create_and_decode_jwt():
    user = _make_user()
    token = create_jwt(user)
    claims = _decode_jwt(token)
    assert claims is not None
    assert claims["sub"] == "1"
    assert claims["provider"] == "github"
    assert claims["name"] == "Octo Cat"
    assert claims["avatar_url"] == "https://example.com/avatar.png"
    assert claims["email"] == "octo@example.com"


def test_create_jwt_google_provider():
    user = _make_user(id=2, provider="google", name="Test User", email="test@gmail.com")
    token = create_jwt(user)
    claims = _decode_jwt(token)
    assert claims["provider"] == "google"
    assert claims["email"] == "test@gmail.com"


def test_create_jwt_no_email():
    user = _make_user(email=None)
    token = create_jwt(user)
    claims = _decode_jwt(token)
    assert "email" not in claims


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
    assert result["api_key"] is True


def test_require_auth_valid_jwt():
    user = _make_user(id=99)
    token = create_jwt(user)
    cred = MagicMock()
    cred.credentials = token
    result = require_auth(credentials=cred)
    assert result["user_id"] == 99
    assert result["provider"] == "github"


def test_require_auth_invalid_token():
    from fastapi import HTTPException
    cred = MagicMock()
    cred.credentials = "totally-bogus-token"
    with pytest.raises(HTTPException) as exc_info:
        require_auth(credentials=cred)
    assert exc_info.value.status_code == 401
    assert "Invalid" in exc_info.value.detail


# ── get_user_id_from_auth ────────────────────────────────────────


def test_get_user_id_from_auth_jwt():
    assert get_user_id_from_auth({"user_id": 42}) == 42


def test_get_user_id_from_auth_api_key():
    assert get_user_id_from_auth({"api_key": True}) is None


# ── upsert_user ──────────────────────────────────────────────────


def test_upsert_user_creates_new(test_engine):
    user = upsert_user(
        provider="github",
        provider_id="12345",
        name="New User",
        email="new@example.com",
        avatar_url="https://example.com/avatar.png",
    )
    assert user.id is not None
    assert user.provider == "github"
    assert user.provider_id == "12345"
    assert user.name == "New User"


def test_upsert_user_updates_existing(test_engine):
    user1 = upsert_user(provider="google", provider_id="99", name="Old Name", email="a@b.com")
    user2 = upsert_user(provider="google", provider_id="99", name="New Name", email="c@d.com")
    assert user1.id == user2.id
    assert user2.name == "New Name"
    assert user2.email == "c@d.com"


# ── exchange_github_code ──────────────────────────────────────────


def test_exchange_github_code_not_configured():
    from app.auth import exchange_github_code
    from fastapi import HTTPException
    with patch("app.auth.GITHUB_CLIENT_ID", ""), patch("app.auth.GITHUB_CLIENT_SECRET", ""):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(exchange_github_code("some-code"))
        assert exc_info.value.status_code == 500
        assert "not configured" in exc_info.value.detail


def test_exchange_github_code_token_exchange_fails():
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
            asyncio.run(exchange_github_code("bad-code"))
        assert exc_info.value.status_code == 502


def test_exchange_github_code_no_access_token():
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
            asyncio.run(exchange_github_code("bad-code"))
        assert exc_info.value.status_code == 401
        assert "bad code" in exc_info.value.detail


def test_exchange_github_code_user_fetch_fails():
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
            asyncio.run(exchange_github_code("good-code"))
        assert exc_info.value.status_code == 502


def test_exchange_github_code_success():
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
        result = asyncio.run(exchange_github_code("good-code"))
        assert result["login"] == "octocat"


# ── exchange_google_code ──────────────────────────────────────────


def test_exchange_google_code_not_configured():
    from app.auth import exchange_google_code
    from fastapi import HTTPException
    with patch("app.auth.GOOGLE_CLIENT_ID", ""), patch("app.auth.GOOGLE_CLIENT_SECRET", ""):
        with pytest.raises(HTTPException) as exc_info:
            asyncio.run(exchange_google_code("some-code", "http://localhost/callback"))
        assert exc_info.value.status_code == 500
        assert "not configured" in exc_info.value.detail


def test_exchange_google_code_success():
    from app.auth import exchange_google_code

    token_resp = MagicMock()
    token_resp.status_code = 200
    token_resp.json.return_value = {"access_token": "ya29.abc123"}

    user_resp = MagicMock()
    user_resp.status_code = 200
    user_resp.json.return_value = {
        "id": "1234",
        "email": "user@gmail.com",
        "name": "Google User",
        "picture": "https://lh3.googleusercontent.com/photo.jpg",
    }

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=token_resp)
    mock_client.get = AsyncMock(return_value=user_resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("app.auth.GOOGLE_CLIENT_ID", "id"), \
         patch("app.auth.GOOGLE_CLIENT_SECRET", "secret"), \
         patch("app.auth.httpx.AsyncClient", return_value=mock_client):
        result = asyncio.run(exchange_google_code("good-code", "http://localhost/callback"))
        assert result["email"] == "user@gmail.com"
        assert result["name"] == "Google User"
