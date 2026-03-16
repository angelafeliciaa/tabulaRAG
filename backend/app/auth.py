import hmac
import os
import secrets
from datetime import datetime, timedelta, timezone

import httpx
import jwt
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select

from app.db import SessionLocal
from app.models import User

_bearer = HTTPBearer(auto_error=False)

GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
JWT_SECRET = os.getenv("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "168"))  # 7 days


def _decode_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except (jwt.InvalidTokenError, jwt.ExpiredSignatureError):
        return None


def create_jwt(user: User) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user.id),
        "provider": user.provider,
        "name": user.name,
        "avatar_url": user.avatar_url or "",
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    if user.email:
        payload["email"] = user.email
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def upsert_user(provider: str, provider_id: str, name: str,
                email: str | None = None, avatar_url: str | None = None) -> User:
    with SessionLocal() as db:
        user = db.execute(
            select(User).where(User.provider == provider, User.provider_id == provider_id)
        ).scalar_one_or_none()

        if user is None:
            user = User(
                provider=provider,
                provider_id=provider_id,
                name=name,
                email=email,
                avatar_url=avatar_url or "",
            )
            db.add(user)
        else:
            user.name = name
            if email is not None:
                user.email = email
            if avatar_url is not None:
                user.avatar_url = avatar_url

        db.commit()
        db.refresh(user)
        # Detach from session so it can be used outside
        db.expunge(user)
        return user


def require_auth(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> dict:
    """Returns auth context: {"user_id": int, ...} for JWT, {"api_key": True} for API key."""
    if credentials is None:
        raise HTTPException(status_code=401, detail="Missing authentication")

    token = credentials.credentials

    # Try API key first
    api_key = os.getenv("API_KEY", "").strip()
    if api_key and hmac.compare_digest(token, api_key):
        return {"api_key": True}

    # Try JWT
    claims = _decode_jwt(token)
    if claims is not None:
        return {
            "user_id": int(claims["sub"]),
            "provider": claims.get("provider", "github"),
            "name": claims.get("name", ""),
        }

    raise HTTPException(status_code=401, detail="Invalid or expired token")


def get_user_id_from_auth(auth: dict) -> int | None:
    """Extract user_id from auth context. Returns None for API key auth (superuser)."""
    if auth.get("api_key"):
        return None
    return auth.get("user_id")


async def exchange_github_code(code: str) -> dict:
    if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="GitHub OAuth not configured",
        )

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://github.com/login/oauth/access_token",
            json={
                "client_id": GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code": code,
            },
            headers={"Accept": "application/json"},
            timeout=10.0,
        )

    if token_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="GitHub token exchange failed")

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        error = token_data.get("error_description", "Unknown error")
        raise HTTPException(status_code=401, detail=f"GitHub auth failed: {error}")

    async with httpx.AsyncClient() as client:
        user_resp = await client.get(
            "https://api.github.com/user",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            timeout=10.0,
        )

    if user_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch GitHub user")

    return user_resp.json()


async def exchange_google_code(code: str, redirect_uri: str) -> dict:
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Google OAuth not configured",
        )

    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"Accept": "application/json"},
            timeout=10.0,
        )

    if token_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Google token exchange failed")

    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        error = token_data.get("error_description", "Unknown error")
        raise HTTPException(status_code=401, detail=f"Google auth failed: {error}")

    async with httpx.AsyncClient() as client:
        user_resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10.0,
        )

    if user_resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to fetch Google user")

    return user_resp.json()
