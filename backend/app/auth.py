import os
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

_bearer = HTTPBearer(auto_error=False)


def require_api_key(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),
) -> None:
    api_key = os.getenv("API_KEY", "tabularag-dev-key").strip()
    if not api_key:
        return
    if credentials is None or credentials.credentials != api_key:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing API key",
        )
