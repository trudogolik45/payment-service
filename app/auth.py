import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import APIKeyHeader

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def require_api_key(
    request: Request, api_key: Annotated[str | None, Depends(api_key_header)]
) -> None:
    expected: str = request.app.state.settings.api_key
    if api_key is None or not secrets.compare_digest(api_key, expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing X-API-Key")
