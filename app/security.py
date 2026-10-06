from fastapi import Header, HTTPException, status

from app.config import settings


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Проверка статического API-ключа. Применяется ко всем эндпоинтам.
    Отсутствующий или неверный ключ -> 401."""
    if x_api_key != settings.api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing X-API-Key",
        )
