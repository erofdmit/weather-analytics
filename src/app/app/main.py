from __future__ import annotations

import httpx
from fastapi import FastAPI

from app.api.routes.weather import router as weather_router
from app.core.config import get_settings


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Weather Aggregator API",
        version="0.1.0",
    )

    app.include_router(weather_router, prefix="/api")

    @app.on_event("startup")
    async def startup_event() -> None:
        timeout = httpx.Timeout(settings.http_timeout)
        app.state.http_client = httpx.AsyncClient(timeout=timeout)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        client: httpx.AsyncClient | None = getattr(app.state, "http_client", None)
        if client is not None:
            await client.aclose()

    return app


app = create_app()
