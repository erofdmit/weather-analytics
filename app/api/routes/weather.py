from __future__ import annotations

import httpx
from app.core.config import Settings, get_settings
from app.models.weather import AggregatedWeatherResponse
from app.services.aggregator import WeatherAggregator
from app.services.weather_providers.open_meteo import OpenMeteoProvider
from app.services.weather_providers.openweather import OpenWeatherMapProvider
from app.services.weather_providers.weatherapi import WeatherAPIProvider
from app.services.weather_providers.weatherbit import WeatherbitProvider
from app.services.weather_providers.weatherstack import WeatherstackProvider
from fastapi import APIRouter, Depends, Query, Request

router = APIRouter(prefix="/weather", tags=["weather"])


def _get_http_client(request: Request) -> httpx.AsyncClient:
    client = getattr(request.app.state, "http_client", None)
    if client is None:
        client = httpx.AsyncClient()
        request.app.state.http_client = client
    return client


@router.get(
    "/current",
    response_model=AggregatedWeatherResponse,
    summary="Текущая погода по координатам из нескольких провайдеров",
)
async def get_current_weather(
    request: Request,
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота"),
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> AggregatedWeatherResponse:
    """
    Агрегированный эндпоинт: возвращает текущую погоду от 1–5 провайдеров,
    плюс простые средние по температуре и влажности.
    """

    client = _get_http_client(request)

    providers = [OpenMeteoProvider(client)]

    if settings.openweather_api_key:
        providers.append(OpenWeatherMapProvider(client, settings.openweather_api_key))

    if settings.weatherapi_api_key:
        providers.append(WeatherAPIProvider(client, settings.weatherapi_api_key))

    if settings.weatherbit_api_key:
        providers.append(WeatherbitProvider(client, settings.weatherbit_api_key))

    if settings.weatherstack_api_key:
        providers.append(WeatherstackProvider(client, settings.weatherstack_api_key))

    aggregator = WeatherAggregator(providers)
    return await aggregator.get_aggregated_weather(lat=lat, lon=lon)


@router.get("/health", summary="Проверка живости сервиса")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
