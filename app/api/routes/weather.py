from __future__ import annotations

from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Query, Request

from app.core.config import Settings, get_settings
from app.models.weather import (
    AggregatedForecastResponse,
    AggregatedWeatherResponse,
)
from app.services.aggregator import ForecastAggregator, WeatherAggregator
from app.services.weather_providers.open_meteo import OpenMeteoProvider
from app.services.weather_providers.open_meteo_forecast import (
    OpenMeteoForecastProvider,
)
from app.services.weather_providers.openweather import OpenWeatherMapProvider
from app.services.weather_providers.openweather_forecast import (
    OpenWeatherMapForecastProvider,
)
from app.services.weather_providers.weatherapi import WeatherAPIProvider
from app.services.weather_providers.weatherapi_forecast import (
    WeatherAPIForecastProvider,
)
from app.services.weather_providers.weatherbit import WeatherbitProvider
from app.services.weather_providers.weatherbit_forecast import (
    WeatherbitForecastProvider,
)
from app.services.weather_providers.weatherstack import WeatherstackProvider
from app.services.weather_providers.weatherstack_forecast import (
    WeatherstackForecastProvider,
)

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
    settings: Settings = Depends(get_settings),
) -> AggregatedWeatherResponse:
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


@router.get(
    "/forecast",
    response_model=AggregatedForecastResponse,
    summary="Прогноз погоды по координатам (часовой) из нескольких провайдеров",
)
async def get_forecast_weather(
    request: Request,
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота"),
    hours: int = Query(
        24,
        ge=1,
        le=168,
        description="Горизонт прогноза в часах (1–168)",
    ),
    settings: Settings = Depends(get_settings),  # noqa: B008
) -> AggregatedForecastResponse:
    client = _get_http_client(request)

    providers = [OpenMeteoForecastProvider(client)]

    if settings.openweather_api_key:
        providers.append(
            OpenWeatherMapForecastProvider(client, settings.openweather_api_key)
        )

    if settings.weatherapi_api_key:
        providers.append(
            WeatherAPIForecastProvider(client, settings.weatherapi_api_key)
        )

    if settings.weatherbit_api_key:
        providers.append(
            WeatherbitForecastProvider(client, settings.weatherbit_api_key)
        )

    if settings.weatherstack_api_key:
        providers.append(
            WeatherstackForecastProvider(client, settings.weatherstack_api_key)
        )

    aggregator = ForecastAggregator(providers)
    return await aggregator.get_aggregated_forecast(
        lat=lat,
        lon=lon,
        hours=hours,
    )


@router.get("/health", summary="Проверка живости сервиса")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}
