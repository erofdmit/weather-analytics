from __future__ import annotations

import json
from typing import Annotated

import httpx
from bson import json_util
from fastapi import APIRouter, Depends, Query, Request
from loguru import logger

from app.core.config import Settings, get_settings
from app.db.mongodb import mongo_client
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
    settings: Settings = Depends(get_settings),  # noqa: B008
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
    result = await aggregator.get_aggregated_weather(lat=lat, lon=lon)
    
    # Save to MongoDB
    try:
        mongo_client.save_current_weather(
            latitude=lat,
            longitude=lon,
            request_data={"lat": lat, "lon": lon},
            response_data=result.model_dump(),
            status_code=200,
            error_message=None
        )
    except Exception as e:
        logger.error(f"Failed to save current weather to MongoDB: {e}")
    
    return result


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
    result = await aggregator.get_aggregated_forecast(
        lat=lat,
        lon=lon,
        hours=hours,
    )
    
    # Save to MongoDB
    try:
        mongo_client.save_forecast(
            latitude=lat,
            longitude=lon,
            hours=hours,
            request_data={"lat": lat, "lon": lon, "hours": hours},
            response_data=result.model_dump(),
            status_code=200,
            error_message=None
        )
    except Exception as e:
        logger.error(f"Failed to save forecast to MongoDB: {e}")
    
    return result


@router.get("/health", summary="Проверка живости сервиса")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/history/current/recent", summary="Последние записи текущей погоды из MongoDB")
async def get_recent_current_weather_history(
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """Получить последние записи текущей погоды из MongoDB"""
    try:
        data = mongo_client.get_recent_current_weather(limit=limit)
        data_json = json.loads(json_util.dumps(data))
        return {
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch recent current weather: {e}")
        return {"error": str(e), "count": 0, "data": []}


@router.get("/history/current/location", summary="История текущей погоды по координатам")
async def get_current_weather_history_by_location(
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота"),
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """Получить историю текущей погоды для конкретных координат из MongoDB"""
    try:
        data = mongo_client.get_current_weather_by_location(
            latitude=lat,
            longitude=lon,
            limit=limit
        )
        data_json = json.loads(json_util.dumps(data))
        return {
            "latitude": lat,
            "longitude": lon,
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch current weather history: {e}")
        return {"error": str(e), "count": 0, "data": []}


@router.get("/history/forecast/recent", summary="Последние прогнозы из MongoDB")
async def get_recent_forecasts_history(
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """Получить последние прогнозы из MongoDB"""
    try:
        data = mongo_client.get_recent_forecasts(limit=limit)
        data_json = json.loads(json_util.dumps(data))
        return {
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch recent forecasts: {e}")
        return {"error": str(e), "count": 0, "data": []}


@router.get("/history/forecast/location", summary="История прогнозов по координатам")
async def get_forecast_history_by_location(
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота"),
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """Получить историю прогнозов для конкретных координат из MongoDB"""
    try:
        data = mongo_client.get_forecasts_by_location(
            latitude=lat,
            longitude=lon,
            limit=limit
        )
        data_json = json.loads(json_util.dumps(data))
        return {
            "latitude": lat,
            "longitude": lon,
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch forecast history: {e}")
        return {"error": str(e), "count": 0, "data": []}