from __future__ import annotations

import json

import httpx
from app.core.config import Settings, get_settings
from app.db.mongodb import mongo_client
from app.models.weather import AggregatedWeatherResponse
from app.services.aggregator import WeatherAggregator
from app.services.weather_providers.open_meteo import OpenMeteoProvider
from app.services.weather_providers.openweather import OpenWeatherMapProvider
from app.services.weather_providers.weatherapi import WeatherAPIProvider
from app.services.weather_providers.weatherbit import WeatherbitProvider
from app.services.weather_providers.weatherstack import WeatherstackProvider
from bson import json_util
from fastapi import APIRouter, Depends, Query, Request
from loguru import logger

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
    result = await aggregator.get_aggregated_weather(lat=lat, lon=lon)
    
    # Save request and response to MongoDB
    try:
        mongo_client.save_weather_request(
            latitude=lat,
            longitude=lon,
            request_data={"lat": lat, "lon": lon},
            response_data=result.model_dump(),
            status_code=200,
            error_message=None
        )
    except Exception as e:
        logger.error(f"Failed to save weather data to MongoDB: {e}")
        # Don't fail the request if MongoDB save fails
    
    return result


@router.get("/health", summary="Проверка health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/history/recent", summary="Последние записи из MongoDB")
async def get_recent_history(
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """
    Получить последние записи погоды из MongoDB
    """
    try:
        data = mongo_client.get_recent_weather_data(limit=limit)
        # Convert MongoDB ObjectId to string
        data_json = json.loads(json_util.dumps(data))
        return {
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch recent history: {e}")
        return {"error": str(e), "count": 0, "data": []}


@router.get("/history/location", summary="История по координатам")
async def get_location_history(
    lat: float = Query(..., description="Широта"),
    lon: float = Query(..., description="Долгота"),
    limit: int = Query(10, description="Количество записей", ge=1, le=100)
) -> dict:
    """
    Получить историю погоды для конкретных координат из MongoDB
    """
    try:
        data = mongo_client.get_weather_by_location(
            latitude=lat,
            longitude=lon,
            limit=limit
        )
        # Convert MongoDB ObjectId to string
        data_json = json.loads(json_util.dumps(data))
        return {
            "latitude": lat,
            "longitude": lon,
            "count": len(data_json),
            "data": data_json
        }
    except Exception as e:
        logger.error(f"Failed to fetch location history: {e}")
        return {"error": str(e), "count": 0, "data": []}
