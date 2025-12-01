from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import WeatherProvider, WeatherSample
from app.services.weather_providers.base import BaseWeatherProvider


class WeatherAPIProvider(BaseWeatherProvider):
    """
    https://api.weatherapi.com/v1/current.json?key={KEY}&q={lat},{lon}&aqi=no
    """

    name = "weatherapi"
    BASE_URL = "https://api.weatherapi.com/v1/current.json"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        params = {
            "key": self._api_key,
            "q": f"{lat},{lon}",
            "aqi": "no",
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        current = data.get("current") or {}

        temp_c = current.get("temp_c")
        humidity = current.get("humidity")
        wind_kph = current.get("wind_kph")

        condition_obj = current.get("condition") or {}
        condition = condition_obj.get("text")

        last_updated = current.get("last_updated")
        observation_time = _parse_weatherapi_datetime(last_updated)

        return WeatherSample(
            provider=WeatherProvider.WEATHERAPI,
            temperature_c=float(temp_c),
            wind_speed_kph=float(wind_kph) if wind_kph is not None else None,
            humidity=float(humidity) if humidity is not None else None,
            condition=condition,
            observation_time=observation_time,
            raw=data,
        )


def _parse_weatherapi_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
