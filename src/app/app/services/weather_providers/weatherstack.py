from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import WeatherProvider, WeatherSample
from app.services.weather_providers.base import BaseWeatherProvider


class WeatherstackProvider(BaseWeatherProvider):
    """
    http://api.weatherstack.com/current
    """

    name = "weatherstack"
    BASE_URL = "http://api.weatherstack.com/current"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        params = {
            "access_key": self._api_key,
            "query": f"{lat},{lon}",
            "units": "m",
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        if "error" in data:
            raise ValueError(f"Weatherstack error: {data['error']}")

        current = data.get("current") or {}
        location = data.get("location") or {}

        temp_c = current.get("temperature")
        humidity = current.get("humidity")
        wind_speed_kph = current.get("wind_speed")

        descriptions = current.get("weather_descriptions") or []
        condition = descriptions[0] if descriptions else None

        localtime = location.get("localtime")
        observation_time = _parse_localtime(localtime)

        return WeatherSample(
            provider=WeatherProvider.WEATHERSTACK,
            temperature_c=float(temp_c),
            wind_speed_kph=(
                float(wind_speed_kph) if wind_speed_kph is not None else None
            ),
            humidity=float(humidity) if humidity is not None else None,
            condition=condition,
            observation_time=observation_time,
            raw=data,
        )


def _parse_localtime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
