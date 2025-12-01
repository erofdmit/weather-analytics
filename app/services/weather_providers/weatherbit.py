from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import WeatherProvider, WeatherSample
from app.services.weather_providers.base import BaseWeatherProvider


class WeatherbitProvider(BaseWeatherProvider):
    """
    https://api.weatherbit.io/v2.0/current
    """

    name = "weatherbit"
    BASE_URL = "https://api.weatherbit.io/v2.0/current"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        params = {
            "lat": lat,
            "lon": lon,
            "key": self._api_key,
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        data_list = data.get("data") or []
        if not data_list:
            raise ValueError("Weatherbit: пустой список 'data' в ответе")

        current = data_list[0]

        temp_c = current.get("temp")
        humidity = current.get("rh")
        wind_spd_ms = current.get("wind_spd")
        weather_obj = current.get("weather") or {}
        condition = weather_obj.get("description")

        wind_speed_kph = (
            float(wind_spd_ms) * 3.6 if wind_spd_ms is not None else None
        )

        ob_time = current.get("ob_time")
        observation_time = _parse_ob_time(ob_time)

        return WeatherSample(
            provider=WeatherProvider.WEATHERBIT,
            temperature_c=float(temp_c),
            wind_speed_kph=wind_speed_kph,
            humidity=float(humidity) if humidity is not None else None,
            condition=condition,
            observation_time=observation_time,
            raw=data,
        )


def _parse_ob_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
