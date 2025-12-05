from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import WeatherProvider, WeatherSample
from app.services.weather_providers.base import BaseWeatherProvider


class OpenMeteoProvider(BaseWeatherProvider):
    """
    https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&current_weather=true
    """

    name = "open_meteo"
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self, client: httpx.AsyncClient) -> None:
        super().__init__(client)

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        params = {
            "latitude": lat,
            "longitude": lon,
            "current_weather": True,
        }

        response = await self._client.get(self.BASE_URL, params=params)  # type: ignore[arg-type]
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        current = data.get("current_weather") or {}

        return WeatherSample(
            provider=WeatherProvider.OPEN_METEO,
            temperature_c=float(current.get("temperature")) if current.get("temperature") is not None else None,  # type: ignore[arg-type]
            wind_speed_kph=(
                float(current.get("windspeed"))  # type: ignore[arg-type]
                if current.get("windspeed") is not None
                else None
            ),
            humidity=None,
            condition=None,
            observation_time=_parse_iso_datetime(current.get("time")),
            raw=data,
        )


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None
