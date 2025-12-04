from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import ForecastPoint, ProviderForecast, WeatherProvider
from app.services.weather_providers.base import BaseForecastProvider


class WeatherbitForecastProvider(BaseForecastProvider):
    """

      https://api.weatherbit.io/v2.0/forecast/hourly?lat={lat}&lon={lon}&key={KEY}&hours={H}

    """

    name = "weatherbit_forecast"
    BASE_URL = "https://api.weatherbit.io/v2.0/forecast/hourly"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> ProviderForecast:
        params = {
            "lat": lat,
            "lon": lon,
            "key": self._api_key,
            "hours": hours,
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        data_list = data.get("data") or []

        points: list[ForecastPoint] = []
        for item in data_list:
            temp = item.get("temp")
            hum = item.get("rh")
            wind_ms = item.get("wind_spd")
            t_raw = item.get("timestamp_local") or item.get("timestamp_utc")

            if temp is None:
                continue

            wind_kph = float(wind_ms) * 3.6 if wind_ms is not None else None

            points.append(
                ForecastPoint(
                    time=_parse_time(t_raw),
                    temperature_c=float(temp),
                    humidity=float(hum) if hum is not None else None,
                    wind_speed_kph=wind_kph,
                )
            )

        return ProviderForecast(
            provider=WeatherProvider.WEATHERBIT,
            points=points,
        )


def _parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.now()
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.now()
