from __future__ import annotations

from datetime import datetime, timezone
from math import ceil
from typing import Any

import httpx

from app.models.weather import ForecastPoint, ProviderForecast, WeatherProvider
from app.services.weather_providers.base import BaseForecastProvider


class OpenWeatherMapForecastProvider(BaseForecastProvider):
    """

    Эндпоинт:
      https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={API key}&units=metric

    """

    name = "openweathermap_forecast"
    BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> ProviderForecast:
        max_points = ceil(min(hours, 5 * 24) / 3)

        params = {
            "lat": lat,
            "lon": lon,
            "appid": self._api_key,
            "units": "metric",
            "cnt": max_points,
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        list_items = data.get("list") or []

        points: list[ForecastPoint] = []
        for item in list_items:
            main = item.get("main") or {}
            wind = item.get("wind") or {}

            temp = main.get("temp")
            hum = main.get("humidity")
            wind_ms = wind.get("speed")
            wind_kph = (
                float(wind_ms) * 3.6 if wind_ms is not None else None
            )

            dt_val = item.get("dt")
            if isinstance(dt_val, (int, float)):
                t = datetime.fromtimestamp(dt_val, tz=timezone.utc)
            else:
                t = datetime.now(timezone.utc)

            points.append(
                ForecastPoint(
                    time=t,
                    temperature_c=float(temp),
                    humidity=float(hum) if hum is not None else None,
                    wind_speed_kph=wind_kph,
                )
            )

        return ProviderForecast(
            provider=WeatherProvider.OPENWEATHER,
            points=points,
        )
