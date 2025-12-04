from __future__ import annotations

from datetime import datetime
from math import ceil
from typing import Any

import httpx

from app.models.weather import ForecastPoint, ProviderForecast, WeatherProvider
from app.services.weather_providers.base import BaseForecastProvider


class WeatherAPIForecastProvider(BaseForecastProvider):
    """

      https://api.weatherapi.com/v1/forecast.json?key={KEY}&q={lat},{lon}&days={days}

    """

    name = "weatherapi_forecast"
    BASE_URL = "https://api.weatherapi.com/v1/forecast.json"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> ProviderForecast:
        days = max(1, ceil(hours / 24))

        params = {
            "key": self._api_key,
            "q": f"{lat},{lon}",
            "days": days,
            "aqi": "no",
            "alerts": "no",
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        forecast = data.get("forecast") or {}
        forecast_days = forecast.get("forecastday") or []

        points: list[ForecastPoint] = []
        for day in forecast_days:
            hours_list = day.get("hour") or []
            for h in hours_list:
                t_raw = h.get("time")
                temp_c = h.get("temp_c")
                hum = h.get("humidity")
                wind_kph = h.get("wind_kph")

                if temp_c is None:
                    continue

                points.append(
                    ForecastPoint(
                        time=_parse_time(t_raw),
                        temperature_c=float(temp_c),
                        humidity=float(hum) if hum is not None else None,
                        wind_speed_kph=float(wind_kph)
                        if wind_kph is not None
                        else None,
                    )
                )

        if len(points) > hours:
            points = points[:hours]

        return ProviderForecast(
            provider=WeatherProvider.WEATHERAPI,
            points=points,
        )


def _parse_time(value: str | None) -> datetime:
    if not value:
        return datetime.now()
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.now()
