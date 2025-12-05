from __future__ import annotations

from datetime import datetime
from datetime import time as dtime
from math import ceil
from typing import Any

import httpx

from app.models.weather import ForecastPoint, ProviderForecast, WeatherProvider
from app.services.weather_providers.base import BaseForecastProvider


class WeatherstackForecastProvider(BaseForecastProvider):
    """

    http://api.weatherstack.com/forecast?access_key={KEY}&query={lat,lon}&forecast_days={N}&hourly=1&units=m


    """

    name = "weatherstack_forecast"
    BASE_URL = "http://api.weatherstack.com/forecast"

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
            "access_key": self._api_key,
            "query": f"{lat},{lon}",
            "forecast_days": days,
            "hourly": 1,
            "units": "m",
        }

        response = await self._client.get(self.BASE_URL, params=params)  # type: ignore[arg-type]
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        if "error" in data:
            raise ValueError(f"Weatherstack forecast error: {data['error']}")

        forecast = data.get("forecast") or {}

        points: list[ForecastPoint] = []
        for date_str, day_data in forecast.items():
            hourly_list = day_data.get("hourly") or []
            for h in hourly_list:
                temp = h.get("temperature")
                hum = h.get("humidity")
                wind_kph = h.get("wind_speed")

                if temp is None:
                    continue

                t_value = h.get("time")
                dt = _combine_date_and_time_string(date_str, t_value)

                points.append(
                    ForecastPoint(
                        time=dt,
                        temperature_c=float(temp),
                        humidity=float(hum) if hum is not None else None,
                        wind_speed_kph=(
                            float(wind_kph) if wind_kph is not None else None
                        ),
                    )
                )

        if len(points) > hours:
            points = points[:hours]

        return ProviderForecast(
            provider=WeatherProvider.WEATHERSTACK,
            points=points,
        )


def _combine_date_and_time_string(date_str: str, time_str: str | None) -> datetime:
    try:
        base_date = datetime.fromisoformat(date_str).date()
    except Exception:
        return datetime.now()

    try:
        minutes = int(time_str or "0")
    except Exception:
        minutes = 0

    hours = minutes // 100 if minutes >= 100 else minutes // 60
    t = dtime(hour=hours)
    return datetime.combine(base_date, t)
