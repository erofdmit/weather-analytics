from __future__ import annotations

from datetime import datetime
from typing import Any

import httpx

from app.models.weather import ForecastPoint, ProviderForecast, WeatherProvider
from app.services.weather_providers.base import BaseForecastProvider


class OpenMeteoForecastProvider(BaseForecastProvider):
    """

      https://api.open-meteo.com/v1/forecast?latitude=...&longitude=...&
      hourly=temperature_2m,relativehumidity_2m,windspeed_10m&forecast_hours=...

    """

    name = "open_meteo_forecast"
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    async def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> ProviderForecast:
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
            "forecast_hours": hours,
            "timezone": "auto",
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        hourly = data.get("hourly") or {}
        times: list[str] = hourly.get("time") or []
        temps: list[float] = hourly.get("temperature_2m") or []
        hums: list[float] = hourly.get("relativehumidity_2m") or []
        winds: list[float] = hourly.get("windspeed_10m") or []

        points: list[ForecastPoint] = []
        count = min(len(times), len(temps))
        for idx in range(count):
            t_raw = times[idx]
            temp = temps[idx]
            hum = hums[idx] if idx < len(hums) else None
            wind = winds[idx] if idx < len(winds) else None

            points.append(
                ForecastPoint(
                    time=_parse_iso(t_raw),
                    temperature_c=float(temp),
                    humidity=float(hum) if hum is not None else None,
                    wind_speed_kph=float(wind) if wind is not None else None,
                )
            )

        return ProviderForecast(
            provider=WeatherProvider.OPEN_METEO,
            points=points,
        )


def _parse_iso(value: str | None) -> datetime:
    if not value:
        return datetime.now()
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.now()
