from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import httpx

from app.models.weather import WeatherProvider, WeatherSample
from app.services.weather_providers.base import BaseWeatherProvider


class OpenWeatherMapProvider(BaseWeatherProvider):
    """
    https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API key}
    """

    name = "openweathermap"
    BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

    def __init__(self, client: httpx.AsyncClient, api_key: str) -> None:
        super().__init__(client)
        self._api_key = api_key

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        params = {
            "lat": lat,
            "lon": lon,
            "appid": self._api_key,
            "units": "metric",
        }

        response = await self._client.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data: dict[str, Any] = response.json()

        main = data.get("main") or {}
        wind = data.get("wind") or {}

        temp_c = main.get("temp")
        humidity = main.get("humidity")
        wind_speed_ms = wind.get("speed")
        wind_speed_kph = (
            float(wind_speed_ms) * 3.6 if wind_speed_ms is not None else None
        )

        weather_list = data.get("weather") or []
        condition = (
            weather_list[0].get("description")
            if weather_list and isinstance(weather_list[0], dict)
            else None
        )

        dt_value = data.get("dt")
        observation_time = (
            datetime.fromtimestamp(dt_value, tz=timezone.utc)
            if isinstance(dt_value, (int, float))
            else None
        )

        return WeatherSample(
            provider=WeatherProvider.OPENWEATHER,
            temperature_c=float(temp_c),
            wind_speed_kph=wind_speed_kph,
            humidity=float(humidity) if humidity is not None else None,
            condition=condition,
            observation_time=observation_time,
            raw=data,
        )
