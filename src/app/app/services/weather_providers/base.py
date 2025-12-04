from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from app.models.weather import WeatherSample, ProviderForecast


class BaseWeatherProvider(ABC):
    """Базовый класс провайдера погодных данных (текущая погода)."""

    name: str

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @abstractmethod
    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        """Получить текущую погоду по координатам."""
        raise NotImplementedError


class BaseForecastProvider(ABC):
    """Базовый класс провайдера прогноза погоды."""

    name: str

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @abstractmethod
    async def get_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> ProviderForecast:
        """Получить прогноз по координатам на указанное число часов."""
        raise NotImplementedError
