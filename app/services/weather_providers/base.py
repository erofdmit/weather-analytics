from __future__ import annotations

from abc import ABC, abstractmethod

import httpx

from app.models.weather import WeatherSample


class BaseWeatherProvider(ABC):

    name: str

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    @abstractmethod
    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        raise NotImplementedError
