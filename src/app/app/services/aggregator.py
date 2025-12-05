import asyncio
from statistics import mean
from typing import Iterable, List

from app.models.weather import (
    AggregatedForecastResponse,
    AggregatedWeatherResponse,
    ProviderForecast,
    WeatherSample,
)
from app.services.weather_providers.base import (
    BaseForecastProvider,
    BaseWeatherProvider,
)


class WeatherAggregator:
    def __init__(self, providers: Iterable[BaseWeatherProvider]) -> None:
        self._providers: List[BaseWeatherProvider] = list(providers)

    async def get_aggregated_weather(
        self,
        lat: float,
        lon: float,
    ) -> AggregatedWeatherResponse:
        tasks = [p.get_weather(lat, lon) for p in self._providers]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        samples: list[WeatherSample] = []
        for result in results:
            if isinstance(result, Exception):
                continue
            if isinstance(result, WeatherSample):
                samples.append(result)

        avg_temp = _safe_mean(
            sample.temperature_c
            for sample in samples
            if sample.temperature_c is not None
        )
        avg_humidity = _safe_mean(
            sample.humidity for sample in samples if sample.humidity is not None
        )

        return AggregatedWeatherResponse(
            latitude=lat,
            longitude=lon,
            samples=samples,
            average_temperature_c=avg_temp,
            average_humidity=avg_humidity,
        )


def _safe_mean(values: Iterable[float]) -> float | None:
    values_list = [v for v in values]
    if not values_list:
        return None
    return float(mean(values_list))


class ForecastAggregator:
    """Агрегатор прогнозов погоды от нескольких провайдеров"""

    def __init__(self, providers: Iterable[BaseForecastProvider]) -> None:
        self._providers: list[BaseForecastProvider] = list(providers)

    async def get_aggregated_forecast(
        self,
        lat: float,
        lon: float,
        hours: int,
    ) -> AggregatedForecastResponse:
        tasks = [p.get_forecast(lat, lon, hours) for p in self._providers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        forecasts: list[ProviderForecast] = []
        for result in results:
            if isinstance(result, Exception):
                # Пропускаем ошибки
                continue
            if isinstance(result, ProviderForecast):
                forecasts.append(result)

        return AggregatedForecastResponse(
            latitude=lat,
            longitude=lon,
            hours=hours,
            forecasts=forecasts,
        )
