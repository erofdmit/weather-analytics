from __future__ import annotations

import pytest

from app.models.weather import AggregatedWeatherResponse, WeatherProvider, WeatherSample
from app.services.aggregator import WeatherAggregator


class DummyProvider:

    def __init__(self, sample: WeatherSample) -> None:
        self._sample = sample

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        return self._sample


class ErrorProvider:

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        raise RuntimeError("provider failed")


@pytest.mark.anyio
async def test_aggregator_computes_average_temperature_and_humidity() -> None:
    sample1 = WeatherSample(
        provider=WeatherProvider.OPEN_METEO,
        temperature_c=10.0,
        wind_speed_kph=5.0,
        humidity=50.0,
        condition="Cloudy",
        observation_time=None,
        raw=None,
    )
    sample2 = WeatherSample(
        provider=WeatherProvider.OPENWEATHER,
        temperature_c=20.0,
        wind_speed_kph=10.0,
        humidity=70.0,
        condition="Sunny",
        observation_time=None,
        raw=None,
    )

    aggregator = WeatherAggregator(
        providers=[DummyProvider(sample1), DummyProvider(sample2)]
    )

    result: AggregatedWeatherResponse = await aggregator.get_aggregated_weather(
        lat=52.52, lon=13.405
    )

    assert result.latitude == 52.52
    assert result.longitude == 13.405
    assert len(result.samples) == 2

    assert result.average_temperature_c == pytest.approx(15.0)
    assert result.average_humidity == pytest.approx(60.0)


@pytest.mark.anyio
async def test_aggregator_skips_failed_providers() -> None:
    sample_ok = WeatherSample(
        provider=WeatherProvider.OPEN_METEO,
        temperature_c=12.0,
        wind_speed_kph=3.0,
        humidity=40.0,
        condition="OK",
        observation_time=None,
        raw=None,
    )

    aggregator = WeatherAggregator(
        providers=[DummyProvider(sample_ok), ErrorProvider()]
    )

    result = await aggregator.get_aggregated_weather(lat=1.0, lon=2.0)

    assert len(result.samples) == 1
    assert result.samples[0].temperature_c == pytest.approx(12.0)
    assert result.average_temperature_c == pytest.approx(12.0)
    assert result.average_humidity == pytest.approx(40.0)
