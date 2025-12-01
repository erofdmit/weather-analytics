from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.models.weather import WeatherProvider
from app.services.weather_providers.openweather import OpenWeatherMapProvider
from tests.utils import MockAsyncClient


@pytest.mark.anyio
async def test_openweather_provider_parses_and_converts_units() -> None:
    fake_json = {
        "main": {
            "temp": 15.0,
            "humidity": 82,
        },
        "wind": {
            "speed": 2.0,
        },
        "weather": [
            {"description": "clear sky"},
        ],
        "dt": 1609459200,
    }

    client = MockAsyncClient(fake_json)
    provider = OpenWeatherMapProvider(client, api_key="TEST_KEY")

    sample = await provider.get_weather(lat=10.0, lon=20.0)

    assert sample.provider == WeatherProvider.OPENWEATHER
    assert sample.temperature_c == pytest.approx(15.0)
    assert sample.humidity == pytest.approx(82.0)

    assert sample.wind_speed_kph == pytest.approx(2.0 * 3.6)

    assert sample.condition == "clear sky"
    assert isinstance(sample.observation_time, datetime)
    assert sample.observation_time.tzinfo == timezone.utc

    assert len(client.requests) == 1
    params = client.requests[0]["params"]
    assert params["appid"] == "TEST_KEY"
    assert params["units"] == "metric"
