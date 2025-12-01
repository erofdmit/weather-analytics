from __future__ import annotations

from datetime import datetime

import pytest
from app.models.weather import WeatherProvider
from app.services.weather_providers.weatherbit import WeatherbitProvider
from tests.utils import MockAsyncClient


@pytest.mark.anyio
async def test_weatherbit_provider_parses_and_converts_wind_speed() -> None:
    fake_json = {
        "data": [
            {
                "temp": 17.0,
                "rh": 60,
                "wind_spd": 3.0,
                "weather": {"description": "Light rain"},
                "ob_time": "2025-12-01 12:15",
            }
        ],
        "count": 1,
    }

    client = MockAsyncClient(fake_json)
    provider = WeatherbitProvider(client, api_key="TEST_KEY")

    sample = await provider.get_weather(lat=40.0, lon=30.0)

    assert sample.provider == WeatherProvider.WEATHERBIT
    assert sample.temperature_c == pytest.approx(17.0)
    assert sample.humidity == pytest.approx(60.0)

    assert sample.wind_speed_kph == pytest.approx(3.0 * 3.6)
    assert sample.condition == "Light rain"
    assert isinstance(sample.observation_time, datetime)

    assert len(client.requests) == 1
    params = client.requests[0]["params"]
    assert params["lat"] == 40.0
    assert params["lon"] == 30.0
    assert params["key"] == "TEST_KEY"


@pytest.mark.anyio
async def test_weatherbit_provider_fails_on_empty_data() -> None:
    client = MockAsyncClient({"data": []})
    provider = WeatherbitProvider(client, api_key="TEST_KEY")

    with pytest.raises(ValueError):
        await provider.get_weather(lat=0.0, lon=0.0)
