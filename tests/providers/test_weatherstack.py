from __future__ import annotations

from datetime import datetime

import pytest
from app.models.weather import WeatherProvider
from app.services.weather_providers.weatherstack import WeatherstackProvider
from tests.utils import MockAsyncClient


@pytest.mark.anyio
async def test_weatherstack_provider_parses_response() -> None:
    fake_json = {
        "location": {
            "name": "Amsterdam",
            "localtime": "2025-12-01 10:45",
        },
        "current": {
            "temperature": 9,
            "humidity": 80,
            "wind_speed": 15,
            "weather_descriptions": ["Overcast"],
        },
    }

    client = MockAsyncClient(fake_json)
    provider = WeatherstackProvider(client, api_key="TEST_KEY")

    sample = await provider.get_weather(lat=52.37, lon=4.9)

    assert sample.provider == WeatherProvider.WEATHERSTACK
    assert sample.temperature_c == pytest.approx(9.0)
    assert sample.humidity == pytest.approx(80.0)
    assert sample.wind_speed_kph == pytest.approx(15.0)
    assert sample.condition == "Overcast"
    assert isinstance(sample.observation_time, datetime)

    assert len(client.requests) == 1
    params = client.requests[0]["params"]
    assert params["access_key"] == "TEST_KEY"
    assert params["query"] == "52.37,4.9"
    assert params["units"] == "m"


@pytest.mark.anyio
async def test_weatherstack_provider_raises_on_error_field() -> None:
    fake_json = {
        "error": {
            "code": 101,
            "info": "Invalid access key",
        }
    }

    client = MockAsyncClient(fake_json)
    provider = WeatherstackProvider(client, api_key="BAD_KEY")

    with pytest.raises(ValueError):
        await provider.get_weather(lat=0.0, lon=0.0)
