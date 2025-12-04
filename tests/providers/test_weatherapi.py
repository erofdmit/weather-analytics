from __future__ import annotations

from datetime import datetime

import pytest
from app.models.weather import WeatherProvider
from app.services.weather_providers.weatherapi import WeatherAPIProvider
from tests.utils import MockAsyncClient


@pytest.mark.anyio
async def test_weatherapi_provider_parses_response() -> None:
    fake_json = {
        "location": {
            "name": "Berlin",
        },
        "current": {
            "temp_c": 21.3,
            "humidity": 55,
            "wind_kph": 12.0,
            "condition": {"text": "Partly cloudy"},
            "last_updated": "2025-12-01 14:30",
        },
    }

    client = MockAsyncClient(fake_json)
    provider = WeatherAPIProvider(client, api_key="TEST_KEY")

    sample = await provider.get_weather(lat=52.52, lon=13.405)

    assert sample.provider == WeatherProvider.WEATHERAPI
    assert sample.temperature_c == pytest.approx(21.3)
    assert sample.humidity == pytest.approx(55.0)
    assert sample.wind_speed_kph == pytest.approx(12.0)
    assert sample.condition == "Partly cloudy"
    assert isinstance(sample.observation_time, datetime)

    assert len(client.requests) == 1
    params = client.requests[0]["params"]
    assert params["key"] == "TEST_KEY"
    assert params["q"] == "52.52,13.405"
    assert params["aqi"] == "no"
