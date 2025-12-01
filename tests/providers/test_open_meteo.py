from __future__ import annotations

from datetime import datetime

import pytest

from app.models.weather import WeatherProvider
from app.services.weather_providers.open_meteo import OpenMeteoProvider
from tests.utils import MockAsyncClient


@pytest.mark.anyio
async def test_open_meteo_provider_parses_response() -> None:
    fake_json = {
        "latitude": 52.52,
        "longitude": 13.405,
        "current_weather": {
            "temperature": 18.5,
            "windspeed": 10.0,
            "time": "2025-12-01T10:00:00",
        },
    }

    client = MockAsyncClient(fake_json)
    provider = OpenMeteoProvider(client)

    sample = await provider.get_weather(lat=52.52, lon=13.405)

    assert sample.provider == WeatherProvider.OPEN_METEO
    assert sample.temperature_c == pytest.approx(18.5)
    assert sample.wind_speed_kph == pytest.approx(10.0)
    assert sample.humidity is None
    assert isinstance(sample.observation_time, datetime)

    assert len(client.requests) == 1
    params = client.requests[0]["params"]
    assert params["latitude"] == 52.52
    assert params["longitude"] == 13.405
    assert params["current_weather"] is True
