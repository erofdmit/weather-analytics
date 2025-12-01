from __future__ import annotations

import pytest
from app.api.routes import weather as weather_routes
from app.core.config import Settings, get_settings
from app.models.weather import WeatherProvider, WeatherSample
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient


class FakeOpenMeteoProvider:

    def __init__(self, client: object) -> None:
        self.client = client

    async def get_weather(self, lat: float, lon: float) -> WeatherSample:
        return WeatherSample(
            provider=WeatherProvider.OPEN_METEO,
            temperature_c=10.0,
            wind_speed_kph=5.0,
            humidity=40.0,
            condition="Test condition",
            observation_time=None,
            raw=None,
        )


@pytest.mark.anyio
async def test_health_endpoint(app: FastAPI) -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/weather/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_current_weather_endpoint_aggregates_from_provider(
    app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:

    test_settings = Settings(
        openweather_api_key="",
        weatherapi_api_key="",
        weatherbit_api_key="",
        weatherstack_api_key="",
    )

    app.dependency_overrides[get_settings] = lambda: test_settings

    monkeypatch.setattr(weather_routes, "OpenMeteoProvider", FakeOpenMeteoProvider)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get(
            "/api/weather/current",
            params={"lat": 52.52, "lon": 13.405},
        )

    assert response.status_code == 200
    data = response.json()

    assert data["latitude"] == pytest.approx(52.52)
    assert data["longitude"] == pytest.approx(13.405)

    assert len(data["samples"]) == 1
    sample = data["samples"][0]

    assert sample["provider"] == WeatherProvider.OPEN_METEO.value
    assert sample["temperature_c"] == pytest.approx(10.0)
    assert sample["humidity"] == pytest.approx(40.0)

    assert data["average_temperature_c"] == pytest.approx(10.0)
    assert data["average_humidity"] == pytest.approx(40.0)
