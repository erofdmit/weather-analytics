from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class WeatherProvider(str, Enum):
    OPEN_METEO = "open_meteo"
    OPENWEATHER = "openweathermap"
    WEATHERAPI = "weatherapi"
    WEATHERBIT = "weatherbit"
    WEATHERSTACK = "weatherstack"


class WeatherSample(BaseModel):
    provider: WeatherProvider = Field(..., description="Имя провайдера данных")
    temperature_c: float = Field(..., description="Температура воздуха, °C")
    wind_speed_kph: float | None = Field(None, description="Скорость ветра, км/ч")
    humidity: float | None = Field(None, description="Относительная влажность, %")
    condition: str | None = Field(
        None, description="Человеко-читаемое описание погодных условий"
    )
    observation_time: datetime | None = Field(
        None, description="Время измерения (как его отдал провайдер)"
    )

    raw: dict[str, Any] | None = Field(
        default=None,
        description="Опционально — сырые данные провайдера (для дебага)",
    )


class AggregatedWeatherResponse(BaseModel):

    latitude: float
    longitude: float
    samples: list[WeatherSample]

    average_temperature_c: float | None = Field(
        None, description="Средняя температура по всем провайдерам"
    )
    average_humidity: float | None = Field(
        None, description="Средняя влажность по всем провайдерам"
    )
