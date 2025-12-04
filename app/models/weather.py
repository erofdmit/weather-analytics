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
    """Нормализованная запись о погоде от одного провайдера (текущее состояние)."""

    provider: WeatherProvider = Field(..., description="Имя провайдера данных")
    temperature_c: float = Field(..., description="Температура воздуха, °C")
    wind_speed_kph: float | None = Field(
        None, description="Скорость ветра, км/ч"
    )
    humidity: float | None = Field(
        None, description="Относительная влажность, %"
    )
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
    """Ответ API cо сводной информацией по нескольким провайдерам (текущее состояние)."""

    latitude: float
    longitude: float
    samples: list[WeatherSample]

    average_temperature_c: float | None = Field(
        None, description="Средняя температура по всем провайдерам"
    )
    average_humidity: float | None = Field(
        None, description="Средняя влажность по всем провайдерам"
    )


# ====== Модели для прогноза ======


class ForecastPoint(BaseModel):
    """Точка прогноза в конкретный момент времени."""

    time: datetime = Field(..., description="Момент времени прогноза")
    temperature_c: float = Field(..., description="Температура, °C")
    wind_speed_kph: float | None = Field(
        None, description="Скорость ветра, км/ч"
    )
    humidity: float | None = Field(
        None, description="Относительная влажность, %"
    )


class ProviderForecast(BaseModel):
    """Прогноз от одного провайдера."""

    provider: WeatherProvider = Field(..., description="Имя провайдера")
    points: list[ForecastPoint] = Field(
        ..., description="Список точек прогноза"
    )


class AggregatedForecastResponse(BaseModel):
    """Ответ API c прогнозом погоды от нескольких провайдеров."""

    latitude: float
    longitude: float
    hours: int = Field(..., description="Горизонт прогноза в часах")
    forecasts: list[ProviderForecast] = Field(
        ..., description="Список прогнозов от провайдеров"
    )
