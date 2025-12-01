from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_prefix="",
    )
    
    openweather_api_key: str = ""
    weatherapi_api_key: str = ""
    weatherbit_api_key: str = ""
    weatherstack_api_key: str = ""

    http_timeout: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
