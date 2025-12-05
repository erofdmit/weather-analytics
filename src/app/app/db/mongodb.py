"""
Модуль интеграции с MongoDB
"""

import os
from datetime import datetime
from typing import Optional

from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


class MongoDBClient:
    """Клиент для работы с MongoDB"""

    _instance: Optional["MongoDBClient"] = None
    _client: Optional[MongoClient] = None
    _db: Optional[Database] = None
    _weather_collection: Optional[Collection] = None
    _forecast_collection: Optional[Collection] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._connect()

    def _connect(self):
        """Установка соединения с MongoDB"""
        mongo_config = {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD", "admin"),
            "host": os.getenv("MONGO_HOST", "localhost"),
            "port": int(os.getenv("MONGO_PORT", "27017")),
            "authSource": "admin",
        }

        logger.info(
            f"Подключение к MongoDB: {mongo_config['host']}:{mongo_config['port']}"
        )

        try:
            self._client = MongoClient(**mongo_config)
            self._db = self._client["weather_analytics_db"]
            self._weather_collection = self._db["weather_current"]
            self._forecast_collection = self._db["weather_forecast"]

            # Проверка соединения
            self._client.admin.command("ping")
            logger.info("Успешно подключились к MongoDB")
        except Exception as e:
            logger.error(f"Не удалось подключиться к MongoDB: {e}")
            # Не бросаем исключение, позволяем приложению работать без MongoDB
            self._client = None

    @property
    def weather_collection(self) -> Optional[Collection]:
        """Коллекция текущей погоды"""
        return self._weather_collection

    @property
    def forecast_collection(self) -> Optional[Collection]:
        """Коллекция прогнозов"""
        return self._forecast_collection

    def save_current_weather(
        self,
        latitude: float,
        longitude: float,
        request_data: dict,
        response_data: dict,
        status_code: int,
        error_message: Optional[str] = None,
    ) -> bool:
        """Сохранение текущей погоды в MongoDB"""
        if self._weather_collection is None:
            logger.warning("Коллекция погоды недоступна, пропускаем сохранение")
            return False

        try:
            document = {
                "type": "current",
                "latitude": latitude,
                "longitude": longitude,
                "request": request_data,
                "response": response_data,
                "status_code": status_code,
                "error_message": error_message,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }

            self._weather_collection.insert_one(document)
            logger.info(f"Сохранена текущая погода: lat={latitude}, lon={longitude}")
            return True

        except Exception as e:
            logger.error(f"Ошибка сохранения текущей погоды: {e}")
            return False

    def save_forecast(
        self,
        latitude: float,
        longitude: float,
        hours: int,
        request_data: dict,
        response_data: dict,
        status_code: int,
        error_message: Optional[str] = None,
    ) -> bool:
        """Сохранение прогноза в MongoDB"""
        if self._forecast_collection is None:
            logger.warning("Коллекция прогнозов недоступна, пропускаем сохранение")
            return False

        try:
            document = {
                "type": "forecast",
                "latitude": latitude,
                "longitude": longitude,
                "hours": hours,
                "request": request_data,
                "response": response_data,
                "status_code": status_code,
                "error_message": error_message,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }

            self._forecast_collection.insert_one(document)
            logger.info(
                f"Сохранён прогноз: lat={latitude}, lon={longitude}, hours={hours}"
            )
            return True

        except Exception as e:
            logger.error(f"Ошибка сохранения прогноза: {e}")
            return False

    def get_recent_current_weather(self, limit: int = 10) -> list:
        """Получение последних записей текущей погоды"""
        if self._weather_collection is None:
            logger.warning("Коллекция погоды недоступна")
            return []

        try:
            cursor = self._weather_collection.find(
                {}, sort=[("created_at", -1)], limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Ошибка получения текущей погоды: {e}")
            return []

    def get_recent_forecasts(self, limit: int = 10) -> list:
        """Получение последних прогнозов"""
        if self._forecast_collection is None:
            logger.warning("Коллекция прогнозов недоступна")
            return []

        try:
            cursor = self._forecast_collection.find(
                {}, sort=[("created_at", -1)], limit=limit
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Ошибка получения прогнозов: {e}")
            return []

    def get_current_weather_by_location(
        self, latitude: float, longitude: float, limit: int = 10
    ) -> list:
        """Получение текущей погоды для координат"""
        if self._weather_collection is None:
            logger.warning("Коллекция погоды недоступна")
            return []

        try:
            cursor = self._weather_collection.find(
                {"latitude": latitude, "longitude": longitude, "status_code": 200},
                sort=[("created_at", -1)],
                limit=limit,
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Ошибка получения текущей погоды: {e}")
            return []

    def get_forecasts_by_location(
        self, latitude: float, longitude: float, limit: int = 10
    ) -> list:
        """Получение прогнозов для координат"""
        if self._forecast_collection is None:
            logger.warning("Коллекция прогнозов недоступна")
            return []

        try:
            cursor = self._forecast_collection.find(
                {"latitude": latitude, "longitude": longitude, "status_code": 200},
                sort=[("created_at", -1)],
                limit=limit,
            )
            return list(cursor)
        except Exception as e:
            logger.error(f"Ошибка получения прогнозов: {e}")
            return []

    def close(self):
        """Закрытие соединения с MongoDB"""
        if self._client:
            self._client.close()
            logger.info("Соединение с MongoDB закрыто")


# Глобальный экземпляр клиента
mongo_client = MongoDBClient()
