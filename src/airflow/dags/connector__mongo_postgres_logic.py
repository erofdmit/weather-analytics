"""
ETL логика для переноса данных из MongoDB в PostgreSQL DWH
"""

import os
from datetime import datetime, timedelta

from dotenv import load_dotenv
from loguru import logger
from pymongo import MongoClient
from sqlalchemy import Column, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

load_dotenv()


def convert_datetime_to_str(obj):
    """Рекурсивно конвертирует datetime в строки в словарях и списках"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: convert_datetime_to_str(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_str(item) for item in obj]
    return obj


def get_config():
    """Получение конфигурации для MongoDB и PostgreSQL"""
    return {
        "mongo": {
            "username": os.getenv("MONGO_INITDB_ROOT_USERNAME"),
            "password": os.getenv("MONGO_INITDB_ROOT_PASSWORD"),
            "host": os.getenv("MONGO_HOST"),
            "port": int(os.getenv("MONGO_PORT")),
            "authSource": "admin",
        },
        "postgres": {
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
            "url": (
                f"postgresql://"
                f"{os.getenv('POSTGRES_USER')}:"
                f"{os.getenv('POSTGRES_PASSWORD')}@"
                f"{os.getenv('POSTGRES_HOST')}:"
                f"{os.getenv('POSTGRES_PORT')}"
                f"/{os.getenv('POSTGRES_DB')}"
            ),
        },
    }


Base = declarative_base()


class WeatherCurrent(Base):
    """Таблица для текущей погоды"""

    __tablename__ = "weather_current"
    __table_args__ = {"schema": "raw"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    weather_id = Column(String, nullable=False)  # Убрали unique=True для SCD Type 2
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    request = Column(JSONB)
    response = Column(JSONB)
    status_code = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    valid_from_dttm = Column(DateTime, server_default=func.now())
    valid_to_dttm = Column(DateTime, nullable=False, default="5999-01-01")


class WeatherForecast(Base):
    """Таблица для прогнозов погоды"""

    __tablename__ = "weather_forecast"
    __table_args__ = {"schema": "raw"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    forecast_id = Column(String, nullable=False)  # Убрали unique=True для SCD Type 2
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    hours = Column(Integer, nullable=False)
    request = Column(JSONB)
    response = Column(JSONB)
    status_code = Column(Integer)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

    valid_from_dttm = Column(DateTime, server_default=func.now())
    valid_to_dttm = Column(DateTime, nullable=False, default="5999-01-01")


def declare_database_in_postgres():
    """Создание схемы и таблиц в PostgreSQL"""
    try:
        postgres_config = get_config()["postgres"]
        engine = create_engine(postgres_config["url"])

        with engine.begin() as connection:  # begin() автоматически делает commit
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

        Base.metadata.create_all(engine, checkfirst=True)
        logger.info("Schema and tables created successfully in PostgreSQL")

    except Exception as e:
        logger.error(f"Error creating database schema: {e}")
        raise


def get_current_weather_from_mongo(updated_at: datetime = None):
    """
    Получение данных текущей погоды из MongoDB

    Args:
        updated_at: Фильтр по дате обновления для инкрементальной загрузки
    """
    mongo_config = get_config()["mongo"]

    try:
        client = MongoClient(**mongo_config)
        db = client["weather_analytics_db"]
        collection = db["weather_current"]

        logger.info(
            f"Getting current weather from MongoDB with updated_at: {updated_at}"
        )

        filter_condition = (
            {} if updated_at is None else {"updated_at": {"$gt": updated_at}}
        )
        data = collection.find(filter_condition)

        for item in data:
            yield item

        client.close()

    except Exception as e:
        logger.error(f"Error getting data from MongoDB: {e}")
        raise


def get_forecasts_from_mongo(updated_at: datetime = None):
    """
    Получение прогнозов из MongoDB

    Args:
        updated_at: Фильтр по дате обновления для инкрементальной загрузки
    """
    mongo_config = get_config()["mongo"]

    try:
        client = MongoClient(**mongo_config)
        db = client["weather_analytics_db"]
        collection = db["weather_forecast"]

        logger.info(f"Getting forecasts from MongoDB with updated_at: {updated_at}")

        filter_condition = (
            {} if updated_at is None else {"updated_at": {"$gt": updated_at}}
        )
        data = collection.find(filter_condition)

        for item in data:
            yield item

        client.close()

    except Exception as e:
        logger.error(f"Error getting forecasts from MongoDB: {e}")
        raise


def get_last_update_at(session, model, shift_days: int = 1):
    """
    Получение времени последнего обновления

    Args:
        session: Сессия SQLAlchemy
        model: Модель таблицы
        shift_days: Сдвиг в днях для повторной обработки
    """
    last_update_at = session.query(func.max(model.updated_at)).scalar()

    if last_update_at is None:
        last_update_at = datetime(1970, 1, 1)
    else:
        last_update_at = last_update_at + timedelta(days=-shift_days)

    return last_update_at


def upsert_current_weather(session, data):
    """
    Загрузка текущей погоды в PostgreSQL
    """
    logger.info("Upserting current weather to PostgreSQL")

    try:
        count = 0
        skipped = 0
        for item in data:
            weather_id = str(item["_id"])

            # Проверяем, существует ли уже активная запись с таким weather_id
            existing = (
                session.query(WeatherCurrent)
                .filter(
                    WeatherCurrent.weather_id == weather_id,
                    WeatherCurrent.valid_to_dttm == datetime(5999, 1, 1),
                )
                .first()
            )

            if existing:
                # Если запись уже существует и не изменилась, пропускаем
                if existing.updated_at == item.get("updated_at"):
                    skipped += 1
                    continue

                # Закрываем старую версию
                existing.valid_to_dttm = func.now()
                session.commit()

            # Добавляем новую версию
            weather = WeatherCurrent(
                weather_id=weather_id,
                latitude=item.get("latitude"),
                longitude=item.get("longitude"),
                request=convert_datetime_to_str(item.get("request")),
                response=convert_datetime_to_str(item.get("response")),
                status_code=item.get("status_code"),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at"),
            )

            session.add(weather)
            session.commit()
            count += 1

        logger.info(
            f"Upserted {count} current weather records successfully, skipped {skipped} duplicates"
        )
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"Error during current weather upsert: {e}")
        raise


def upsert_forecasts(session, data):
    """
    Загрузка прогнозов в PostgreSQL
    """
    logger.info("Upserting forecasts to PostgreSQL")

    try:
        count = 0
        skipped = 0
        for item in data:
            forecast_id = str(item["_id"])

            # Проверяем, существует ли уже активная запись с таким forecast_id
            existing = (
                session.query(WeatherForecast)
                .filter(
                    WeatherForecast.forecast_id == forecast_id,
                    WeatherForecast.valid_to_dttm == datetime(5999, 1, 1),
                )
                .first()
            )

            if existing:
                # Если запись уже существует и не изменилась, пропускаем
                if existing.updated_at == item.get("updated_at"):
                    skipped += 1
                    continue

                # Закрываем старую версию
                existing.valid_to_dttm = func.now()
                session.commit()

            # Добавляем новую версию
            forecast = WeatherForecast(
                forecast_id=forecast_id,
                latitude=item.get("latitude"),
                longitude=item.get("longitude"),
                hours=item.get("hours"),
                request=convert_datetime_to_str(item.get("request")),
                response=convert_datetime_to_str(item.get("response")),
                status_code=item.get("status_code"),
                created_at=item.get("created_at"),
                updated_at=item.get("updated_at"),
            )

            session.add(forecast)
            session.commit()
            count += 1

        logger.info(
            f"Upserted {count} forecast records successfully, skipped {skipped} duplicates"
        )
        return count

    except Exception as e:
        session.rollback()
        logger.error(f"Error during forecast upsert: {e}")
        raise


def move_data_to_postgres():
    """
    Основная функция переноса данных из MongoDB в PostgreSQL
    """
    logger.info("Starting ETL process: MongoDB -> PostgreSQL")

    postgres_config = get_config()["postgres"]
    engine = create_engine(postgres_config["url"])
    Session = sessionmaker(bind=engine)
    session = Session()

    # Создаем схему и таблицы
    declare_database_in_postgres()

    try:
        # Загрузка текущей погоды
        logger.info("Processing current weather data...")
        last_update_current = get_last_update_at(session, WeatherCurrent)
        current_data = get_current_weather_from_mongo(updated_at=last_update_current)
        current_count = upsert_current_weather(session, current_data)

        # Загрузка прогнозов
        logger.info("Processing forecast data...")
        last_update_forecast = get_last_update_at(session, WeatherForecast)
        forecast_data = get_forecasts_from_mongo(updated_at=last_update_forecast)
        forecast_count = upsert_forecasts(session, forecast_data)

        logger.info(
            f"ETL completed successfully! "
            f"Current: {current_count}, Forecasts: {forecast_count}"
        )

        return {
            "success": True,
            "current_weather_count": current_count,
            "forecast_count": forecast_count,
        }

    except Exception as e:
        session.rollback()
        logger.error(f"ETL process failed: {e}")
        raise

    finally:
        session.close()
