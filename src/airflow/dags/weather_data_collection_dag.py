"""
DAG для периодического сбора данных о погоде
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def collect_weather_data(**context):
    """
    Функция для сбора текущих данных о погоде из weather API
    """
    # Список городов для сбора данных
    cities = [
        # Россия
        {"name": "Moscow", "lat": 55.75, "lon": 37.62},
        {"name": "Saint Petersburg", "lat": 59.93, "lon": 30.36},
        {"name": "Novosibirsk", "lat": 55.03, "lon": 82.92},
        {"name": "Yekaterinburg", "lat": 56.84, "lon": 60.65},
        {"name": "Kazan", "lat": 55.79, "lon": 49.12},
        {"name": "Vladivostok", "lat": 43.12, "lon": 131.89},
        {"name": "Murmansk", "lat": 68.97, "lon": 33.08},
        {"name": "Yuzhno-Sakhalinsk", "lat": 46.96, "lon": 142.73},
        # Европа
        {"name": "London", "lat": 51.51, "lon": -0.13},
        {"name": "Paris", "lat": 48.85, "lon": 2.35},
        {"name": "Berlin", "lat": 52.52, "lon": 13.40},
        {"name": "Madrid", "lat": 40.42, "lon": -3.70},
        {"name": "Rome", "lat": 41.90, "lon": 12.50},
        # Америка
        {"name": "New York", "lat": 40.71, "lon": -74.01},
        {"name": "Los Angeles", "lat": 34.05, "lon": -118.24},
        {"name": "Chicago", "lat": 41.88, "lon": -87.63},
        {"name": "Miami", "lat": 25.76, "lon": -80.19},
        {"name": "Toronto", "lat": 43.65, "lon": -79.38},
    ]

    # URL weather API (предполагается, что app запущен)
    api_url = "http://weather_app:8000/api/weather/current"

    results = []
    for city in cities:
        try:
            logger.info(f"Сбор текущих данных для {city['name']}")
            response = requests.get(
                api_url, params={"lat": city["lat"], "lon": city["lon"]}, timeout=30
            )

            if response.status_code == 200:
                data = response.json()
                results.append(
                    {"city": city["name"], "status": "success", "data": data}
                )
                logger.info(f"Текущие данные успешно собраны для {city['name']}")
            else:
                logger.error(
                    f"Ошибка сбора данных для {city['name']}: {response.status_code}"
                )
                results.append(
                    {
                        "city": city["name"],
                        "status": "error",
                        "error": f"HTTP {response.status_code}",
                    }
                )

        except Exception as e:
            logger.error(f"Ошибка сбора данных для {city['name']}: {e}")
            results.append({"city": city["name"], "status": "error", "error": str(e)})

    # Сохраняем результаты в XCom
    context["task_instance"].xcom_push(key="collection_results", value=results)

    logger.info(f"Собраны текущие данные для {len(results)} городов")
    return results


def collect_forecast_data(**context):
    """
    Функция для сбора прогнозов погоды из weather API
    """
    # Список городов (тот же, что и для текущей погоды)
    cities = [
        # Россия
        {"name": "Moscow", "lat": 55.75, "lon": 37.62},
        {"name": "Saint Petersburg", "lat": 59.93, "lon": 30.36},
        {"name": "Novosibirsk", "lat": 55.03, "lon": 82.92},
        {"name": "Yekaterinburg", "lat": 56.84, "lon": 60.65},
        {"name": "Kazan", "lat": 55.79, "lon": 49.12},
        {"name": "Vladivostok", "lat": 43.12, "lon": 131.89},
        {"name": "Murmansk", "lat": 68.97, "lon": 33.08},
        {"name": "Yuzhno-Sakhalinsk", "lat": 46.96, "lon": 142.73},
        # Европа
        {"name": "London", "lat": 51.51, "lon": -0.13},
        {"name": "Paris", "lat": 48.85, "lon": 2.35},
        {"name": "Berlin", "lat": 52.52, "lon": 13.40},
        {"name": "Madrid", "lat": 40.42, "lon": -3.70},
        {"name": "Rome", "lat": 41.90, "lon": 12.50},
        # Америка
        {"name": "New York", "lat": 40.71, "lon": -74.01},
        {"name": "Los Angeles", "lat": 34.05, "lon": -118.24},
        {"name": "Chicago", "lat": 41.88, "lon": -87.63},
        {"name": "Miami", "lat": 25.76, "lon": -80.19},
        {"name": "Toronto", "lat": 43.65, "lon": -79.38},
    ]

    # Горизонты прогнозов в часах
    forecast_hours = [1, 5, 10, 24, 48, 72, 96, 120, 144, 168]

    # URL forecast API
    api_url = "http://weather_app:8000/api/weather/forecast"

    results = []
    for city in cities:
        for hours in forecast_hours:
            try:
                logger.info(f"Сбор прогноза на {hours}ч для {city['name']}")
                response = requests.get(
                    api_url,
                    params={"lat": city["lat"], "lon": city["lon"], "hours": hours},
                    timeout=30,
                )

                if response.status_code == 200:
                    data = response.json()
                    results.append(
                        {
                            "city": city["name"],
                            "hours": hours,
                            "status": "success",
                            "data": data,
                        }
                    )
                    logger.info(
                        f"Прогноз на {hours}ч успешно собран для {city['name']}"
                    )
                else:
                    logger.error(
                        f"Ошибка сбора прогноза для {city['name']} ({hours}ч): {response.status_code}"
                    )
                    results.append(
                        {
                            "city": city["name"],
                            "hours": hours,
                            "status": "error",
                            "error": f"HTTP {response.status_code}",
                        }
                    )

            except Exception as e:
                logger.error(
                    f"Ошибка сбора прогноза для {city['name']} ({hours}ч): {e}"
                )
                results.append(
                    {
                        "city": city["name"],
                        "hours": hours,
                        "status": "error",
                        "error": str(e),
                    }
                )

    # Сохраняем результаты в XCom
    context["task_instance"].xcom_push(key="forecast_results", value=results)

    logger.info(f"Собраны прогнозы для {len(results)} комбинаций город/горизонт")
    return results


with DAG(
    "weather_data_collection",
    default_args=default_args,
    description="Периодический сбор текущих данных и прогнозов погоды для городов мира",
    schedule_interval=timedelta(hours=1),  # Каждый час
    catchup=False,
    tags=["weather", "data-collection"],
) as dag:

    collect_current_task = PythonOperator(
        task_id="collect_current_weather",
        python_callable=collect_weather_data,
        provide_context=True,
    )

    collect_forecast_task = PythonOperator(
        task_id="collect_forecast_weather",
        python_callable=collect_forecast_data,
        provide_context=True,
    )

    # Задачи выполняются параллельно (независимо друг от друга)
