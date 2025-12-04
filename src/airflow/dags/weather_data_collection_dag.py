"""
DAG для периодического сбора данных о погоде
"""
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from loguru import logger

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def collect_weather_data(**context):
    """
    Функция для сбора данных о погоде из weather API
    """
    # Список городов для сбора данных
    cities = [
        {"name": "Moscow", "lat": 55.75, "lon": 37.62},
        {"name": "Saint Petersburg", "lat": 59.93, "lon": 30.36},
        {"name": "Novosibirsk", "lat": 55.03, "lon": 82.92},
        {"name": "Yekaterinburg", "lat": 56.84, "lon": 60.65},
        {"name": "Kazan", "lat": 55.79, "lon": 49.12},
    ]
    
    # URL weather API (предполагается, что app запущен)
    api_url = "http://weather_app:8000/api/weather/current"
    
    results = []
    for city in cities:
        try:
            logger.info(f"Collecting weather data for {city['name']}")
            response = requests.get(
                api_url,
                params={"lat": city["lat"], "lon": city["lon"]},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                results.append({
                    "city": city["name"],
                    "status": "success",
                    "data": data
                })
                logger.info(f"Successfully collected data for {city['name']}")
            else:
                logger.error(f"Failed to collect data for {city['name']}: {response.status_code}")
                results.append({
                    "city": city["name"],
                    "status": "error",
                    "error": f"HTTP {response.status_code}"
                })
                
        except Exception as e:
            logger.error(f"Error collecting data for {city['name']}: {e}")
            results.append({
                "city": city["name"],
                "status": "error",
                "error": str(e)
            })
    
    # Сохраняем результаты в XCom
    context['task_instance'].xcom_push(key='collection_results', value=results)
    
    logger.info(f"Collected data for {len(results)} cities")
    return results


with DAG(
    'weather_data_collection',
    default_args=default_args,
    description='Периодический сбор данных о погоде для разных городов',
    schedule_interval=timedelta(hours=1),  # Каждый час
    catchup=False,
    tags=['weather', 'data-collection'],
) as dag:
    
    collect_task = PythonOperator(
        task_id='collect_weather_data',
        python_callable=collect_weather_data,
        provide_context=True,
    )
    
    collect_task

