"""
DAG для переноса данных из MongoDB в PostgreSQL DWH
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connector__mongo_postgres_logic import move_data_to_postgres

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "connector__mongo_postgres",
    default_args=default_args,
    description="ETL: MongoDB -> PostgreSQL DWH для weather analytics",
    schedule_interval="@hourly",  # Каждый час
    catchup=False,
    tags=["etl", "mongodb", "postgres", "weather"],
)

with dag:
    etl_task = PythonOperator(
        task_id="move_weather_data_to_dwh",
        python_callable=move_data_to_postgres,
        provide_context=True,
    )
