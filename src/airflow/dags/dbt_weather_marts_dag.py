from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_weather_marts",
    default_args=default_args,
    description="Run dbt incremental pipelines (stg/ods/dm) for weather analytics",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "marts", "weather"],
) as dag:

    # ожидаем данные в двх
    wait_for_raw = ExternalTaskSensor(
        task_id="wait_for_connector_mongo_postgres",
        external_dag_id="connector__mongo_postgres",
        external_task_id="move_weather_data_to_dwh",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        timeout=60 * 60,
        poke_interval=60,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        cwd=DBT_DIR,
        bash_command="dbt deps --project-dir . --profiles-dir .",
        append_env=True,
        env={
            "DBT_PROJECT_DIR": DBT_DIR,
            "DBT_PROFILES_DIR": DBT_DIR,
        },
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_cities",
        cwd=DBT_DIR,
        bash_command="dbt seed --select cities --project-dir . --profiles-dir .",
        append_env=True,
        env={
            "DBT_PROJECT_DIR": DBT_DIR,
            "DBT_PROFILES_DIR": DBT_DIR,
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run_incremental",
        cwd=DBT_DIR,
        bash_command="dbt run --select tag:stg tag:ods tag:dm --project-dir . --profiles-dir .",
        append_env=True,
        env={
            "DBT_PROJECT_DIR": DBT_DIR,
            "DBT_PROFILES_DIR": DBT_DIR,
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        cwd=DBT_DIR,
        bash_command="dbt test --select tag:stg tag:ods tag:dm --project-dir . --profiles-dir .",
        append_env=True,
        env={
            "DBT_PROJECT_DIR": DBT_DIR,
            "DBT_PROFILES_DIR": DBT_DIR,
        },
    )

    wait_for_raw >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
