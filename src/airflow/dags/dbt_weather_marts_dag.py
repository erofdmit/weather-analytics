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
# упрощаем создание bash команды для dbt
def dbt(cmd: str) -> str:
    return f"""
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"

echo "=== DBT_DIR content ==="
ls -la {DBT_DIR}
ls -la {DBT_DIR}/dbt_project.yml
ls -la {DBT_DIR}/profiles.yml

echo "=== dbt version ==="
dbt --version 2>&1

echo "=== Running dbt: {cmd} ==="
dbt {cmd} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --log-level debug 2>&1
""".strip()

with DAG(
    dag_id="dbt_weather_marts",
    default_args=default_args,
    description="Run dbt incremental pipelines (stg/ods/dm) for weather analytics",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "marts", "weather"],
) as dag:
    # ожидаем dwh
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

    common_env = {
        "DBT_PROJECT_DIR": DBT_DIR,
        "DBT_PROFILES_DIR": DBT_DIR,
    }

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt("deps"),
        append_env=True,
        env=common_env,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_cities",
        bash_command=dbt("seed --select cities"),
        append_env=True,
        env=common_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_incremental",
        bash_command=dbt("run --select tag:stg tag:ods tag:dm"),
        append_env=True,
        env=common_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt("test --select tag:stg tag:ods tag:dm"),
        append_env=True,
        env=common_env,
    )

    wait_for_raw >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
