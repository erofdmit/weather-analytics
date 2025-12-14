from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

# Пути внутри контейнера weather_dbt
DBT_CONTAINER = "weather_dbt"
DBT_DIR = "/dbt"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Функция для выполнения dbt команд в контейнере weather_dbt
def dbt(cmd: str) -> str:
    return f"""
set -euo pipefail

echo "=== Running dbt in {DBT_CONTAINER} container: {cmd} ==="
docker exec {DBT_CONTAINER} dbt {cmd} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} 2>&1
""".strip()

# Функция для Elementary команд в контейнере weather_dbt
def edr(cmd: str) -> str:
    return f"""
set -euo pipefail

echo "=== Checking Elementary installation in {DBT_CONTAINER} ==="
docker exec {DBT_CONTAINER} bash -c "pip show elementary-data || pip install elementary-data[postgres]"

echo "=== Running Elementary in {DBT_CONTAINER}: {cmd} ==="
docker exec {DBT_CONTAINER} edr {cmd} --profiles-dir {DBT_DIR} --project-dir {DBT_DIR} 2>&1
""".strip()

with DAG(
    dag_id="dbt_weather_marts",
    default_args=default_args,
    description="Run dbt incremental pipelines (stg/ods/dm) for weather analytics with Elementary reporting",
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "marts", "weather", "elementary"],
) as dag:
    # ВРЕМЕННО ОТКЛЮЧЕН: ожидаем dwh (можно включить когда connector__mongo_postgres будет работать)
    # wait_for_raw = ExternalTaskSensor(
    #     task_id="wait_for_connector_mongo_postgres",
    #     external_dag_id="connector__mongo_postgres",
    #     external_task_id="move_weather_data_to_dwh",
    #     allowed_states=["success"],
    #     failed_states=["failed", "skipped"],
    #     mode="reschedule",
    #     timeout=300,
    #     poke_interval=60,
    #     soft_fail=True,
    # )

    dbt_seed = BashOperator(
        task_id="dbt_seed_cities",
        bash_command=dbt("seed --select cities"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run_incremental",
        bash_command=dbt("run --select tag:stg tag:ods tag:dm"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt("test --select tag:stg tag:ods tag:dm"),
    )

    # Генерация Elementary отчета напрямую как index.html
    elementary_report = BashOperator(
        task_id="elementary_generate_report",
        bash_command=f"""
        set -euo pipefail
        
        echo "=== Checking Elementary installation in {DBT_CONTAINER} ==="
        docker exec {DBT_CONTAINER} bash -c "pip show elementary-data || pip install elementary-data[postgres]"
        
        echo "=== Generating Elementary report at $(date) ==="
        docker exec {DBT_CONTAINER} bash -c "
            mkdir -p /tmp/elementary_reports
            edr report \\
                --profiles-dir {DBT_DIR} \\
                --project-dir {DBT_DIR} \\
                --file-path /tmp/elementary_reports/index.html \\
                2>&1 || echo '⚠️ Report generation had issues, but continuing...'
        "
        
        echo "Elementary report generated at localhost:8081"
        """,
        trigger_rule="all_done",
    )

    # Цепочка выполнения (dbt_deps теперь выполняется в CI/CD)
    # wait_for_raw >> dbt_seed >> dbt_run >> dbt_test >> elementary_report
    dbt_seed >> dbt_run >> dbt_test >> elementary_report
