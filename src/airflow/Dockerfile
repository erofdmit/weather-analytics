FROM apache/airflow:2.10.5

# Устанавливаем дополнительные зависимости
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
USER airflow

