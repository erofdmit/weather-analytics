FROM python:3.11-slim

# Устанавливаем зависимости системы
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /dbt

# Устанавливаем dbt и зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем проект dbt
COPY . .

# Создаем директорию для логов
RUN mkdir -p logs target

# Устанавливаем переменные окружения
ENV DBT_PROFILES_DIR=/dbt
ENV DBT_PROJECT_DIR=/dbt

# По умолчанию запускаем bash (для интерактивного режима)
CMD ["bash"]
