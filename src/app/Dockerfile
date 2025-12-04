# Dockerfile
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Установка uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Копирование файлов зависимостей
COPY pyproject.toml uv.lock README.md ./

# Установка зависимостей (только production)
RUN uv sync --frozen --no-dev --no-install-project

# Копирование исходного кода
COPY app ./app

# Установка самого проекта
RUN uv sync --frozen --no-dev

EXPOSE 8000

# Запуск приложения через uv
CMD ["uv", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
