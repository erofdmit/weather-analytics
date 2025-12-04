# Weather Analytics / FastAPI Weather Aggregator

Сервис на **FastAPI**, который агрегирует текущую погоду из нескольких публичных API
(Open-Meteo, OpenWeatherMap, WeatherAPI, Weatherbit, Weatherstack) и возвращает нормализованный ответ
с усреднёнными значениями температуры и влажности.

## Стек

* Python 3.11
* FastAPI
* httpx (async HTTP-клиент)
* Pydantic / pydantic-settings
* pytest / pytest-anyio
* Ruff (линтер) + Black (форматтер)
* uv (управление зависимостями)
* Docker
* GitHub Actions (CI)

---

## Установка и запуск локально

### 1. Установка uv

Если uv ещё не установлен:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

После установки перезагрузи терминал или выполни:

```bash
source $HOME/.local/bin/env
```

---

### 2. Клонирование проекта

```bash
git clone https://github.com/your-org/weather-analytics.git
cd weather-analytics
```

---

### 3. Настройка окружения и зависимостей

uv автоматически создаст виртуальное окружение и установит зависимости:

```bash
uv sync
```

Если хочешь установить только production-зависимости (без dev):

```bash
uv sync --no-dev
```

---

### 4. Переменные окружения

Создай `.env` в корне проекта (опционально, если нужны платные провайдеры):

```env
OPENWEATHER_API_KEY=your_openweather_key
WEATHERAPI_API_KEY=your_weatherapi_key
WEATHERBIT_API_KEY=your_weatherbit_key
WEATHERSTACK_API_KEY=your_weatherstack_key
HTTP_TIMEOUT=5
```

Без этих ключей будет работать только Open-Meteo (без ключа).

---

### 5. Запуск приложения

Через uv:

```bash
uv run uvicorn app.main:app --reload
```

По умолчанию API будет доступен по адресу:

```url
http://127.0.0.1:8000
```

Основные эндпоинты:

* **GET /api/weather/health** — healthcheck
* **GET /api/weather/current?lat=52.52&lon=13.405** — агрегированная погода по координатам

---

## Тесты

Юнит-тесты написаны на `pytest` / `pytest-anyio`.

```bash
uv sync

uv run pytest
```

Более подробный вывод:

```bash
uv run pytest -vv
```

---

## Линтинг и форматирование

Используется:

* **Ruff** — линтер (flake8/isort/pylint-lite в одном)
* **Black** — форматтер кода

Запуск линтера:

```bash
uv run ruff check .
```

Автофиксы:

```bash
uv run ruff check . --fix
```

Форматирование кода:

```bash
uv run black app tests
```

---

## Docker

В репозитории есть Dockerfile, который использует uv внутри контейнера.

Сборка образа:

```bash
docker build -t weather-analytics .
```

Запуск контейнера:

```bash
docker run --rm \
  -p 8000:8000 \
  --env-file .env \
  weather-analytics
```

После этого API будет доступен на:

```url
http://localhost:8000
```

---

## CI (GitHub Actions)

Workflow расположен в `.github/workflows/ci.yml` и запускается на `push` и `pull_request`.

Состоит из двух job'ов:

### **lint** — обязательный job

* устанавливает uv
* устанавливает Python через uv
* выполняет `uv sync`
* запускает `uv run ruff check .`

### **test** — опциональный job

* зависит от `lint` (`needs: [lint]`)
* также ставит зависимости через uv
* запускает `uv run pytest`
* помечен параметром `continue-on-error: true`, поэтому падение тестов не ломает весь pipeline
