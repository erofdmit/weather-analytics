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
* Poetry (управление зависимостями)
* Docker
* GitHub Actions (CI)

---

## Установка и запуск локально

### 1. Установка Poetry

Если Poetry ещё не установлен:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Убедись, что `poetry` доступен в `$PATH` (обычно нужно добавить `~/.local/bin`).

---

### 2. Клонирование проекта

```bash
git clone https://github.com/your-org/weather-analytics.git
cd weather-analytics
```

---

### 3. Настройка окружения и зависимостей

Poetry сам создаст виртуальное окружение и поставит зависимости из `pyproject.toml`:

```bash
poetry install
```

Если хочешь поставить только прод-зависимости:

```bash
poetry install --only main
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

Через Poetry:

```bash
poetry run uvicorn app.main:app --reload
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
poetry install

poetry run pytest
```

Более подробный вывод:

```bash
poetry run pytest -vv
```

---

## Линтинг и форматирование

Используется:

* **Ruff** — линтер (flake8/isort/pylint-lite в одном)
* **Black** — форматтер кода

Запуск линтера:

```bash
poetry run ruff check .
```

Автофиксы:

```bash
poetry run ruff check . --fix
```

Форматирование кода:

```bash
poetry run black app tests
```

---

## Docker

В репозитории есть Dockerfile, который использует Poetry внутри контейнера.

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

* устанавливает Python и Poetry
* выполняет `poetry install`
* запускает `poetry run ruff check .`

### **test** — опциональный job

* зависит от `lint` (`needs: [lint]`)
* также ставит зависимости через Poetry
* запускает `poetry run pytest`
* помечен параметром `continue-on-error: true`, поэтому падение тестов не ломает весь pipeline
