# Weather Analytics Platform

**Платформа для сбора, хранения и анализа данных о погоде из нескольких источников с ETL пайплайнами и Data Warehouse.**

## 🎯 Что это?

Полнофункциональная система для:
- 🌍 Сбора текущих данных и прогнозов погоды для **18 городов** (Россия, Европа, Америка)
- 📊 Агрегации данных из **5 погодных API** (Open-Meteo, OpenWeatherMap, WeatherAPI, Weatherbit, Weatherstack)
- 💾 Хранения сырых данных в **MongoDB**
- 🔄 ETL процессов с **Apache Airflow** для загрузки в **PostgreSQL DWH**
- 🔍 Анализа данных через **Jupyter Notebook** и **pandas**

## 🏗️ Архитектура

```
┌─────────────────┐
│  Weather APIs   │ ← 5 источников данных
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  FastAPI App    │ ← Агрегация и нормализация
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    MongoDB      │ ← Сырые данные (Raw Layer)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Airflow ETL    │ ← Оркестрация пайплайнов
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ PostgreSQL DWH  │ ← Аналитическое хранилище
└─────────────────┘
```

## 📦 Технологический стек

**Backend & API:**
- Python 3.11
- FastAPI + httpx (async)
- Pydantic (валидация данных)

**Базы данных:**
- MongoDB (NoSQL для сырых данных)
- PostgreSQL 16 (реляционное DWH)

**Оркестрация:**
- Apache Airflow (ETL пайплайны)
- Celery + Redis (распределённые задачи)

**Инструменты разработки:**
- uv (управление зависимостями)
- Ruff (линтер)
- Black (форматтер)
- MyPy (проверка типов)
- pytest (тестирование)

**Инфраструктура:**
- Docker & Docker Compose
- GitHub Actions (CI/CD)

## 🚀 Быстрый старт

### 1. Запуск всех сервисов

```bash
# Weather App + MongoDB
cd src/app && docker compose up -d && cd ../..

# PostgreSQL DWH
cd src/dwh && docker compose up -d && cd ../..

# Airflow (scheduler, worker, webserver)
cd src/airflow && docker compose up -d && cd ../..
```

### 2. Проверка работы

```bash
# Проверить API
curl http://localhost:8000/api/weather/health

# Получить текущую погоду для Москвы
curl "http://localhost:8000/api/weather/current?lat=55.75&lon=37.62"

# Получить прогноз на 24 часа
curl "http://localhost:8000/api/weather/forecast?lat=55.75&lon=37.62&hours=24"
```

### 3. Доступ к сервисам

| Сервис | URL | Credentials |
|--------|-----|-------------|
| Weather API | http://localhost:8000 | - |
| API Docs (Swagger) | http://localhost:8000/docs | - |
| Airflow UI | http://localhost:8080 | admin / admin |
| MongoDB | localhost:27017 | admin / admin123 |
| PostgreSQL DWH | localhost:5432 | postgres / postgres123 |

## 📊 Данные

### Города (18 городов)

**Россия (8):** Москва, Санкт-Петербург, Новосибирск, Екатеринбург, Казань, Владивосток, Мурманск, Южно-Сахалинск

**Европа (5):** Лондон, Париж, Берлин, Мадрид, Рим

**Америка (5):** Нью-Йорк, Лос-Анджелес, Чикаго, Майами, Торонто

### Горизонты прогнозов

1, 5, 10, 24, 48, 72, 96, 120, 144, 168 часов (до 7 дней)

### Расписание сбора данных

- **Текущая погода:** каждый час для всех 18 городов
- **Прогнозы:** каждый час для всех 18 городов × 10 горизонтов = 180 запросов
- **ETL в DWH:** каждый час (инкрементальная загрузка)

## 🔄 Airflow DAGs

### 1. `weather_data_collection`
- **Расписание:** Каждый час
- **Задачи:**
  - `collect_current_weather` — сбор текущих данных
  - `collect_forecast_weather` — сбор прогнозов
- **Результат:** Данные сохраняются в MongoDB

### 2. `connector__mongo_postgres`
- **Расписание:** Каждый час
- **Задачи:**
  - `move_weather_data_to_dwh` — ETL из MongoDB в PostgreSQL
- **Особенности:**
  - Инкрементальная загрузка (только новые данные)
  - SCD Type 2 (версионирование изменений)
  - Две таблицы: `raw.weather_current` и `raw.weather_forecast`

## 📁 Структура проекта

```
weather-analytics/
├── src/
│   ├── app/                    # FastAPI приложение
│   │   ├── app/
│   │   │   ├── api/           # API endpoints
│   │   │   ├── core/          # Конфигурация
│   │   │   ├── db/            # MongoDB клиент
│   │   │   ├── models/        # Pydantic модели
│   │   │   └── services/      # Бизнес-логика
│   │   ├── Dockerfile
│   │   ├── docker-compose.yml
│   │   └── pyproject.toml
│   │
│   ├── dwh/                   # PostgreSQL Data Warehouse
│   │   └── docker-compose.yml
│   │
│   └── airflow/               # Apache Airflow
│       ├── dags/
│       │   ├── weather_data_collection_dag.py
│       │   ├── connector__mongo_postgres_dag.py
│       │   └── connector__mongo_postgres_logic.py
│       ├── Dockerfile
│       ├── docker-compose.yml
│       └── requirements.txt
│
├── test/                      # Jupyter notebooks для анализа
│   └── check.ipynb
│
├── .github/workflows/         # CI/CD
│   └── ci.yml
│
└── README.md
```

## 🛠️ Разработка

### Установка зависимостей

```bash
# Установить uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Установить зависимости
cd src/app
uv sync --dev
```

### Запуск локально

```bash
cd src/app

# Запустить только MongoDB
docker compose up -d mongodb

# Запустить приложение
MONGO_HOST=localhost uv run uvicorn app.main:app --reload
```

### Линтинг и форматирование

```bash
cd src/app

# Проверка кода
uv run ruff check .

# Автоматическое исправление
uv run ruff check . --fix

# Форматирование
uv run black .

# Проверка типов
uv run mypy app/ --ignore-missing-imports
```

### Тестирование

```bash
cd src/app
uv run pytest -v
```

## 📊 Анализ данных

Jupyter Notebook для анализа данных: `test/check.ipynb`

```bash
# Установить зависимости
pip install jupyter pandas pymongo sqlalchemy psycopg2-binary

# Запустить Jupyter
jupyter notebook test/check.ipynb
```

Notebook содержит:
- Подключение к MongoDB и PostgreSQL
- Загрузку данных в pandas DataFrame
- Базовый анализ и визуализацию

## 🔍 Проверка данных

### MongoDB

```bash
cd src/app
python check_mongo.py
```

### PostgreSQL DWH

```bash
cd src/dwh
docker compose exec postgres psql -U postgres -d weather_dwh

# Примеры запросов:
SELECT COUNT(*) FROM raw.weather_current;
SELECT COUNT(*) FROM raw.weather_forecast;

# Активные записи (SCD Type 2)
SELECT COUNT(*) FROM raw.weather_current 
WHERE valid_to_dttm = '5999-01-01';
```

### DBeaver

Для визуального просмотра данных используйте DBeaver:

**MongoDB:**
- Host: localhost
- Port: 27017
- Database: weather_analytics_db
- User: admin
- Password: admin123

**PostgreSQL:**
- Host: localhost
- Port: 5432
- Database: weather_dwh
- User: postgres
- Password: postgres123

## 🎯 Особенности реализации

### ETL процесс

1. **Extract:** Получение данных из MongoDB с фильтрацией по `updated_at`
2. **Transform:** Конвертация datetime в строки, подготовка JSON
3. **Load:** Загрузка в PostgreSQL с применением SCD Type 2

### SCD Type 2 (Slowly Changing Dimensions)

Каждая запись имеет:
- `valid_from_dttm` — дата начала актуальности
- `valid_to_dttm` — дата окончания актуальности (5999-01-01 для текущих)

При изменении данных:
1. Старая версия получает `valid_to_dttm = NOW()`
2. Создаётся новая версия с `valid_to_dttm = 5999-01-01`

### Качество кода

✅ Все проверки пройдены:
- **Ruff** (линтер) — 0 ошибок
- **Black** (форматтер) — код отформатирован
- **MyPy** (типы) — основные проверки пройдены
- **PEP8** — соответствие стандартам

## 📝 API Endpoints

### Текущая погода
```
GET /api/weather/current?lat={lat}&lon={lon}
```

### Прогноз погоды
```
GET /api/weather/forecast?lat={lat}&lon={lon}&hours={hours}
```

### История (из MongoDB)
```
GET /api/weather/history/current/recent?limit={limit}
GET /api/weather/history/current/location?lat={lat}&lon={lon}&limit={limit}
GET /api/weather/history/forecast/recent?limit={limit}
GET /api/weather/history/forecast/location?lat={lat}&lon={lon}&limit={limit}
```

### Health check
```
GET /api/weather/health
```

## 🤝 Авторы

- Dmitry Erofeev
- Eduard Polyakov
- Maxim Piskaev

