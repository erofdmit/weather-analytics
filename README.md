# 🌤️ Weather Analytics Platform

> **Аналитическая платформа для сравнения точности прогнозов погоды от различных провайдеров**

Проект представляет собой полноценную data-платформу, которая собирает данные о погоде от 4 провайдеров (OpenWeatherMap, WeatherAPI, OpenMeteo, Weatherbit), сравнивает их прогнозы по температуре, влажности и скорости ветра с фактическими наблюдениями по 18 городам мира (Москва, Санкт-Петербург, Новосибирск, Екатеринбург, Казань, Владивосток, Мурманск, Южно-Сахалинск, Лондон, Париж, Берлин, Мадрид, Рим, Нью-Йорк, Лос-Анджелес, Чикаго, Майами, Торонто) на различных горизонтах прогнозирования (1, 5, 10, 24, 48, 72, 96, 120, 144, 168 часов), и предоставляет аналитические витрины для оценки точности каждого провайдера.

---

## 🔗 Ссылки на компоненты

| Компонент | URL | Описание |
|-----------|-----|----------|
| **Apache Airflow** | http://82.26.151.110:8080/home | Оркестрация DAG'ов и мониторинг pipeline |
| **Elementary Report** | http://82.26.151.110:8081/#/report/dashboard | Отчеты о качестве данных и тесты dbt |
| **PostgreSQL (DWH)** | http://82.26.151.110:5432 (доступ по ssh) | Аналитическое хранилище данных |
| **MongoDB** | http://82.26.151.110::27017 (доступ по ssh) | Хранилище сырых данных от провайдеров |
| **FastAPI Swagger** | http://82.26.151.110:8000/docs | API документация и тестирование endpoints |

**Учетные данные:**
- **Airflow**: `admin` / `admin`
- **PostgreSQL**:  db:weather_dwh / `postgres` / `postgres123` 
- **MongoDB**: `admin` / `admin123` 

---
**Основные DAG'и в Airflow:**
- `weather_data_collection` — сбор данных от провайдеров (ежечасно)
- `connector__mongo_postgres` — перенос данных MongoDB → PostgreSQL (ежечасно)
- `dbt_weather_marts` — трансформация данных через dbt (ежечасно)
---

## 🎯 Концепция проекта

### Решение

**Weather Analytics Platform** — это аналитическая система, которая:

1. **Собирает данные** от множества провайдеров погоды в режиме реального времени
2. **Сохраняет историю** прогнозов и фактических наблюдений
3. **Сравнивает точность** прогнозов с реальными данными
4. **Предоставляет метрики** (MAE, точность) для каждого провайдера
5. **Визуализирует результаты** через интерактивные дашборды


---

## 🏗️ Архитектура

### Высокоуровневая архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                      Weather API Providers                       │
│   OpenMeteo │ OpenWeatherMap │ WeatherAPI │ WeatherBit │ ...    │
└─────────────────────────────────────────────────────────────────┘
                              ↓ HTTP/REST
┌─────────────────────────────────────────────────────────────────┐
│                     FastAPI Application                          │
│  • Агрегация данных от 5+ провайдеров                           │
│  • Асинхронные запросы (aiohttp)                                │
│  • Обработка ошибок и retry-логика                              │
│  • REST API для внешних запросов                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓ Save
┌─────────────────────────────────────────────────────────────────┐
│                    MongoDB (Operational DB)                      │
│  • Гибкая схема для разных провайдеров                          │
│  • JSONB для сырых ответов API                                  │
│  • История всех запросов и ответов                              │
└─────────────────────────────────────────────────────────────────┘
                              ↓ ELT (hourly)
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Orchestration)                │
│  DAG 1: connector__mongo_postgres  → Перенос MongoDB → PostgreSQL│
│  DAG 2: weather_data_collection    → Сбор данных от провайдеров │
│  DAG 3: dbt_weather_marts          → Трансформация данных (dbt) │
└─────────────────────────────────────────────────────────────────┘
                              ↓ Load
┌─────────────────────────────────────────────────────────────────┐
│                  PostgreSQL (Data Warehouse)                     │
│  • raw schema    → Сырые данные из MongoDB (SCD Type 2)         │
│  • stg schema    → Staging views (парсинг JSONB)                │
│  • ods schema    → Operational Data Store (нормализация)        │
│  • dm schema     → Data Marts (аналитические витрины)           │
│  • elementary    → Мониторинг качества данных                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓ Transform
┌─────────────────────────────────────────────────────────────────┐
│                        DBT (Transformations)                     │
│  • Инкрементальная загрузка (delete+insert, merge)              │
│  • Jinja-шаблоны для динамических запросов                      │
│  • Все 4 типа dbt-core тестов + Elementary тесты                │
│  • Автогенерируемая документация                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓ Visualize
┌─────────────────────────────────────────────────────────────────┐
│                    Redash (BI & Dashboards)                      │
│  • Рейтинг провайдеров по точности                              │
│  • MAE/RMSE по горизонтам прогноза                              │
│  • Тепловая карта точности по городам                           │
│  • Сравнение метрик в динамике                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Принципы архитектуры

1. **Separation of Concerns**: каждый компонент отвечает за свою задачу
2. **Scalability**: горизонтальное масштабирование через Docker
3. **Observability**: логирование, мониторинг, алертинг на каждом уровне
4. **Data Quality**: автоматическое тестирование данных (dbt + Elementary)
5. **Reproducibility**: Infrastructure as Code, версионирование схем
6. **Incremental Processing**: обработка только новых данных

---

## 🛠️ Технологический стек

### Backend & API
- **FastAPI** 0.109+ — современный асинхронный веб-фреймворк
- **Pydantic** 2.0+ — валидация данных и сериализация
- **aiohttp** — асинхронные HTTP-запросы к провайдерам
- **httpx** — HTTP-клиент с поддержкой async/await

### Databases
- **MongoDB** 7.0+ — operational database для сырых данных
- **PostgreSQL** 16+ — аналитическое хранилище (DWH)

### Data Pipeline
- **Apache Airflow** 2.8+ — оркестрация ETL-процессов
- **dbt-core** 1.7.17 — трансформация данных (ELT)
- **dbt-postgres** — адаптер для PostgreSQL
- **elementary-data** 0.15.1 — мониторинг качества данных

### Visualization
- **Redash** 10.1+ — BI-платформа для дашбордов
- **dbt docs** — автогенерируемая документация моделей

### DevOps & CI/CD
- **Docker** & **Docker Compose** — контейнеризация
- **GitHub Actions** — CI/CD pipeline
- **pre-commit** — автоматические проверки кода
- **uv** — быстрый менеджер пакетов Python

### Code Quality
- **Ruff** — быстрый линтер Python
- **Black** — форматтер Python
- **isort** — сортировка импортов
- **SQLFluff** — линтер и форматтер SQL
- **mypy** — статическая типизация Python

### Testing
- **pytest** — unit и integration тесты
- **dbt test** — тестирование данных
- **Elementary** — аномалии и data quality

---

## 📁 Структура проекта

```
weather-analytics/
├── .github/
│   └── workflows/
│       └── ci.yml                    # CI/CD pipeline (lint, test, dbt, deploy)
│
├── src/
│   ├── app/                          # FastAPI приложение
│   │   ├── app/
│   │   │   ├── api/routes/           # REST API endpoints
│   │   │   ├── services/             # Бизнес-логика
│   │   │   │   └── weather_providers/  # Интеграции с провайдерами
│   │   │   ├── models/               # Pydantic модели
│   │   │   ├── db/                   # MongoDB клиент
│   │   │   └── core/                 # Конфигурация
│   │   ├── docker-compose.yml        # MongoDB + FastAPI
│   │   ├── Dockerfile
│   │   ├── pyproject.toml            # Зависимости (uv)
│   │   └── README.md
│   │
│   ├── airflow/                      # Apache Airflow
│   │   ├── dags/
│   │   │   ├── connector__mongo_postgres_dag.py      # ETL: MongoDB → PostgreSQL
│   │   │   ├── connector__mongo_postgres_logic.py    # Логика переноса данных
│   │   │   ├── weather_data_collection_dag.py        # Сбор данных от провайдеров
│   │   │   └── dbt_weather_marts_dag.py              # Запуск dbt трансформаций
│   │   ├── docker-compose.yml        # Airflow + scheduler + webserver
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── nginx-elementary.conf     # Nginx для Elementary отчетов
│   │
│   ├── dwh/                          # Data Warehouse (PostgreSQL)
│   │   ├── docker-compose.yml        # PostgreSQL + dbt контейнер
│   │   └── .env.example
│   │
│   ├── dbt/                          # DBT проект
│   │   ├── models/
│   │   │   ├── stg/                  # Staging: парсинг JSONB
│   │   │   │   ├── stg_weather_current.sql
│   │   │   │   ├── stg_weather_forecast.sql
│   │   │   │   ├── stg_schema.yml
│   │   │   │   └── sources.yml
│   │   │   ├── ods/                  # Operational Data Store
│   │   │   │   ├── ods_weather_observations.sql
│   │   │   │   ├── ods_weather_forecasts.sql
│   │   │   │   └── ods_schema.yml
│   │   │   └── dm/                   # Data Marts
│   │   │       ├── dm_provider_accuracy_overall.sql
│   │   │       ├── dm_provider_accuracy_by_city.sql
│   │   │       ├── dm_forecast_pivot.sql
│   │   │       └── dm_schema.yml
│   │   ├── tests/generic/            # Кастомные dbt тесты
│   │   │   ├── test_consistent_city_coordinates.sql
│   │   │   └── test_reasonable_feels_like_difference.sql
│   │   ├── macros/                   # Переиспользуемые макросы
│   │   │   ├── generate_schema_name.sql
│   │   │   ├── scd_helpers.sql
│   │   │   ├── temperature_conversion.sql
│   │   │   └── wind_direction.sql
│   │   ├── seeds/
│   │   │   └── cities.csv            # Справочник городов
│   │   ├── dbt_project.yml
│   │   ├── profiles.yml
│   │   ├── packages.yml              # dbt_utils, elementary
│   │   ├── requirements.txt
│   │   └── README.md
│   │
│   └── redash/                       # Redash для визуализации
│       ├── docker-compose.yml
│       ├── queries/                  # Готовые SQL-запросы для дашбордов
│       │   ├── 01_provider_ranking.sql
│       │   ├── 02_mae_by_horizons.sql
│       │   ├── 03_accuracy_by_city.sql
│       │   ├── 04_best_provider_by_city.sql
│       │   ├── 05_metrics_comparison.sql
│       │   └── 06_heatmap_city_provider.sql
│       └── README.md
│
├── tests/                            # Python тесты
│   ├── providers/                    # Тесты провайдеров погоды
│   ├── test_aggregator.py
│   ├── test_api.py
│   └── conftest.py
│
├── .pre-commit-config.yaml           # Pre-commit хуки
├── .sqlfluff                         # Конфигурация SQLFluff
├── .gitignore
└── README.md                         # Этот файл
```

---

