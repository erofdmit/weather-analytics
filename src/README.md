# Weather Analytics - Структура проекта

Проект разделен на три основные части:

## 📁 Структура

```
src/
├── app/          # FastAPI приложение для сбора данных о погоде
├── dwh/          # Data Warehouse (PostgreSQL)
└── airflow/      # Airflow для оркестрации ETL процессов
```

---

## 🌤️ app/ - Weather API Application

FastAPI приложение, которое:
- Агрегирует данные о погоде из нескольких источников
- Сохраняет все запросы и ответы в MongoDB
- Предоставляет REST API для получения данных

### Запуск

```bash
cd app/
docker-compose up -d
```

API будет доступен на: `http://localhost:8000`

### Эндпоинты

- `GET /api/weather/current?lat=55.75&lon=37.62` - текущая погода
- `GET /api/weather/health` - healthcheck
- `GET /api/weather/history/recent?limit=10` - последние записи из MongoDB
- `GET /api/weather/history/location?lat=55.75&lon=37.62` - история по координатам
- `GET /docs` - Swagger документация

### Проверка MongoDB

```bash
cd app/
python check_mongo.py
```

---

## 🗄️ dwh/ - Data Warehouse

PostgreSQL база данных для хранения обработанных данных.

### Запуск

```bash
cd dwh/
docker-compose up -d
```

PostgreSQL будет доступен на: `localhost:5432`

### Подключение

```bash
psql -h localhost -U postgres -d weather_dwh
```

---

## 🔄 airflow/ - Orchestration

Apache Airflow для автоматизации ETL процессов.

### Запуск

```bash
cd airflow/
docker-compose up -d
```

Airflow UI будет доступен на: `http://localhost:8080`

**Логин:** admin  
**Пароль:** admin (настраивается в `.env`)

### DAGs

- `weather_data_collection` - Периодический сбор данных о погоде для разных городов

---

## 🚀 Быстрый старт

### 1. Запустить все сервисы

```bash
# Запустить DWH
cd src/dwh && docker-compose up -d && cd ../..

# Запустить Weather App с MongoDB
cd src/app && docker-compose up -d && cd ../..

# Запустить Airflow
cd src/airflow && docker-compose up -d && cd ../..
```

### 2. Проверить работу

```bash
# Проверить Weather API
curl http://localhost:8000/api/weather/health

# Получить погоду для Москвы
curl "http://localhost:8000/api/weather/current?lat=55.75&lon=37.62"

# Проверить MongoDB
cd src/app && python check_mongo.py
```

### 3. Открыть Airflow UI

Перейти на `http://localhost:8080` и активировать DAG `weather_data_collection`

---

## 🛑 Остановка всех сервисов

```bash
cd src/app && docker-compose down && cd ../..
cd src/dwh && docker-compose down && cd ../..
cd src/airflow && docker-compose down && cd ../..
```

---

## 📊 Архитектура данных

```
┌─────────────────┐
│  Weather APIs   │
│ (Open-Meteo,    │
│  OpenWeather,   │
│  WeatherAPI,    │
│  etc.)          │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  FastAPI App    │
│  (src/app)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│    MongoDB      │      │    Airflow      │
│  (Raw Data)     │◄─────┤  (Orchestration)│
└────────┬────────┘      └─────────────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │
│  (Data Warehouse)│
└─────────────────┘
```

---

## 🔧 Конфигурация

Каждый сервис имеет свой `.env` файл:

- `app/.env` - настройки MongoDB и API ключи
- `dwh/.env` - настройки PostgreSQL
- `airflow/.env` - настройки Airflow

Примеры конфигураций находятся в `.env.example` файлах.

