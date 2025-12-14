# 🌤️ Weather Analytics DBT Project

> **Трансформация данных о погоде для сравнения точности прогнозов от различных провайдеров**

Проект dbt для обработки данных о погоде от 4 провайдеров (OpenWeatherMap, WeatherAPI, OpenMeteo, Weatherbit) по 18 городам мира на 10 горизонтах прогнозирования (1-168 часов).

---


## 🏗️ Архитектура

### Трехслойная архитектура данных

```
┌─────────────────────────────────────────────────────────────────┐
│                    RAW Layer (PostgreSQL)                        │
│  • raw.weather_current  - текущая погода (JSONB)                │
│  • raw.weather_forecast - прогнозы (JSONB)                      │
│  • SCD Type 2 (valid_from_dttm, valid_to_dttm)                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    STG Layer (Views)                             │
│  • stg_weather_current  - парсинг JSONB текущей погоды          │
│  • stg_weather_forecast - парсинг JSONB прогнозов               │
│  • Incremental strategy: delete+insert                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    ODS Layer (Incremental Tables)                │
│  • ods_weather_observations - нормализованные наблюдения         │
│  • ods_weather_forecasts    - нормализованные прогнозы           │
│  • Суррогатные ключи, дедупликация, вычисляемые поля            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    DM Layer (Tables/Incremental)                 │
│  • dm_provider_accuracy_overall  - общая точность провайдеров    │
│  • dm_provider_accuracy_by_city  - точность по городам           │
│  • dm_forecast_pivot             - прогнозы в pivot-формате      │
│  • MAE метрики, ранжирование, агрегации                          │
└─────────────────────────────────────────────────────────────────┘
```

### Поток данных

1. **RAW** → Сырые данные из MongoDB (через Airflow ETL)
2. **STG** → Парсинг JSONB, развертывание массивов, фильтрация
3. **ODS** → Нормализация, дедупликация, суррогатные ключи
4. **DM** → Агрегация, вычисление метрик точности, ранжирование

---

## 🛠️ Технологический стек

| Компонент | Версия | Назначение |
|-----------|--------|------------|
| **dbt-core** | 1.7.17 | Трансформация данных |
| **dbt-postgres** | 1.7.17 | Адаптер для PostgreSQL |
| **PostgreSQL** | 16 | Хранилище данных |
| **dbt-utils** | 1.1.1 | Утилиты (surrogate_key, recency) |
| **elementary-data** | 0.15.1 | Мониторинг качества данных |
| **Jinja2** | - | Шаблонизация SQL |

---

## 📁 Структура проекта

```
src/dbt/
├── dbt_project.yml              # Конфигурация проекта
├── profiles.yml                 # Настройки подключения к БД
├── packages.yml                 # Внешние пакеты (dbt_utils, elementary)
├── requirements.txt             # Python зависимости
│
├── models/                      # Модели данных
│   ├── stg/                     # Staging layer
│   │   ├── sources.yml          # Определение источников (raw.*)
│   │   ├── stg_schema.yml       # Документация + тесты STG моделей
│   │   ├── stg_weather_current.sql     # Парсинг текущей погоды
│   │   └── stg_weather_forecast.sql    # Парсинг прогнозов
│   │
│   ├── ods/                     # Operational Data Store
│   │   ├── ods_schema.yml       # Документация + тесты ODS моделей
│   │   ├── ods_weather_observations.sql  # Нормализованные наблюдения
│   │   └── ods_weather_forecasts.sql     # Нормализованные прогнозы
│   │
│   └── dm/                      # Data Marts
│       ├── dm_schema.yml        # Документация + тесты DM моделей
│       ├── dm_provider_accuracy_overall.sql   # Общая точность
│       ├── dm_provider_accuracy_by_city.sql   # Точность по городам
│       └── dm_forecast_pivot.sql              # Pivot прогнозов
│
├── macros/                      # Пользовательские макросы
│   ├── generate_schema_name.sql        # Кастомная логика схем
│   ├── scd_helpers.sql                 # Хелперы для SCD Type 2
│   ├── temperature_conversion.sql      # Конвертация °C → °F
│   └── wind_direction.sql              # Градусы → стороны света
│
├── seeds/                       # Справочные данные
│   └── cities.csv               # Справочник городов (18 городов)
│
└── tests/                       # Кастомные тесты
    └── generic/
        ├── test_consistent_city_coordinates.sql      # Проверка координат
        └── test_reasonable_feels_like_difference.sql # Проверка feels_like
```

---

## 🎯 Слои данных

### 1️⃣ STG (Staging Layer)

**Materialization:** `incremental` (delete+insert)  
**Назначение:** Парсинг JSONB из raw таблиц, развертывание массивов

#### `stg_weather_current`

**Описание:** Разворачивает массив `samples` из `raw.weather_current`, где каждый sample — это данные от одного провайдера.

**Ключевые особенности:**
- ✅ Парсинг JSONB → структурированные колонки
- ✅ Развертывание массива `response.samples` (один провайдер = одна строка)
- ✅ Извлечение дополнительных полей из `raw` данных каждого провайдера
- ✅ Join с справочником городов (`ref('cities')`)
- ✅ Фильтрация только активных записей (SCD Type 2: `valid_to_dttm = '5999-01-01'`)
- ✅ Фильтрация только успешных запросов (`status_code = 200`)
- ✅ Инкрементальная загрузка по `created_at`

**Извлекаемые поля:**
- Основные: `temperature_celsius`, `humidity_percent`, `wind_speed_kph`, `wind_speed_ms`
- Дополнительные: `pressure_hpa`, `visibility_km`, `feels_like_celsius`, `wind_direction_degrees`, `cloud_cover_percent`, `uv_index`, `precipitation_mm`, `weather_condition`

**Jinja-конструкции:**
```sql
{{ config(...) }}
{{ source('raw', 'weather_current') }}
{% if is_incremental() %}
{{ this }}
{% endif %}
{{ ref('cities') }}
```

---

#### `stg_weather_forecast`

**Описание:** Разворачивает массив `forecasts` из `raw.weather_forecast`, где каждый forecast содержит массив `points` (прогнозы по часам).

**Ключевые особенности:**
- ✅ Двойное развертывание: `forecasts` → `points` (один час = одна строка)
- ✅ Извлечение `hours_ahead` из raw таблицы (горизонт прогноза)
- ✅ Вычисление `forecast_run_hour` (округление `created_at` до часа)
- ✅ Join с справочником городов
- ✅ Инкрементальная загрузка с использованием CTE `max_created_at`
- ✅ Фильтрация только активных записей и успешных запросов

**Извлекаемые поля:**
- `temperature_celsius`, `humidity_percent`, `wind_speed_kph`
- `forecast_timestamp` (целевое время прогноза)
- `forecast_run_hour` (время запуска прогноза)
- `hours_ahead` (горизонт прогноза: 1, 5, 10, 24, 48, 72, 96, 120, 144, 168)

**Jinja-конструкции:**
```sql
{{ config(...) }}
{{ source('raw', 'weather_forecast') }}
{% if is_incremental() %}
{{ this }}
{% endif %}
{{ ref('cities') }}
```

---

### 2️⃣ ODS (Operational Data Store)

**Materialization:** `incremental` (delete+insert для observations, merge для forecasts)  
**Назначение:** Нормализованные данные с суррогатными ключами и вычисляемыми полями

#### `ods_weather_observations`

**Описание:** Нормализованные наблюдения за погодой от разных провайдеров.

**Ключевые особенности:**
- ✅ Суррогатный ключ: `dbt_utils.generate_surrogate_key(['city', 'provider', 'observation_hour'])`
- ✅ Дедупликация через `row_number() over (partition by ... order by ...)`
- ✅ Вычисляемые поля: `day_of_week`, `hour_of_day`, `observation_date`
- ✅ Инкрементальная загрузка по `source_changed_at`
- ✅ Стратегия: `delete+insert` (удаление старых + вставка новых)

**Unique key:** `['city', 'provider', 'observation_hour']`

**Использование:**
- Базовая таблица для сравнения прогнозов с реальностью
- Источник "фактических" значений для расчета MAE

---

#### `ods_weather_forecasts`

**Описание:** Нормализованные прогнозы погоды от разных провайдеров на фиксированные горизонты.

**Ключевые особенности:**
- ✅ Суррогатный ключ: `dbt_utils.generate_surrogate_key(['city', 'provider', 'forecast_timestamp', 'hours_ahead'])`
- ✅ Фильтрация только нужных горизонтов: `hours_ahead in (1, 5, 10, 24, 48, 72, 96, 120, 144, 168)`
- ✅ Вычисляемые поля:
  - `forecast_horizon_category` (short_term, medium_term, long_term)
  - `target_datetime` (forecast_run_hour + hours_ahead)
  - `forecast_day_of_week`, `forecast_hour_of_day`, `forecast_run_date`
- ✅ Дедупликация через `row_number()`
- ✅ Инкрементальная загрузка по `source_changed_at`
- ✅ Стратегия: `merge` (обновление существующих + вставка новых)

**Unique key:** `['city', 'provider', 'forecast_timestamp', 'hours_ahead']`

**Использование:**
- Базовая таблица для расчета точности прогнозов
- Join с `ods_weather_observations` по `city` и `target_datetime`

---

### 3️⃣ DM (Data Marts)

**Materialization:** `table` (для accuracy моделей), `incremental` (для pivot)  
**Назначение:** Финальные витрины для дашбордов и аналитики

#### `dm_provider_accuracy_overall`

**Описание:** Общая точность провайдеров по всем городам.

**Ключевые метрики:**
- ✅ **MAE (Mean Absolute Error)** для температуры, влажности, скорости ветра
- ✅ MAE по каждому горизонту прогноза (1h, 5h, 10h, 24h, 48h, 72h, 96h, 120h, 144h, 168h)
- ✅ Средний MAE по всем горизонтам (`avg_temp_mae`, `avg_humidity_mae`, `avg_wind_mae`)
- ✅ Ранжирование провайдеров:
  - `provider_rank_by_temp` (по температуре)
  - `provider_rank_by_humidity` (по влажности)
  - `provider_rank_by_wind` (по ветру)
  - `overall_rank_score` (среднее арифметическое рангов)
- ✅ Количество прогнозов (`total_forecasts`)

**Логика расчета:**
1. Прогнозы из `ods_weather_forecasts`
2. Реальные значения = среднее по всем провайдерам из `ods_weather_observations`
3. Join по `city` и `target_datetime = observation_hour`
4. Вычисление абсолютной ошибки: `ABS(forecast - actual)`
5. Агрегация MAE по провайдеру и горизонту
6. Pivot по горизонтам (10 колонок для каждой метрики)
7. Ранжирование через `ROW_NUMBER() OVER (ORDER BY avg_mae ASC)`

**SQL-фичи:**
- ✅ CTE (Common Table Expressions) — 6 CTE
- ✅ Window Functions — `ROW_NUMBER() OVER (...)`
- ✅ CASE WHEN для pivot
- ✅ `dbt_utils.generate_surrogate_key()`

**Использование:**
```sql
-- Топ-3 провайдера по температуре
SELECT provider, avg_temp_mae, provider_rank_by_temp
FROM dm.dm_provider_accuracy_overall
ORDER BY provider_rank_by_temp
LIMIT 3;
```

---

#### `dm_provider_accuracy_by_city`

**Описание:** Точность провайдеров по каждому городу (18 городов).

**Ключевые метрики:**
- ✅ Те же метрики, что и в `dm_provider_accuracy_overall`, но с группировкой по `city`
- ✅ MAE для каждого города и провайдера
- ✅ Ранжирование провайдеров внутри каждого города:
  - `provider_rank_by_temp` (лучший провайдер для города по температуре)
  - `provider_rank_by_humidity`
  - `provider_rank_by_wind`
  - `overall_rank_score`

**Логика расчета:**
- Аналогична `dm_provider_accuracy_overall`, но с `PARTITION BY city` в window functions

**SQL-фичи:**
- ✅ CTE — 6 CTE
- ✅ Window Functions — `ROW_NUMBER() OVER (PARTITION BY city ORDER BY ...)`
- ✅ CASE WHEN для pivot
- ✅ `dbt_utils.generate_surrogate_key()`

**Использование:**
```sql
-- Лучший провайдер для Москвы
SELECT provider, avg_temp_mae, provider_rank_by_temp
FROM dm.dm_provider_accuracy_by_city
WHERE city = 'Moscow'
ORDER BY overall_rank_score
LIMIT 1;
```

---

#### `dm_forecast_pivot`

**Описание:** Прогнозы в pivot-формате для удобного анализа.

**Структура:**
- Одна строка = один прогноз от провайдера для города в определенный день
- Колонки с прогнозами на разные горизонты (10 горизонтов × 3 метрики = 30 колонок)
- Реальные значения от этого провайдера
- Средние значения от других провайдеров (консенсус)

**Ключевые особенности:**
- ✅ Pivot по горизонтам: `temp_forecast_1h`, `temp_forecast_5h`, ..., `temp_forecast_168h`
- ✅ Аналогично для влажности и скорости ветра
- ✅ Реальные значения: `temp_actual_avg`, `humidity_actual_avg`, `wind_actual_avg`
- ✅ Консенсус провайдеров: `temp_consensus_avg`, `temp_consensus_stddev`
- ✅ Инкрементальная загрузка (merge)
- ✅ Unique key: `['provider', 'city', 'forecast_run_date']`

**SQL-фичи:**
- ✅ CTE — 8 CTE
- ✅ CASE WHEN для pivot (3 × 10 = 30 CASE выражений)
- ✅ Window Functions для консенсуса
- ✅ `dbt_utils.generate_surrogate_key()`

**Использование:**
```sql
-- Сравнение прогноза на 24 часа с реальностью
SELECT 
    provider,
    city,
    temp_forecast_24h,
    temp_actual_avg,
    ABS(temp_forecast_24h - temp_actual_avg) as error_24h
FROM dm.dm_forecast_pivot
WHERE forecast_run_date = CURRENT_DATE - INTERVAL '1 day';
```

---

## 🔄 Инкрементальная загрузка

### Стратегии

| Слой | Модель | Стратегия | Unique Key | Описание |
|------|--------|-----------|------------|----------|
| **STG** | `stg_weather_current` | `delete+insert` | `['weather_id', 'provider', 'observation_hour']` | Удаление старых + вставка новых |
| **STG** | `stg_weather_forecast` | `delete+insert` | `['forecast_id', 'provider', 'forecast_timestamp', 'hours_ahead']` | Удаление старых + вставка новых |
| **ODS** | `ods_weather_observations` | `delete+insert` | `['city', 'provider', 'observation_hour']` | Удаление старых + вставка новых |
| **ODS** | `ods_weather_forecasts` | `merge` | `['city', 'provider', 'forecast_timestamp', 'hours_ahead']` | Обновление существующих + вставка новых |
| **DM** | `dm_forecast_pivot` | `merge` | `['provider', 'city', 'forecast_run_date']` | Обновление существующих + вставка новых |

### Преимущества инкрементальной загрузки

- ⚡ **Быстрая обработка** — только новые данные (не весь датасет)
- 💾 **Экономия ресурсов** — меньше нагрузка на БД
- 🔄 **Идемпотентность** — можно перезапускать без дублирования
- 📊 **Масштабируемость** — работает с большими объемами данных

### Пример инкрементальной логики

```sql
{% if is_incremental() %}
    -- Загружаем только новые данные
    and created_at > (select coalesce(max(created_at), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
```

---

## 🧪 Тестирование

### dbt-core тесты (все 4 типа)

#### 1. `unique` — Уникальность ключей

```yaml
- name: observation_id
  tests:
    - unique
```

**Где используется:**
- `stg_weather_current.id`
- `stg_weather_forecast.forecast_id`
- `ods_weather_observations.observation_id`
- `ods_weather_forecasts.forecast_id`

---

#### 2. `not_null` — Проверка на NULL

```yaml
- name: temperature_celsius
  tests:
    - not_null
```

**Где используется:**
- Все ключевые поля во всех моделях
- `temperature_celsius`, `humidity_percent`, `wind_speed_kph`
- `city`, `provider`, `observation_hour`, `forecast_timestamp`

---

#### 3. `accepted_values` — Допустимые значения

```yaml
- name: provider
  tests:
    - accepted_values:
        values: ['open_meteo', 'openweathermap', 'weatherapi', 'weatherbit']
```

**Где используется:**
- `provider` (4 провайдера)
- `city` (18 городов)
- `hours_ahead` (10 горизонтов: 1, 5, 10, 24, 48, 72, 96, 120, 144, 168)

---

#### 4. `relationships` — Связи между таблицами

```yaml
- name: city
  tests:
    - relationships:
        to: ref('ods_weather_forecasts')
        field: city
```

**Где используется:**
- `ods_weather_observations.city` → `ods_weather_forecasts.city`
- Проверка целостности данных между слоями

---

### dbt-utils тесты

#### `recency` — Свежесть данных

```yaml
- dbt_utils.recency:
    datepart: hour
    field: created_at
    interval: 5
    config:
      severity: error
```

**Где используется:**
- `stg_weather_current` — данные не старше 5 часов
- `stg_weather_forecast` — данные не старше 5 часов

---

### Elementary тесты (6 типов)

#### 1. `volume_anomalies` — Аномалии в объеме данных

```yaml
- elementary.volume_anomalies:
    timestamp_column: observation_hour
    time_bucket:
      period: day
      count: 1
    config:
      severity: warn
```

**Назначение:** Отслеживает резкое падение/рост количества записей

---

#### 2. `freshness_anomalies` — Аномалии свежести

```yaml
- elementary.freshness_anomalies:
    timestamp_column: observation_hour
    time_bucket:
      period: hour
      count: 1
    config:
      severity: error
```

**Назначение:** Проверяет, что данные обновляются регулярно

---

#### 3. `dimension_anomalies` — Аномалии в распределении

```yaml
- elementary.dimension_anomalies:
    dimensions:
      - provider
    timestamp_column: observation_hour
    time_bucket:
      period: day
      count: 1
    config:
      severity: warn
```

**Назначение:** Проверяет распределение данных по провайдерам (один провайдер перестал отдавать данные)

---

#### 4. `column_anomalies` — Аномалии в колонках

```yaml
- elementary.column_anomalies:
    column_anomalies:
      - min
      - max
      - null_count
    timestamp_column: observation_hour
    config:
      severity: warn
```

**Назначение:** Отслеживает выбросы в значениях (температура -100°C, влажность 200%)

---

#### 5. `all_columns_anomalies` — Проверка всех колонок

```yaml
- elementary.all_columns_anomalies:
    timestamp_column: observation_hour
    config:
      severity: warn
```

**Назначение:** Автоматическая проверка всех числовых колонок на аномалии

---

#### 6. `event_freshness_anomalies` — Аномалии событий

```yaml
- elementary.event_freshness_anomalies:
    event_timestamp_column: observation_hour
    update_timestamp_column: source_changed_at
    config:
      severity: warn
```

**Назначение:** Проверяет задержку между временем события и временем обновления

---

### Кастомные generic тесты

#### `test_consistent_city_coordinates`

**Назначение:** Проверяет, что города не меняют свои координаты со временем.

**Логика:**
- Группирует данные по городу и координатам
- Проверяет, что у города только одна пара координат
- Допустимая погрешность: `tolerance=0.1` градуса

**Использование:**
```yaml
tests:
  - consistent_city_coordinates:
      city_column: city
      lat_column: latitude
      lon_column: longitude
      tolerance: 0.1
```

---

#### `test_reasonable_feels_like_difference`

**Назначение:** Проверяет, что разница между ощущаемой и реальной температурой не слишком большая.

**Логика:**
- Вычисляет `ABS(feels_like_celsius - temperature_celsius)`
- Проверяет, что разница не превышает `max_difference=15°C`

**Использование:**
```yaml
tests:
  - reasonable_feels_like_difference:
      column_name: feels_like_celsius
      max_difference: 15
```

---

## 🔧 Макросы

### `celsius_to_fahrenheit`

**Назначение:** Конвертация температуры из Цельсия в Фаренгейт.

```sql
{% macro celsius_to_fahrenheit(celsius_column) %}
    round((({{ celsius_column }} * 9/5) + 32)::numeric, 1)
{% endmacro %}
```

**Использование:**
```sql
{{ celsius_to_fahrenheit('temperature_celsius') }} as temperature_fahrenheit
```

---

### `wind_direction_to_cardinal`

**Назначение:** Конвертация направления ветра из градусов в стороны света (N, NE, E, SE, S, SW, W, NW).

```sql
{% macro wind_direction_to_cardinal(degrees_column) %}
    case
        when {{ degrees_column }} between 0 and 22.5 then 'N'
        when {{ degrees_column }} between 22.5 and 67.5 then 'NE'
        when {{ degrees_column }} between 67.5 and 112.5 then 'E'
        ...
    end
{% endmacro %}
```

**Использование:**
```sql
{{ wind_direction_to_cardinal('wind_direction_degrees') }} as wind_direction_cardinal
```

---

### `generate_schema_name`

**Назначение:** Кастомная логика генерации имен схем (переопределяет стандартное поведение dbt).

**Логика:**
- Если `custom_schema_name` не задан → используется `target.schema` (по умолчанию `public`)
- Если задан → используется `custom_schema_name` (например, `stg`, `ods`, `dm`)

---

### `scd_helpers`

**Назначение:** Хелперы для работы с SCD Type 2 (Slowly Changing Dimensions).

**Функции:**
- Проверка активных записей (`valid_to_dttm = '5999-01-01'`)
- Фильтрация исторических записей

---

## 🌱 Seeds

### `cities.csv`

**Назначение:** Справочник городов для join с STG моделями.

**Структура:**
```csv
latitude,longitude,city,country
55.75,37.62,Moscow,Russia
59.93,30.36,Saint Petersburg,Russia
51.51,-0.13,London,United Kingdom
...
```

**Количество городов:** 18

**Города:**
- **Россия:** Moscow, Saint Petersburg, Novosibirsk, Yekaterinburg, Kazan, Vladivostok, Murmansk, Yuzhno-Sakhalinsk
- **Европа:** London, Paris, Berlin, Madrid, Rome
- **США:** New York, Los Angeles, Chicago, Miami
- **Канада:** Toronto

**Загрузка:**
```bash
dbt seed
```

---

## 📊 Мониторинг качества

### Elementary Data

**Возможности:**
- 📊 Автоматическое обнаружение аномалий
- 📈 Мониторинг свежести данных
- 📉 Отслеживание объема данных
- 🔔 Алерты при проблемах
- 📄 HTML-отчеты с визуализацией

### Генерация отчета

```bash
# В dbt контейнере
edr report --file-path /tmp/elementary_reports/index.html

# Просмотр в браузере
open http://localhost:8082
```

### Метрики качества

Elementary отслеживает:
- ✅ Свежесть данных (последнее обновление)
- ✅ Объем данных (количество записей)
- ✅ Аномалии в значениях (выбросы, NULL)
- ✅ Распределение категорий (провайдеры, города)
- ✅ Тренды изменений
- ✅ Время выполнения моделей
- ✅ Результаты тестов (pass/fail)

---

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
# Зайти в dbt контейнер
docker exec -it weather_dbt bash

# Установить dbt пакеты (выполняется автоматически в CI/CD)
dbt deps

# Проверить подключение
dbt debug
```

### 2. Загрузка справочников

```bash
# Загрузить справочник городов
dbt seed
```

### 3. Первый запуск (полная перестройка)

```bash
# Запустить все модели с полной перестройкой
dbt run --full-refresh

# Запустить тесты
dbt test

# Сгенерировать документацию
dbt docs generate
dbt docs serve --port 8080
```

### 4. Инкрементальные обновления

```bash
# Обновить только новые данные (инкрементально)
dbt run

# Запустить конкретный слой
dbt run --select stg
dbt run --select ods
dbt run --select dm

# Запустить конкретную модель
dbt run --select dm_provider_accuracy_overall
```

### 5. Генерация Elementary отчета

```bash
# Сгенерировать отчет о качестве данных
edr report --file-path /tmp/elementary_reports/index.html

# Просмотр в браузере
open http://localhost:8082
```

---

## 🔧 Команды dbt

### Основные команды

```bash
dbt debug          # Проверка подключения к БД
dbt deps           # Установка пакетов (dbt_utils, elementary)
dbt seed           # Загрузка справочников (cities.csv)
dbt compile        # Компиляция SQL без выполнения
dbt run            # Запуск моделей
dbt test           # Запуск тестов
dbt docs generate  # Генерация документации
dbt docs serve     # Запуск веб-сервера с документацией
dbt clean          # Очистка target/ и dbt_packages/
```

### Селекторы

```bash
# По названию модели
dbt run --select stg_weather_current
dbt run --select dm_provider_accuracy_overall

# По тегу
dbt run --select tag:stg
dbt run --select tag:ods
dbt run --select tag:dm

# По папке
dbt run --select models/stg
dbt run --select models/dm

# Графы зависимостей
dbt run --select +dm_provider_accuracy_overall  # модель + все upstream
dbt run --select dm_provider_accuracy_overall+  # модель + все downstream
dbt run --select +dm_provider_accuracy_overall+ # модель + upstream + downstream
```

### Флаги

```bash
--full-refresh     # Полная перестройка (игнорирует инкрементальность)
--threads 8        # Параллельные потоки (по умолчанию 4)
--target prod      # Целевое окружение (dev/prod)
--debug            # Режим отладки (подробные логи)
--vars '{key: value}'  # Передача переменных
```

### Примеры комбинаций

```bash
# Перестроить только STG слой с 8 потоками
dbt run --select tag:stg --full-refresh --threads 8

# Запустить только Elementary тесты
dbt test --select tag:elementary

# Скомпилировать без выполнения (для отладки)
dbt compile --select dm_provider_accuracy_overall

# Посмотреть скомпилированный SQL
cat target/compiled/weather_analytics/models/dm/dm_provider_accuracy_overall.sql
```

---

## 📈 Использование витрин

### Для дашборда "Рейтинг провайдеров"

```sql
-- Общий рейтинг провайдеров
SELECT 
    provider,
    avg_temp_mae,
    avg_humidity_mae,
    avg_wind_mae,
    provider_rank_by_temp,
    provider_rank_by_humidity,
    provider_rank_by_wind,
    overall_rank_score,
    total_forecasts
FROM dm.dm_provider_accuracy_overall
ORDER BY overall_rank_score;
```

### Для дашборда "Точность по горизонтам"

```sql
-- MAE по горизонтам прогноза (температура)
SELECT 
    provider,
    temp_mae_1h,
    temp_mae_5h,
    temp_mae_10h,
    temp_mae_24h,
    temp_mae_48h,
    temp_mae_72h,
    temp_mae_96h,
    temp_mae_120h,
    temp_mae_144h,
    temp_mae_168h
FROM dm.dm_provider_accuracy_overall
ORDER BY avg_temp_mae;
```

### Для дашборда "Лучший провайдер по городам"

```sql
-- Лучший провайдер для каждого города
SELECT 
    city,
    provider,
    avg_temp_mae,
    provider_rank_by_temp
FROM dm.dm_provider_accuracy_by_city
WHERE provider_rank_by_temp = 1
ORDER BY city;
```

### Для дашборда "Сравнение прогнозов"

```sql
-- Сравнение прогноза на 24 часа с реальностью
SELECT 
    provider,
    city,
    forecast_run_date,
    temp_forecast_24h,
    temp_actual_avg,
    ABS(temp_forecast_24h - temp_actual_avg) as error_24h
FROM dm.dm_forecast_pivot
WHERE forecast_run_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY error_24h DESC
LIMIT 10;
```

### Для дашборда "Консенсус провайдеров"

```sql
-- Консенсус провайдеров vs отдельный провайдер
SELECT 
    provider,
    city,
    forecast_run_date,
    temp_forecast_24h,
    temp_consensus_avg,
    temp_consensus_stddev,
    ABS(temp_forecast_24h - temp_consensus_avg) as deviation_from_consensus
FROM dm.dm_forecast_pivot
WHERE forecast_run_date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY deviation_from_consensus DESC;
```

---

## 🎓 Дополнительные ресурсы

### Документация

- [dbt Documentation](https://docs.getdbt.com/) — официальная документация dbt
- [Elementary Documentation](https://docs.elementary-data.com/) — документация Elementary
- [dbt Utils Package](https://github.com/dbt-labs/dbt-utils) — утилиты dbt
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices) — лучшие практики

### Внутренняя документация

- [Главный README проекта](../../README.md) — общая информация о проекте
- [dbt Docs](http://localhost:8081) — автогенерируемая документация моделей
- [Elementary Report](http://localhost:8082) — отчет о качестве данных

---

## 📊 Статистика проекта

| Метрика | Значение |
|---------|----------|
| **Моделей** | 7 (2 STG + 2 ODS + 3 DM) |
| **Тестов** | 100+ (dbt-core + dbt-utils + elementary + кастомные) |
| **Макросов** | 4 (temperature_conversion, wind_direction, generate_schema_name, scd_helpers) |
| **Seeds** | 1 (cities.csv, 18 городов) |
| **Источников** | 2 (raw.weather_current, raw.weather_forecast) |
| **Пакетов** | 2 (dbt_utils 1.1.1, elementary 0.15.1) |
| **Провайдеров** | 4 (OpenWeatherMap, WeatherAPI, OpenMeteo, Weatherbit) |
| **Городов** | 18 (Россия, Европа, США, Канада) |
| **Горизонтов прогноза** | 10 (1, 5, 10, 24, 48, 72, 96, 120, 144, 168 часов) |
| **Метрик** | 3 (температура, влажность, скорость ветра) |

---

**Версия:** 1.0.0  
**Последнее обновление:** 2025-01-09  
**Статус:** Production Ready ✅

---

<p align="center">
  <i>Сделано с ❤️ для data community</i>
</p>
