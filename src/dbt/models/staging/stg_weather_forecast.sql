-- Staging модель для прогнозов погоды
-- Разворачивает массив прогнозов в отдельные строки (one row per hour)

{{ config(
    materialized='view',
    tags=['staging', 'forecast']
) }}

with source_data as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        hours,
        request,
        response,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm
    from {{ source('raw', 'weather_forecast') }}
    where valid_to_dttm = '5999-01-01'::timestamp  -- Только активные записи
      and status_code = 200  -- Только успешные запросы
),

-- Извлекаем базовую информацию
base_info as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        hours,
        (request->>'city')::text as city,
        (request->>'country')::text as country,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,
        response
    from source_data
),

-- Разворачиваем массив forecast (каждый элемент - это прогноз на час)
forecast_expanded as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        city,
        country,
        hours,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,

        -- Разворачиваем массив прогнозов
        jsonb_array_elements(response->'forecast') as forecast_item,

        -- Позиция в массиве (час прогноза от 0 до hours-1)
        row_number() over (partition by forecast_id order by ordinality) - 1 as hour_offset
    from base_info
    cross join lateral jsonb_array_elements(response->'forecast') with ordinality
)

select
    id,
    forecast_id,
    latitude,
    longitude,
    city,
    country,

    -- Временные метки
    (forecast_item->>'timestamp')::timestamp as forecast_timestamp,
    hour_offset,

    -- Погодные параметры
    (forecast_item->>'temperature_celsius')::float as temperature_celsius,
    (forecast_item->>'humidity_percent')::float as humidity_percent,
    (forecast_item->>'wind_speed_ms')::float as wind_speed_ms,
    (forecast_item->>'pressure_hpa')::float as pressure_hpa,
    (forecast_item->>'precipitation_mm')::float as precipitation_mm,
    (forecast_item->>'cloud_cover_percent')::float as cloud_cover_percent,
    (forecast_item->>'visibility_km')::float as visibility_km,
    (forecast_item->>'uv_index')::float as uv_index,
    (forecast_item->>'feels_like_celsius')::float as feels_like_celsius,
    (forecast_item->>'wind_direction_degrees')::float as wind_direction_degrees,
    (forecast_item->>'description')::text as weather_description,
    (forecast_item->>'condition')::text as weather_condition,
    (forecast_item->>'precipitation_probability')::float as precipitation_probability,

    -- Метаданные
    status_code,
    created_at,
    updated_at,
    valid_from_dttm,
    valid_to_dttm

from forecast_expanded
order by forecast_id, hour_offset
