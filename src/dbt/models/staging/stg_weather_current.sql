-- Staging модель для текущей погоды
-- Очищает и нормализует данные из raw слоя

{{ config(
    materialized='view',
    tags=['staging', 'current_weather']
) }}

with source_data as (
    select
        id,
        weather_id,
        latitude,
        longitude,
        request,
        response,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm
    from {{ source('raw', 'weather_current') }}
    where valid_to_dttm = '5999-01-01'::timestamp  -- Только активные записи
),

parsed_data as (
    select
        id,
        weather_id,
        latitude,
        longitude,

        -- Извлекаем данные из request
        (request->>'city')::text as city,
        (request->>'country')::text as country,

        -- Извлекаем агрегированные данные из response
        (response->'aggregated'->>'temperature_celsius')::float as temperature_celsius,
        (response->'aggregated'->>'humidity_percent')::float as humidity_percent,
        (response->'aggregated'->>'wind_speed_ms')::float as wind_speed_ms,
        (response->'aggregated'->>'pressure_hpa')::float as pressure_hpa,
        (response->'aggregated'->>'precipitation_mm')::float as precipitation_mm,
        (response->'aggregated'->>'cloud_cover_percent')::float as cloud_cover_percent,
        (response->'aggregated'->>'visibility_km')::float as visibility_km,
        (response->'aggregated'->>'uv_index')::float as uv_index,
        (response->'aggregated'->>'feels_like_celsius')::float as feels_like_celsius,
        (response->'aggregated'->>'wind_direction_degrees')::float as wind_direction_degrees,
        (response->'aggregated'->>'description')::text as weather_description,
        (response->'aggregated'->>'condition')::text as weather_condition,

        -- Метаданные
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,

        -- Количество источников
        jsonb_array_length(response->'sources') as sources_count

    from source_data
)

select
    id,
    weather_id,
    latitude,
    longitude,
    city,
    country,
    temperature_celsius,
    humidity_percent,
    wind_speed_ms,
    pressure_hpa,
    precipitation_mm,
    cloud_cover_percent,
    visibility_km,
    uv_index,
    feels_like_celsius,
    wind_direction_degrees,
    weather_description,
    weather_condition,
    sources_count,
    status_code,
    created_at,
    updated_at,
    valid_from_dttm,
    valid_to_dttm
from parsed_data
where status_code = 200  -- Только успешные запросы
