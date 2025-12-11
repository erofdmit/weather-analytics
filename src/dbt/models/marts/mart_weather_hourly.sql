-- Mart модель: почасовая аналитика погоды
-- Объединяет текущую погоду и прогнозы в единую витрину

{{ config(
    materialized='incremental',
    unique_key=['city', 'observation_timestamp'],
    incremental_strategy='merge',
    tags=['marts', 'hourly']
) }}

with current_weather as (
    select
        city,
        country,
        latitude,
        longitude,
        updated_at as observation_timestamp,
        'current' as data_type,
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
        created_at as ingestion_timestamp
    from {{ ref('stg_weather_current') }}

    {% if is_incremental() %}
        where updated_at > (select max(observation_timestamp) from {{ this }})
    {% endif %}
),

forecast_weather as (
    select
        city,
        country,
        latitude,
        longitude,
        forecast_timestamp as observation_timestamp,
        'forecast' as data_type,
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
        null::integer as sources_count,
        created_at as ingestion_timestamp
    from {{ ref('stg_weather_forecast') }}

    {% if is_incremental() %}
        where forecast_timestamp > (select max(observation_timestamp) from {{ this }})
    {% endif %}
),

combined as (
    select * from current_weather
    union all
    select * from forecast_weather
)

select
    city,
    country,
    latitude,
    longitude,
    observation_timestamp,
    data_type,

    -- Температурные показатели
    temperature_celsius,
    feels_like_celsius,
    round(((temperature_celsius * 9/5) + 32)::numeric, 1) as temperature_fahrenheit,

    -- Влажность и осадки
    humidity_percent,
    precipitation_mm,

    -- Ветер
    wind_speed_ms,
    round((wind_speed_ms * 3.6)::numeric, 1) as wind_speed_kmh,
    wind_direction_degrees,
    case
        when wind_direction_degrees between 0 and 22.5 then 'N'
        when wind_direction_degrees between 22.5 and 67.5 then 'NE'
        when wind_direction_degrees between 67.5 and 112.5 then 'E'
        when wind_direction_degrees between 112.5 and 157.5 then 'SE'
        when wind_direction_degrees between 157.5 and 202.5 then 'S'
        when wind_direction_degrees between 202.5 and 247.5 then 'SW'
        when wind_direction_degrees between 247.5 and 292.5 then 'W'
        when wind_direction_degrees between 292.5 and 337.5 then 'NW'
        when wind_direction_degrees between 337.5 and 360 then 'N'
        else 'Unknown'
    end as wind_direction_cardinal,

    -- Давление и видимость
    pressure_hpa,
    cloud_cover_percent,
    visibility_km,
    uv_index,

    -- Описание погоды
    weather_description,
    weather_condition,

    -- Метаданные
    sources_count,
    ingestion_timestamp,
    current_timestamp as dbt_updated_at

from combined
