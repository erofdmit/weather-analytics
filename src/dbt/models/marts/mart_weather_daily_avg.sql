-- Mart модель: дневная агрегированная статистика по городам
-- Вычисляет средние, минимальные и максимальные значения за день

{{ config(
    materialized='incremental',
    unique_key=['city', 'observation_date'],
    incremental_strategy='merge',
    tags=['marts', 'daily', 'aggregated']
) }}

with hourly_data as (
    select
        city,
        country,
        latitude,
        longitude,
        date_trunc('day', observation_timestamp)::date as observation_date,
        data_type,
        temperature_celsius,
        feels_like_celsius,
        humidity_percent,
        wind_speed_ms,
        pressure_hpa,
        precipitation_mm,
        cloud_cover_percent,
        visibility_km,
        uv_index,
        weather_condition
    from {{ ref('mart_weather_hourly') }}

    {% if is_incremental() %}
        where date_trunc('day', observation_timestamp)::date >=
            (select max(observation_date) from {{ this }})
    {% endif %}
)

select
    city,
    country,
    observation_date,

    -- Географические координаты (средние за день на случай небольших расхождений)
    round(avg(latitude)::numeric, 4) as latitude,
    round(avg(longitude)::numeric, 4) as longitude,

    -- Статистика по температуре
    round(avg(temperature_celsius)::numeric, 1) as avg_temperature_celsius,
    round(min(temperature_celsius)::numeric, 1) as min_temperature_celsius,
    round(max(temperature_celsius)::numeric, 1) as max_temperature_celsius,
    round((max(temperature_celsius) - min(temperature_celsius))::numeric, 1) as temperature_range,

    -- Ощущаемая температура
    round(avg(feels_like_celsius)::numeric, 1) as avg_feels_like_celsius,
    round(min(feels_like_celsius)::numeric, 1) as min_feels_like_celsius,
    round(max(feels_like_celsius)::numeric, 1) as max_feels_like_celsius,

    -- Влажность
    round(avg(humidity_percent)::numeric, 1) as avg_humidity_percent,
    round(min(humidity_percent)::numeric, 1) as min_humidity_percent,
    round(max(humidity_percent)::numeric, 1) as max_humidity_percent,

    -- Ветер
    round(avg(wind_speed_ms)::numeric, 1) as avg_wind_speed_ms,
    round(max(wind_speed_ms)::numeric, 1) as max_wind_speed_ms,

    -- Давление
    round(avg(pressure_hpa)::numeric, 1) as avg_pressure_hpa,
    round(min(pressure_hpa)::numeric, 1) as min_pressure_hpa,
    round(max(pressure_hpa)::numeric, 1) as max_pressure_hpa,

    -- Осадки
    round(sum(precipitation_mm)::numeric, 2) as total_precipitation_mm,
    round(avg(precipitation_mm)::numeric, 2) as avg_precipitation_mm,

    -- Облачность
    round(avg(cloud_cover_percent)::numeric, 1) as avg_cloud_cover_percent,

    -- Видимость
    round(avg(visibility_km)::numeric, 1) as avg_visibility_km,
    round(min(visibility_km)::numeric, 1) as min_visibility_km,

    -- УФ индекс
    round(avg(uv_index)::numeric, 1) as avg_uv_index,
    round(max(uv_index)::numeric, 1) as max_uv_index,

    -- Наиболее частое погодное условие
    mode() within group (order by weather_condition) as most_common_condition,

    -- Количество записей
    count(*) as observations_count,
    count(distinct data_type) as data_types_count,

    -- Метаданные
    current_timestamp as dbt_updated_at

from hourly_data
group by
    city,
    country,
    observation_date
