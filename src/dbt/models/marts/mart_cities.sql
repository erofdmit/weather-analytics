-- Mart модель: статистика по городам
-- Общая информация и исторические данные по каждому городу

{{ config(
    materialized='table',
    tags=['marts', 'cities', 'dimension']
) }}

with city_data as (
    select
        city,
        country,
        latitude,
        longitude,
        min(observation_timestamp) as first_observation,
        max(observation_timestamp) as last_observation,
        count(*) as total_observations,
        count(distinct date_trunc('day', observation_timestamp)) as days_tracked
    from {{ ref('mart_weather_hourly') }}
    where city is not null
    group by city, country, latitude, longitude
),

weather_stats as (
    select
        city,
        round(avg(temperature_celsius)::numeric, 1) as avg_temp_all_time,
        round(min(temperature_celsius)::numeric, 1) as min_temp_all_time,
        round(max(temperature_celsius)::numeric, 1) as max_temp_all_time,
        round(avg(precipitation_mm)::numeric, 2) as avg_precipitation,
        round(avg(humidity_percent)::numeric, 1) as avg_humidity
    from {{ ref('mart_weather_hourly') }}
    where city is not null
    group by city
)

select
    cd.city,
    cd.country,
    round(cd.latitude::numeric, 4) as latitude,
    round(cd.longitude::numeric, 4) as longitude,

    -- Временные рамки
    cd.first_observation,
    cd.last_observation,
    cd.days_tracked,
    cd.total_observations,

    -- Статистика погоды
    ws.avg_temp_all_time,
    ws.min_temp_all_time,
    ws.max_temp_all_time,
    ws.avg_precipitation,
    ws.avg_humidity,

    -- Метаданные
    current_timestamp as dbt_updated_at

from city_data cd
left join weather_stats ws on cd.city = ws.city
