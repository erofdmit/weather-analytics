{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='city',
        tags=['dm', 'analytics', 'city_summary']
    )
}}

/*
    DM витрина: Сводка по городам с историческими данными.
    
    Агрегированная статистика по каждому городу:
    - Период наблюдений
    - Историческая статистика погоды
    - Количество провайдеров
    
    Источник: ods_weather_observations
    Использование: Дашборд "City Weather Overview"
*/

with city_stats as (
    select
        city,
        country,
        round(avg(latitude)::numeric, 4) as latitude,
        round(avg(longitude)::numeric, 4) as longitude,
        
        -- Период наблюдений
        min(observation_time) as first_observation,
        max(observation_time) as last_observation,
        count(distinct date_trunc('day', observation_time)) as days_tracked,
        count(*) as total_observations,
        count(distinct provider) as providers_count,
        
        -- Историческая статистика температуры
        round(avg(temperature_celsius)::numeric, 1) as avg_temperature,
        round(min(temperature_celsius)::numeric, 1) as min_temperature,
        round(max(temperature_celsius)::numeric, 1) as max_temperature,
        round(stddev(temperature_celsius)::numeric, 1) as stddev_temperature,
        
        -- Историческая статистика влажности
        round(avg(humidity_percent)::numeric, 1) as avg_humidity,
        round(min(humidity_percent)::numeric, 1) as min_humidity,
        round(max(humidity_percent)::numeric, 1) as max_humidity,
        
        -- Историческая статистика ветра
        round(avg(wind_speed_ms)::numeric, 1) as avg_wind_speed,
        round(max(wind_speed_ms)::numeric, 1) as max_wind_speed,
        
        -- Историческая статистика осадков
        round(avg(precipitation_mm)::numeric, 2) as avg_precipitation,
        round(sum(precipitation_mm)::numeric, 2) as total_precipitation
        
    from {{ ref('ods_weather_observations') }}
    group by city, country
)

select
    city,
    country,
    latitude,
    longitude,
    
    -- Период наблюдений
    first_observation,
    last_observation,
    days_tracked,
    total_observations,
    providers_count,
    
    -- Средняя частота наблюдений (наблюдений в день)
    round((total_observations::float / nullif(days_tracked, 0))::numeric, 1) as avg_observations_per_day,
    
    -- Историческая статистика
    avg_temperature,
    min_temperature,
    max_temperature,
    stddev_temperature,
    max_temperature - min_temperature as temperature_range,
    
    avg_humidity,
    min_humidity,
    max_humidity,
    
    avg_wind_speed,
    max_wind_speed,
    
    avg_precipitation,
    total_precipitation,
    
    -- Метаданные
    current_timestamp as dbt_updated_at

from city_stats
order by city
