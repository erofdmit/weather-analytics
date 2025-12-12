{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['city', 'provider', 'observation_time'],
        tags=['ods', 'weather', 'observations']
    )
}}

/*
    ODS модель для наблюдений за погодой от разных провайдеров.
    Инкрементальная загрузка с стратегией delete+insert для обновления данных.
    
    Источник: stg_weather_current
    Цель: Нормализованные данные о текущей погоде для дальнейшей аналитики
*/

with source_data as (
    select
        city,
        country,
        latitude,
        longitude,
        provider,
        observation_time,
        temperature_celsius,
        humidity_percent,
        wind_speed_ms,
        wind_speed_kph,
        pressure_hpa,
        precipitation_mm,
        cloud_cover_percent,
        visibility_km,
        uv_index,
        feels_like_celsius,
        wind_direction_degrees,
        weather_condition,
        created_at,
        updated_at
    from {{ ref('stg_weather_current') }}
    
    {% if is_incremental() %}
        -- Загружаем только новые данные
        where observation_time > (
            select coalesce(max(observation_time), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'observation_time']) }} as observation_id,
    city,
    country,
    latitude,
    longitude,
    provider,
    observation_time,
    
    -- Температурные показатели
    temperature_celsius,
    feels_like_celsius,
    round((temperature_celsius * 9.0 / 5.0 + 32)::numeric, 1) as temperature_fahrenheit,
    
    -- Метеорологические параметры
    humidity_percent,
    wind_speed_ms,
    wind_speed_kph,
    pressure_hpa,
    precipitation_mm,
    cloud_cover_percent,
    visibility_km,
    uv_index,
    wind_direction_degrees,
    
    -- Направление ветра (стороны света)
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
    
    weather_condition,
    
    -- Метаданные
    created_at as source_created_at,
    updated_at as source_updated_at,
    current_timestamp as processed_dttm

from source_data
