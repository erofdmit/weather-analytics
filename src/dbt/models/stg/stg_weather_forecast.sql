-- Staging модель для прогнозов погоды
-- Разворачивает прогнозы от разных провайдеров в отдельные строки (one row per hour per provider)

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['forecast_id', 'provider', 'forecast_timestamp'],
    tags=['stg', 'weather', 'forecast']
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
    
    {% if is_incremental() %}
        -- Инкрементальная загрузка: только новые данные
        and created_at > (select max(forecast_created_at) from {{ this }})
    {% endif %}
),

-- Разворачиваем массив forecasts (каждый провайдер - отдельная группа прогнозов)
forecasts_by_provider as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        hours,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,
        
        -- Извлекаем данные каждого провайдера
        jsonb_array_elements(response->'forecasts') as forecast_group
        
    from source_data
    where response->'forecasts' is not null
),

-- Разворачиваем points внутри каждого провайдера (каждый час - отдельная строка)
forecast_points_expanded as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        hours,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,
        
        -- Провайдер прогноза
        (forecast_group->>'provider')::text as provider,
        
        -- Разворачиваем массив points (прогнозы по часам)
        jsonb_array_elements(forecast_group->'points') as point
        
    from forecasts_by_provider
),

-- Парсим данные из каждого point
parsed_forecasts as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        provider,
        
        -- Временная метка прогноза
        (point->>'time')::timestamp as forecast_timestamp,
        
        -- Погодные параметры
        (point->>'temperature_c')::float as temperature_celsius,
        (point->>'humidity')::float as humidity_percent,
        
        -- Скорость ветра (конвертируем из км/ч в м/с)
        ((point->>'wind_speed_kph')::float / 3.6) as wind_speed_ms,
        (point->>'wind_speed_kph')::float as wind_speed_kph,
        
        -- Дополнительные поля (если есть)
        (point->>'pressure_mb')::float as pressure_hpa,
        (point->>'precip_mm')::float as precipitation_mm,
        (point->>'cloud_cover')::float as cloud_cover_percent,
        (point->>'visibility_km')::float as visibility_km,
        (point->>'uv_index')::float as uv_index,
        (point->>'feels_like_c')::float as feels_like_celsius,
        (point->>'wind_degree')::float as wind_direction_degrees,
        (point->>'condition')::text as weather_condition,
        (point->>'precip_probability')::float as precipitation_probability,
        
        -- Метаданные
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm
        
    from forecast_points_expanded
)

select
    pf.id,
    pf.forecast_id,
    pf.latitude,
    pf.longitude,
    c.city,
    c.country,
    pf.provider,
    pf.forecast_timestamp,
    
    -- Вычисляем offset в часах от времени создания прогноза
    -- Используем GREATEST чтобы избежать отрицательных значений
    greatest(0, extract(epoch from (pf.forecast_timestamp - pf.created_at)) / 3600) as hours_ahead,
    
    -- Погодные параметры
    pf.temperature_celsius,
    pf.humidity_percent,
    pf.wind_speed_ms,
    pf.wind_speed_kph,
    pf.pressure_hpa,
    pf.precipitation_mm,
    pf.cloud_cover_percent,
    pf.visibility_km,
    pf.uv_index,
    pf.feels_like_celsius,
    pf.wind_direction_degrees,
    pf.weather_condition,
    pf.precipitation_probability,
    
    -- Метаданные
    pf.status_code,
    pf.created_at as forecast_created_at,
    pf.updated_at,
    pf.valid_from_dttm,
    pf.valid_to_dttm
    
from parsed_forecasts pf
left join {{ ref('cities') }} c
    on round(pf.latitude::numeric, 2) = round(c.latitude::numeric, 2)
    and round(pf.longitude::numeric, 2) = round(c.longitude::numeric, 2)
order by pf.forecast_id, pf.provider, pf.forecast_timestamp
