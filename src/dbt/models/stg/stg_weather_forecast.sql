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
        hours,  -- Пробрасываем hours для использования в hours_ahead
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
        hours,  -- Пробрасываем hours
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
-- Оставляем только поля, которые нужны для сравнения точности прогнозов
parsed_forecasts as (
    select
        id,
        forecast_id,
        latitude,
        longitude,
        hours,  -- Пробрасываем hours
        provider,
        
        -- Временная метка прогноза
        (point->>'time')::timestamp as forecast_timestamp,
        
        -- Основные погодные параметры для сравнения
        (point->>'temperature_c')::float as temperature_celsius,
        (point->>'humidity')::float as humidity_percent,
        (point->>'wind_speed_kph')::float as wind_speed_kph,
        
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
    
    -- Используем hours из raw таблицы (горизонт прогноза из запроса)
    pf.hours as hours_ahead,
    
    -- Основные погодные параметры для сравнения
    pf.temperature_celsius,
    pf.humidity_percent,
    pf.wind_speed_kph,
    
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
