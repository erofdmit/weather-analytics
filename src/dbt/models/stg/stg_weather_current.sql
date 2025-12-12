-- Staging модель для текущей погоды
-- Разворачивает samples от разных провайдеров в отдельные строки

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['weather_id', 'provider', 'observation_time'],
    tags=['stg', 'weather', 'current']
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
      and status_code = 200  -- Только успешные запросы
    
    {% if is_incremental() %}
        -- Инкрементальная загрузка: только новые данные
        and updated_at > (select max(source_updated_at) from {{ this }})
    {% endif %}
),

-- Разворачиваем массив samples (каждый провайдер - отдельная строка)
samples_expanded as (
    select
        id,
        weather_id,
        latitude,
        longitude,
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,
        
        -- Извлекаем данные из каждого sample
        jsonb_array_elements(response->'samples') as sample,
        
        -- Агрегированные значения (для справки)
        (response->>'average_temperature_c')::float as avg_temperature_c,
        (response->>'average_humidity')::float as avg_humidity
    from source_data
    where response->'samples' is not null
),

-- Парсим данные из каждого sample
parsed_samples as (
    select
        id,
        weather_id,
        latitude,
        longitude,
        
        -- Провайдер
        (sample->>'provider')::text as provider,
        
        -- Время наблюдения
        (sample->>'observation_time')::timestamp as observation_time,
        
        -- Погодные параметры
        (sample->>'temperature_c')::float as temperature_celsius,
        (sample->>'humidity')::float as humidity_percent,
        
        -- Скорость ветра (конвертируем из км/ч в м/с)
        ((sample->>'wind_speed_kph')::float / 3.6) as wind_speed_ms,
        (sample->>'wind_speed_kph')::float as wind_speed_kph,
        
        -- Погодное условие
        (sample->>'condition')::text as weather_condition,
        
        -- Извлекаем дополнительные поля из raw данных разных провайдеров
        case 
            when sample->>'provider' = 'openweathermap' then
                (sample->'raw'->'main'->>'pressure')::float
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'pressure_mb')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'pres')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'pressure')::float
        end as pressure_hpa,
        
        case 
            when sample->>'provider' = 'openweathermap' then
                (sample->'raw'->>'visibility')::float / 1000.0  -- метры в км
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'vis_km')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'vis')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'visibility')::float
        end as visibility_km,
        
        case 
            when sample->>'provider' = 'openweathermap' then
                (sample->'raw'->'main'->>'feels_like')::float
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'feelslike_c')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'app_temp')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'feelslike')::float
        end as feels_like_celsius,
        
        case 
            when sample->>'provider' = 'openweathermap' then
                (sample->'raw'->'wind'->>'deg')::float
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'wind_degree')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'wind_dir')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'wind_degree')::float
            when sample->>'provider' = 'open_meteo' then
                (sample->'raw'->'current_weather'->>'winddirection')::float
        end as wind_direction_degrees,
        
        case 
            when sample->>'provider' = 'openweathermap' then
                (sample->'raw'->'clouds'->>'all')::float
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'cloudcover')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'clouds')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'cloudcover')::float
        end as cloud_cover_percent,
        
        case 
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'uv')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'uv')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'uv_index')::float
        end as uv_index,
        
        case 
            when sample->>'provider' = 'weatherapi' then
                (sample->'raw'->'current'->>'precip_mm')::float
            when sample->>'provider' = 'weatherbit' then
                (sample->'raw'->'data'->0->>'precip')::float
            when sample->>'provider' = 'weatherstack' then
                (sample->'raw'->'current'->>'precip')::float
        end as precipitation_mm,
        
        -- Метаданные
        status_code,
        created_at,
        updated_at,
        valid_from_dttm,
        valid_to_dttm,
        
        -- Агрегированные значения
        avg_temperature_c,
        avg_humidity
        
    from samples_expanded
)

select
    ps.id,
    ps.weather_id,
    ps.latitude,
    ps.longitude,
    c.city,
    c.country,
    ps.provider,
    ps.observation_time,
    ps.temperature_celsius,
    ps.humidity_percent,
    ps.wind_speed_ms,
    ps.wind_speed_kph,
    ps.pressure_hpa,
    ps.precipitation_mm,
    ps.cloud_cover_percent,
    ps.visibility_km,
    ps.uv_index,
    ps.feels_like_celsius,
    ps.wind_direction_degrees,
    ps.weather_condition,
    ps.status_code,
    ps.created_at,
    ps.updated_at,
    ps.valid_from_dttm,
    ps.valid_to_dttm,
    ps.avg_temperature_c,
    ps.avg_humidity,
    ps.updated_at as source_updated_at  -- Для инкрементальной загрузки
from parsed_samples ps
left join {{ ref('cities') }} c
    on round(ps.latitude::numeric, 2) = round(c.latitude::numeric, 2)
    and round(ps.longitude::numeric, 2) = round(c.longitude::numeric, 2)
