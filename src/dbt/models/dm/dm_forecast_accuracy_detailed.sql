{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['city', 'provider', 'forecast_created_at', 'observation_time', 'hours_ahead_interval'],
        tags=['dm', 'forecast', 'accuracy', 'detailed']
    )
}}

/*
    Детальная витрина для сравнения прогнозов с реальными наблюдениями.
    
    Логика:
    1. Берем прогноз, созданный в момент forecast_created_at
    2. Находим реальное наблюдение в момент forecast_timestamp
    3. Вычисляем разницу (ошибку) между прогнозом и реальностью
    4. Группируем по интервалам (1, 3, 6, 12, 24, 48 часов)
*/

with forecasts as (
    select
        city,
        country,
        provider,
        forecast_timestamp,
        forecast_created_at,
        hours_ahead,
        hours_ahead_interval,
        temperature_celsius as forecast_temperature,
        humidity_percent as forecast_humidity,
        wind_speed_kph as forecast_wind_speed,
        source_changed_at
    from {{ ref('ods_weather_forecasts') }}
    where hours_ahead_interval is not null
    
    {% if is_incremental() %}
        and source_changed_at > (select coalesce(max(source_changed_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

observations as (
    select
        city,
        provider,
        observation_time,
        temperature_celsius as actual_temperature,
        humidity_percent as actual_humidity,
        wind_speed_kph as actual_wind_speed,
        source_changed_at
    from {{ ref('ods_weather_observations') }}
),

-- Соединяем прогнозы с реальными наблюдениями
matched_data as (
    select
        f.city,
        f.country,
        f.provider,
        f.forecast_created_at,
        f.forecast_timestamp,
        f.hours_ahead,
        f.hours_ahead_interval,
        
        -- Прогнозные значения
        f.forecast_temperature,
        f.forecast_humidity,
        f.forecast_wind_speed,
        
        -- Реальные значения
        o.actual_temperature,
        o.actual_humidity,
        o.actual_wind_speed,
        o.observation_time,
        
        -- Для инкрементальной загрузки
        greatest(f.source_changed_at, o.source_changed_at) as source_changed_at
        
    from forecasts f
    inner join observations o
        on f.city = o.city
        and f.provider = o.provider
        -- Сопоставляем прогноз с реальным наблюдением в тот же момент времени
        -- Допускаем погрешность ±30 минут
        and f.forecast_timestamp between o.observation_time - interval '30 minutes' 
                                     and o.observation_time + interval '30 minutes'
),

-- Вычисляем метрики точности
accuracy_metrics as (
    select
        city,
        country,
        provider,
        forecast_created_at,
        observation_time,
        hours_ahead,
        hours_ahead_interval,
        
        -- Прогнозные значения
        forecast_temperature,
        forecast_humidity,
        forecast_wind_speed,
        
        -- Реальные значения
        actual_temperature,
        actual_humidity,
        actual_wind_speed,
        
        -- Абсолютные ошибки (MAE - Mean Absolute Error)
        abs(forecast_temperature - actual_temperature) as temperature_error_abs,
        abs(forecast_humidity - actual_humidity) as humidity_error_abs,
        abs(forecast_wind_speed - actual_wind_speed) as wind_speed_error_abs,
        
        -- Квадраты ошибок (для RMSE - Root Mean Square Error)
        power(forecast_temperature - actual_temperature, 2) as temperature_error_squared,
        power(forecast_humidity - actual_humidity, 2) as humidity_error_squared,
        power(forecast_wind_speed - actual_wind_speed, 2) as wind_speed_error_squared,
        
        -- Процентные ошибки (MAPE - Mean Absolute Percentage Error)
        case 
            when actual_temperature != 0 then 
                abs((forecast_temperature - actual_temperature) / nullif(actual_temperature, 0)) * 100
            else null
        end as temperature_error_percent,
        
        case 
            when actual_humidity != 0 then 
                abs((forecast_humidity - actual_humidity) / nullif(actual_humidity, 0)) * 100
            else null
        end as humidity_error_percent,
        
        case 
            when actual_wind_speed != 0 then 
                abs((forecast_wind_speed - actual_wind_speed) / nullif(actual_wind_speed, 0)) * 100
            else null
        end as wind_speed_error_percent,
        
        -- Точность в пределах допуска
        case when abs(forecast_temperature - actual_temperature) <= 2 then 1 else 0 end as temperature_accurate_2deg,
        case when abs(forecast_temperature - actual_temperature) <= 5 then 1 else 0 end as temperature_accurate_5deg,
        case when abs(forecast_humidity - actual_humidity) <= 10 then 1 else 0 end as humidity_accurate_10pct,
        case when abs(forecast_wind_speed - actual_wind_speed) <= 5 then 1 else 0 end as wind_speed_accurate_5kph,
        
        source_changed_at
        
    from matched_data
    where actual_temperature is not null
      and actual_humidity is not null
      and actual_wind_speed is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'forecast_created_at', 'observation_time', 'hours_ahead_interval']) }} as accuracy_id,
    city,
    country,
    provider,
    forecast_created_at,
    observation_time,
    hours_ahead,
    hours_ahead_interval,
    
    -- Прогнозные значения
    round(forecast_temperature::numeric, 2) as forecast_temperature,
    round(forecast_humidity::numeric, 1) as forecast_humidity,
    round(forecast_wind_speed::numeric, 1) as forecast_wind_speed,
    
    -- Реальные значения
    round(actual_temperature::numeric, 2) as actual_temperature,
    round(actual_humidity::numeric, 1) as actual_humidity,
    round(actual_wind_speed::numeric, 1) as actual_wind_speed,
    
    -- Метрики ошибок
    round(temperature_error_abs::numeric, 2) as temperature_mae,
    round(humidity_error_abs::numeric, 1) as humidity_mae,
    round(wind_speed_error_abs::numeric, 1) as wind_speed_mae,
    
    round(temperature_error_squared::numeric, 4) as temperature_mse,
    round(humidity_error_squared::numeric, 2) as humidity_mse,
    round(wind_speed_error_squared::numeric, 2) as wind_speed_mse,
    
    round(temperature_error_percent::numeric, 2) as temperature_mape,
    round(humidity_error_percent::numeric, 2) as humidity_mape,
    round(wind_speed_error_percent::numeric, 2) as wind_speed_mape,
    
    -- Флаги точности
    temperature_accurate_2deg,
    temperature_accurate_5deg,
    humidity_accurate_10pct,
    wind_speed_accurate_5kph,
    
    -- Метаданные
    source_changed_at,
    current_timestamp as processed_dttm

from accuracy_metrics

