{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['provider', 'hours_ahead_interval'],
        tags=['dm', 'analytics', 'forecast_accuracy']
    )
}}

/*
    DM витрина: Точность прогнозов по провайдерам и временным интервалам.
    
    Финальная витрина для дашборда сравнения провайдеров.
    Показывает метрики точности (MAE, RMSE, accuracy%) для каждого провайдера
    на разных временных горизонтах (1h, 3h, 6h, 12h, 24h, 48h).
    
    Источники: ods_weather_observations, ods_weather_forecasts
    Использование: Дашборд "Provider Performance Comparison"
*/

with actual_observations as (
    -- Фактические наблюдения (усредненные по всем провайдерам)
    select
        city,
        observation_time,
        avg(temperature_celsius) as actual_temperature,
        avg(humidity_percent) as actual_humidity,
        avg(wind_speed_ms) as actual_wind_speed,
        count(distinct provider) as providers_count
    from {{ ref('ods_weather_observations') }}
    group by city, observation_time
),

forecasts as (
    -- Прогнозы от разных провайдеров
    select
        city,
        provider,
        forecast_timestamp,
        hours_ahead_interval,
        temperature_celsius as forecast_temperature,
        humidity_percent as forecast_humidity,
        wind_speed_ms as forecast_wind_speed
    from {{ ref('ods_weather_forecasts') }}
),

-- Джойним прогнозы с фактическими данными
forecast_vs_actual as (
    select
        f.city,
        f.provider,
        f.hours_ahead_interval,
        f.forecast_temperature,
        f.forecast_humidity,
        f.forecast_wind_speed,
        a.actual_temperature,
        a.actual_humidity,
        a.actual_wind_speed,
        
        -- Абсолютные ошибки
        abs(f.forecast_temperature - a.actual_temperature) as temperature_error_abs,
        abs(f.forecast_humidity - a.actual_humidity) as humidity_error_abs,
        abs(f.forecast_wind_speed - a.actual_wind_speed) as wind_speed_error_abs,
        
        -- Квадратичные ошибки (для RMSE)
        power(f.forecast_temperature - a.actual_temperature, 2) as temperature_squared_error,
        power(f.forecast_humidity - a.actual_humidity, 2) as humidity_squared_error
        
    from forecasts f
    inner join actual_observations a
        on f.city = a.city
        and f.forecast_timestamp = a.observation_time
    where a.actual_temperature is not null
)

select
    provider,
    hours_ahead_interval,
    
    -- Количество прогнозов
    count(*) as total_forecasts,
    count(distinct city) as cities_count,
    
    -- Метрики точности температуры
    round(avg(temperature_error_abs)::numeric, 2) as temperature_mae,  -- Mean Absolute Error
    round(sqrt(avg(temperature_squared_error))::numeric, 2) as temperature_rmse,  -- Root Mean Square Error
    round(percentile_cont(0.5) within group (order by temperature_error_abs)::numeric, 2) as temperature_median_error,
    round(max(temperature_error_abs)::numeric, 2) as temperature_max_error,
    
    -- Процент точных прогнозов
    round((sum(case when temperature_error_abs <= 2 then 1 else 0 end)::float / count(*) * 100)::numeric, 1) as accuracy_within_2deg_pct,
    round((sum(case when temperature_error_abs <= 5 then 1 else 0 end)::float / count(*) * 100)::numeric, 1) as accuracy_within_5deg_pct,
    
    -- Метрики точности влажности
    round(avg(humidity_error_abs)::numeric, 2) as humidity_mae,
    round(sqrt(avg(humidity_squared_error))::numeric, 2) as humidity_rmse,
    round((sum(case when humidity_error_abs <= 10 then 1 else 0 end)::float / count(*) * 100)::numeric, 1) as humidity_accuracy_within_10pct,
    
    -- Метрики точности скорости ветра
    round(avg(wind_speed_error_abs)::numeric, 2) as wind_speed_mae,
    
    -- Метаданные
    current_timestamp as dbt_updated_at

from forecast_vs_actual
group by provider, hours_ahead_interval
order by provider, hours_ahead_interval
