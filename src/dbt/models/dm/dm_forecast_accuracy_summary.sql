{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['city', 'provider', 'hours_ahead_interval', 'date_day'],
        tags=['dm', 'forecast', 'accuracy', 'summary']
    )
}}

/*
    Агрегированная витрина точности прогнозов по интервалам.
    
    Показывает среднюю точность прогнозов для каждого провайдера, города и интервала.
    Используется для построения дашбордов и рейтингов провайдеров.
*/

with detailed_accuracy as (
    select
        city,
        country,
        provider,
        hours_ahead_interval,
        date_trunc('day', observation_time) as date_day,
        
        -- Метрики ошибок
        temperature_mae,
        humidity_mae,
        wind_speed_mae,
        
        temperature_mse,
        humidity_mse,
        wind_speed_mse,
        
        temperature_mape,
        humidity_mape,
        wind_speed_mape,
        
        -- Флаги точности
        temperature_accurate_2deg,
        temperature_accurate_5deg,
        humidity_accurate_10pct,
        wind_speed_accurate_5kph,
        
        source_changed_at
        
    from {{ ref('dm_forecast_accuracy_detailed') }}
    
    {% if is_incremental() %}
        where source_changed_at > (select coalesce(max(source_changed_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

aggregated as (
    select
        city,
        country,
        provider,
        hours_ahead_interval,
        date_day,
        
        -- Количество прогнозов
        count(*) as forecast_count,
        
        -- Средние абсолютные ошибки (MAE)
        avg(temperature_mae) as avg_temperature_mae,
        avg(humidity_mae) as avg_humidity_mae,
        avg(wind_speed_mae) as avg_wind_speed_mae,
        
        -- Среднеквадратичные ошибки (RMSE)
        sqrt(avg(temperature_mse)) as temperature_rmse,
        sqrt(avg(humidity_mse)) as humidity_rmse,
        sqrt(avg(wind_speed_mse)) as wind_speed_rmse,
        
        -- Средние процентные ошибки (MAPE)
        avg(temperature_mape) as avg_temperature_mape,
        avg(humidity_mape) as avg_humidity_mape,
        avg(wind_speed_mape) as avg_wind_speed_mape,
        
        -- Процент точных прогнозов
        round(avg(temperature_accurate_2deg) * 100, 2) as temperature_accuracy_2deg_pct,
        round(avg(temperature_accurate_5deg) * 100, 2) as temperature_accuracy_5deg_pct,
        round(avg(humidity_accurate_10pct) * 100, 2) as humidity_accuracy_10pct_pct,
        round(avg(wind_speed_accurate_5kph) * 100, 2) as wind_speed_accuracy_5kph_pct,
        
        -- Общий балл точности (среднее по всем метрикам)
        round(
            (avg(temperature_accurate_2deg) + 
             avg(humidity_accurate_10pct) + 
             avg(wind_speed_accurate_5kph)) / 3.0 * 100, 
            2
        ) as overall_accuracy_score,
        
        max(source_changed_at) as source_changed_at
        
    from detailed_accuracy
    group by city, country, provider, hours_ahead_interval, date_day
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'hours_ahead_interval', 'date_day']) }} as summary_id,
    city,
    country,
    provider,
    hours_ahead_interval,
    date_day,
    forecast_count,
    
    -- MAE метрики
    round(avg_temperature_mae::numeric, 2) as avg_temperature_mae,
    round(avg_humidity_mae::numeric, 1) as avg_humidity_mae,
    round(avg_wind_speed_mae::numeric, 1) as avg_wind_speed_mae,
    
    -- RMSE метрики
    round(temperature_rmse::numeric, 2) as temperature_rmse,
    round(humidity_rmse::numeric, 1) as humidity_rmse,
    round(wind_speed_rmse::numeric, 1) as wind_speed_rmse,
    
    -- MAPE метрики
    round(avg_temperature_mape::numeric, 2) as avg_temperature_mape,
    round(avg_humidity_mape::numeric, 2) as avg_humidity_mape,
    round(avg_wind_speed_mape::numeric, 2) as avg_wind_speed_mape,
    
    -- Процент точности
    temperature_accuracy_2deg_pct,
    temperature_accuracy_5deg_pct,
    humidity_accuracy_10pct_pct,
    wind_speed_accuracy_5kph_pct,
    overall_accuracy_score,
    
    -- Метаданные
    source_changed_at,
    current_timestamp as processed_dttm

from aggregated

