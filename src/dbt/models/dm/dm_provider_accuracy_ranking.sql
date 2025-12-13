{{
    config(
        materialized='table',
        tags=['dm', 'forecast', 'ranking']
    )
}}

/*
    Рейтинг провайдеров погоды по точности прогнозов.
    
    Показывает, какой провайдер наиболее точен для разных интервалов прогнозирования.
*/

with provider_metrics as (
    select
        provider,
        hours_ahead_interval,
        
        -- Количество прогнозов
        sum(forecast_count) as total_forecasts,
        
        -- Средние метрики по всем городам и дням
        avg(avg_temperature_mae) as avg_temp_mae,
        avg(avg_humidity_mae) as avg_hum_mae,
        avg(avg_wind_speed_mae) as avg_wind_mae,
        
        avg(temperature_rmse) as avg_temp_rmse,
        avg(humidity_rmse) as avg_hum_rmse,
        avg(wind_speed_rmse) as avg_wind_rmse,
        
        -- Средний процент точности
        avg(temperature_accuracy_2deg_pct) as avg_temp_accuracy_2deg,
        avg(temperature_accuracy_5deg_pct) as avg_temp_accuracy_5deg,
        avg(humidity_accuracy_10pct_pct) as avg_hum_accuracy_10pct,
        avg(wind_speed_accuracy_5kph_pct) as avg_wind_accuracy_5kph,
        avg(overall_accuracy_score) as avg_overall_score
        
    from {{ ref('dm_forecast_accuracy_summary') }}
    group by provider, hours_ahead_interval
),

ranked as (
    select
        provider,
        hours_ahead_interval,
        total_forecasts,
        
        -- Метрики
        round(avg_temp_mae::numeric, 2) as avg_temperature_mae,
        round(avg_hum_mae::numeric, 1) as avg_humidity_mae,
        round(avg_wind_mae::numeric, 1) as avg_wind_speed_mae,
        
        round(avg_temp_rmse::numeric, 2) as avg_temperature_rmse,
        round(avg_hum_rmse::numeric, 1) as avg_humidity_rmse,
        round(avg_wind_rmse::numeric, 1) as avg_wind_speed_rmse,
        
        round(avg_temp_accuracy_2deg::numeric, 2) as temperature_accuracy_2deg_pct,
        round(avg_temp_accuracy_5deg::numeric, 2) as temperature_accuracy_5deg_pct,
        round(avg_hum_accuracy_10pct::numeric, 2) as humidity_accuracy_10pct_pct,
        round(avg_wind_accuracy_5kph::numeric, 2) as wind_speed_accuracy_5kph_pct,
        round(avg_overall_score::numeric, 2) as overall_accuracy_score,
        
        -- Ранги по разным метрикам (меньше MAE = лучше)
        row_number() over (partition by hours_ahead_interval order by avg_temp_mae asc) as rank_by_temp_mae,
        row_number() over (partition by hours_ahead_interval order by avg_hum_mae asc) as rank_by_hum_mae,
        row_number() over (partition by hours_ahead_interval order by avg_wind_mae asc) as rank_by_wind_mae,
        
        -- Ранг по общему баллу (больше = лучше)
        row_number() over (partition by hours_ahead_interval order by avg_overall_score desc) as overall_rank
        
    from provider_metrics
    where total_forecasts >= 10  -- Минимум 10 прогнозов для статистической значимости
)

select
    {{ dbt_utils.generate_surrogate_key(['provider', 'hours_ahead_interval']) }} as ranking_id,
    provider,
    hours_ahead_interval,
    total_forecasts,
    
    -- Метрики точности
    avg_temperature_mae,
    avg_humidity_mae,
    avg_wind_speed_mae,
    
    avg_temperature_rmse,
    avg_humidity_rmse,
    avg_wind_speed_rmse,
    
    temperature_accuracy_2deg_pct,
    temperature_accuracy_5deg_pct,
    humidity_accuracy_10pct_pct,
    wind_speed_accuracy_5kph_pct,
    overall_accuracy_score,
    
    -- Ранги
    rank_by_temp_mae,
    rank_by_hum_mae,
    rank_by_wind_mae,
    overall_rank,
    
    -- Метаданные
    current_timestamp as processed_dttm

from ranked
order by hours_ahead_interval, overall_rank

