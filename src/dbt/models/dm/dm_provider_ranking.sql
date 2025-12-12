{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='provider',
        tags=['dm', 'analytics', 'ranking']
    )
}}

/*
    DM витрина: Рейтинг провайдеров по точности прогнозов.
    
    Агрегированный рейтинг провайдеров на основе всех метрик точности.
    Используется для главного дашборда с общим сравнением провайдеров.
    
    Источник: dm_forecast_accuracy_by_interval
    Использование: Дашборд "Overall Provider Ranking"
*/

with provider_metrics as (
    select
        provider,
        
        -- Средние метрики по всем интервалам
        avg(temperature_mae) as avg_temperature_mae,
        avg(temperature_rmse) as avg_temperature_rmse,
        avg(accuracy_within_2deg_pct) as avg_accuracy_2deg,
        avg(accuracy_within_5deg_pct) as avg_accuracy_5deg,
        avg(humidity_mae) as avg_humidity_mae,
        avg(wind_speed_mae) as avg_wind_speed_mae,
        
        -- Метрики для краткосрочных прогнозов (1-6 часов)
        avg(case when hours_ahead_interval <= 6 then temperature_mae end) as short_term_temperature_mae,
        avg(case when hours_ahead_interval <= 6 then accuracy_within_2deg_pct end) as short_term_accuracy_2deg,
        
        -- Метрики для долгосрочных прогнозов (12-48 часов)
        avg(case when hours_ahead_interval >= 12 then temperature_mae end) as long_term_temperature_mae,
        avg(case when hours_ahead_interval >= 12 then accuracy_within_2deg_pct end) as long_term_accuracy_2deg,
        
        sum(total_forecasts) as total_forecasts_all_intervals
        
    from {{ ref('dm_forecast_accuracy_by_interval') }}
    group by provider
),

-- Вычисляем ранги провайдеров
with_ranks as (
    select
        *,
        -- Ранг по общей точности температуры (меньше MAE = лучше)
        row_number() over (order by avg_temperature_mae) as rank_by_temperature_mae,
        
        -- Ранг по проценту точных прогнозов (больше = лучше)
        row_number() over (order by avg_accuracy_2deg desc) as rank_by_accuracy_2deg,
        
        -- Ранг по краткосрочным прогнозам
        row_number() over (order by short_term_temperature_mae) as rank_short_term,
        
        -- Ранг по долгосрочным прогнозам
        row_number() over (order by long_term_temperature_mae) as rank_long_term,
        
        -- Общий ранг (среднее арифметическое рангов)
        (
            row_number() over (order by avg_temperature_mae) +
            row_number() over (order by avg_accuracy_2deg desc) +
            row_number() over (order by avg_humidity_mae) +
            row_number() over (order by avg_wind_speed_mae)
        ) / 4.0 as overall_rank_score
        
    from provider_metrics
)

select
    provider,
    
    -- Общий рейтинг
    row_number() over (order by overall_rank_score) as overall_rank,
    round(overall_rank_score::numeric, 2) as overall_rank_score,
    
    -- Метрики точности (округленные)
    round(avg_temperature_mae::numeric, 2) as avg_temperature_mae,
    round(avg_temperature_rmse::numeric, 2) as avg_temperature_rmse,
    round(avg_accuracy_2deg::numeric, 1) as avg_accuracy_2deg_pct,
    round(avg_accuracy_5deg::numeric, 1) as avg_accuracy_5deg_pct,
    round(avg_humidity_mae::numeric, 2) as avg_humidity_mae,
    round(avg_wind_speed_mae::numeric, 2) as avg_wind_speed_mae,
    
    -- Краткосрочные vs долгосрочные прогнозы
    round(short_term_temperature_mae::numeric, 2) as short_term_temperature_mae,
    round(short_term_accuracy_2deg::numeric, 1) as short_term_accuracy_2deg_pct,
    round(long_term_temperature_mae::numeric, 2) as long_term_temperature_mae,
    round(long_term_accuracy_2deg::numeric, 1) as long_term_accuracy_2deg_pct,
    
    -- Ранги по категориям
    rank_by_temperature_mae,
    rank_by_accuracy_2deg,
    rank_short_term,
    rank_long_term,
    
    -- Количество прогнозов
    total_forecasts_all_intervals,
    
    -- Метаданные
    current_timestamp as dbt_updated_at

from with_ranks
order by overall_rank
