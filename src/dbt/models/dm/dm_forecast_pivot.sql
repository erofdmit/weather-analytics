{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['provider', 'city', 'forecast_run_date'],
        tags=['dm', 'forecast', 'pivot', 'analytics']
    )
}}

/*
    Витрина с развернутыми прогнозами по горизонтам.
    
    Структура:
    - Одна строка = один прогноз от провайдера для города в определенный день
    - Колонки с прогнозами на разные горизонты (1h, 5h, 10h, 24h, 48h, 72h, 96h, 120h, 144h, 168h)
    - Реальные значения от этого провайдера
    - Средние значения от других провайдеров (консенсус)
    
    Использование: Сравнение точности прогнозов на разных горизонтах
*/

with forecasts_base as (
    select
        provider,
        city,
        country,
        forecast_run_date,
        forecast_run_hour,
        hours_ahead,
        forecast_timestamp,
        target_datetime,
        temperature_celsius,
        humidity_percent,
        wind_speed_kph,
        source_changed_at
    from {{ ref('ods_weather_forecasts') }}
    
    {% if is_incremental() %}
        where source_changed_at > (select coalesce(max(source_changed_at), '1900-01-01'::timestamp) from {{ this }})
    {% endif %}
),

-- Pivot прогнозов по горизонтам (температура)
temp_pivoted as (
    select
        provider,
        city,
        country,
        forecast_run_date,
        forecast_run_hour,
        
        max(case when hours_ahead = 1 then temperature_celsius end) as temp_forecast_1h,
        max(case when hours_ahead = 5 then temperature_celsius end) as temp_forecast_5h,
        max(case when hours_ahead = 10 then temperature_celsius end) as temp_forecast_10h,
        max(case when hours_ahead = 24 then temperature_celsius end) as temp_forecast_24h,
        max(case when hours_ahead = 48 then temperature_celsius end) as temp_forecast_48h,
        max(case when hours_ahead = 72 then temperature_celsius end) as temp_forecast_72h,
        max(case when hours_ahead = 96 then temperature_celsius end) as temp_forecast_96h,
        max(case when hours_ahead = 120 then temperature_celsius end) as temp_forecast_120h,
        max(case when hours_ahead = 144 then temperature_celsius end) as temp_forecast_144h,
        max(case when hours_ahead = 168 then temperature_celsius end) as temp_forecast_168h,
        
        max(source_changed_at) as source_changed_at
    from forecasts_base
    group by provider, city, country, forecast_run_date, forecast_run_hour
),

-- Pivot прогнозов по горизонтам (влажность)
humidity_pivoted as (
    select
        provider,
        city,
        forecast_run_date,
        forecast_run_hour,
        
        max(case when hours_ahead = 1 then humidity_percent end) as humidity_forecast_1h,
        max(case when hours_ahead = 5 then humidity_percent end) as humidity_forecast_5h,
        max(case when hours_ahead = 10 then humidity_percent end) as humidity_forecast_10h,
        max(case when hours_ahead = 24 then humidity_percent end) as humidity_forecast_24h,
        max(case when hours_ahead = 48 then humidity_percent end) as humidity_forecast_48h,
        max(case when hours_ahead = 72 then humidity_percent end) as humidity_forecast_72h,
        max(case when hours_ahead = 96 then humidity_percent end) as humidity_forecast_96h,
        max(case when hours_ahead = 120 then humidity_percent end) as humidity_forecast_120h,
        max(case when hours_ahead = 144 then humidity_percent end) as humidity_forecast_144h,
        max(case when hours_ahead = 168 then humidity_percent end) as humidity_forecast_168h
    from forecasts_base
    group by provider, city, forecast_run_date, forecast_run_hour
),

-- Pivot прогнозов по горизонтам (скорость ветра)
wind_pivoted as (
    select
        provider,
        city,
        forecast_run_date,
        forecast_run_hour,
        
        max(case when hours_ahead = 1 then wind_speed_kph end) as wind_forecast_1h,
        max(case when hours_ahead = 5 then wind_speed_kph end) as wind_forecast_5h,
        max(case when hours_ahead = 10 then wind_speed_kph end) as wind_forecast_10h,
        max(case when hours_ahead = 24 then wind_speed_kph end) as wind_forecast_24h,
        max(case when hours_ahead = 48 then wind_speed_kph end) as wind_forecast_48h,
        max(case when hours_ahead = 72 then wind_speed_kph end) as wind_forecast_72h,
        max(case when hours_ahead = 96 then wind_speed_kph end) as wind_forecast_96h,
        max(case when hours_ahead = 120 then wind_speed_kph end) as wind_forecast_120h,
        max(case when hours_ahead = 144 then wind_speed_kph end) as wind_forecast_144h,
        max(case when hours_ahead = 168 then wind_speed_kph end) as wind_forecast_168h
    from forecasts_base
    group by provider, city, forecast_run_date, forecast_run_hour
),

-- Реальные значения от этого провайдера (средние за день)
actual_values as (
    select
        provider,
        city,
        date_trunc('day', observation_hour) as observation_date,
        
        avg(temperature_celsius) as temp_actual_avg_day,
        avg(humidity_percent) as humidity_actual_avg_day,
        avg(wind_speed_kph) as wind_actual_avg_day
    from {{ ref('ods_weather_observations') }}
    group by provider, city, date_trunc('day', observation_hour)
),

-- Средние значения от других провайдеров (консенсус)
consensus_values as (
    select
        city,
        date_trunc('day', observation_hour) as observation_date,
        
        avg(temperature_celsius) as temp_consensus_avg,
        stddev(temperature_celsius) as temp_consensus_stddev,
        avg(humidity_percent) as humidity_consensus_avg,
        stddev(humidity_percent) as humidity_consensus_stddev,
        avg(wind_speed_kph) as wind_consensus_avg,
        stddev(wind_speed_kph) as wind_consensus_stddev,
        count(distinct provider) as providers_count
    from {{ ref('ods_weather_observations') }}
    group by city, date_trunc('day', observation_hour)
)

select
    {{ dbt_utils.generate_surrogate_key(['tp.provider', 'tp.city', 'tp.forecast_run_date']) }} as pivot_id,
    
    -- Идентификаторы
    tp.provider,
    tp.city,
    tp.country,
    tp.forecast_run_date,
    tp.forecast_run_hour,
    
    -- Прогнозы температуры на разные горизонты
    round(tp.temp_forecast_1h::numeric, 2) as temp_forecast_1h,
    round(tp.temp_forecast_5h::numeric, 2) as temp_forecast_5h,
    round(tp.temp_forecast_10h::numeric, 2) as temp_forecast_10h,
    round(tp.temp_forecast_24h::numeric, 2) as temp_forecast_24h,
    round(tp.temp_forecast_48h::numeric, 2) as temp_forecast_48h,
    round(tp.temp_forecast_72h::numeric, 2) as temp_forecast_72h,
    round(tp.temp_forecast_96h::numeric, 2) as temp_forecast_96h,
    round(tp.temp_forecast_120h::numeric, 2) as temp_forecast_120h,
    round(tp.temp_forecast_144h::numeric, 2) as temp_forecast_144h,
    round(tp.temp_forecast_168h::numeric, 2) as temp_forecast_168h,
    
    -- Прогнозы влажности на разные горизонты
    round(hp.humidity_forecast_1h::numeric, 1) as humidity_forecast_1h,
    round(hp.humidity_forecast_5h::numeric, 1) as humidity_forecast_5h,
    round(hp.humidity_forecast_10h::numeric, 1) as humidity_forecast_10h,
    round(hp.humidity_forecast_24h::numeric, 1) as humidity_forecast_24h,
    round(hp.humidity_forecast_48h::numeric, 1) as humidity_forecast_48h,
    round(hp.humidity_forecast_72h::numeric, 1) as humidity_forecast_72h,
    round(hp.humidity_forecast_96h::numeric, 1) as humidity_forecast_96h,
    round(hp.humidity_forecast_120h::numeric, 1) as humidity_forecast_120h,
    round(hp.humidity_forecast_144h::numeric, 1) as humidity_forecast_144h,
    round(hp.humidity_forecast_168h::numeric, 1) as humidity_forecast_168h,
    
    -- Прогнозы скорости ветра на разные горизонты
    round(wp.wind_forecast_1h::numeric, 1) as wind_forecast_1h,
    round(wp.wind_forecast_5h::numeric, 1) as wind_forecast_5h,
    round(wp.wind_forecast_10h::numeric, 1) as wind_forecast_10h,
    round(wp.wind_forecast_24h::numeric, 1) as wind_forecast_24h,
    round(wp.wind_forecast_48h::numeric, 1) as wind_forecast_48h,
    round(wp.wind_forecast_72h::numeric, 1) as wind_forecast_72h,
    round(wp.wind_forecast_96h::numeric, 1) as wind_forecast_96h,
    round(wp.wind_forecast_120h::numeric, 1) as wind_forecast_120h,
    round(wp.wind_forecast_144h::numeric, 1) as wind_forecast_144h,
    round(wp.wind_forecast_168h::numeric, 1) as wind_forecast_168h,
    
    -- Реальные значения от этого провайдера
    round(av.temp_actual_avg_day::numeric, 2) as temp_actual_avg,
    round(av.humidity_actual_avg_day::numeric, 1) as humidity_actual_avg,
    round(av.wind_actual_avg_day::numeric, 1) as wind_actual_avg,
    
    -- Консенсус от других провайдеров
    round(cv.temp_consensus_avg::numeric, 2) as temp_consensus_avg,
    round(cv.temp_consensus_stddev::numeric, 2) as temp_consensus_stddev,
    round(cv.humidity_consensus_avg::numeric, 1) as humidity_consensus_avg,
    round(cv.humidity_consensus_stddev::numeric, 1) as humidity_consensus_stddev,
    round(cv.wind_consensus_avg::numeric, 1) as wind_consensus_avg,
    round(cv.wind_consensus_stddev::numeric, 1) as wind_consensus_stddev,
    cv.providers_count,
    
    -- Метаданные
    tp.source_changed_at,
    current_timestamp as processed_dttm

from temp_pivoted tp
left join humidity_pivoted hp
    on tp.provider = hp.provider
    and tp.city = hp.city
    and tp.forecast_run_date = hp.forecast_run_date
    and tp.forecast_run_hour = hp.forecast_run_hour
left join wind_pivoted wp
    on tp.provider = wp.provider
    and tp.city = wp.city
    and tp.forecast_run_date = wp.forecast_run_date
    and tp.forecast_run_hour = wp.forecast_run_hour
left join actual_values av
    on tp.provider = av.provider
    and tp.city = av.city
    and tp.forecast_run_date = av.observation_date
left join consensus_values cv
    on tp.city = cv.city
    and tp.forecast_run_date = cv.observation_date

