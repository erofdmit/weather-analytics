{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['city', 'provider', 'forecast_timestamp', 'hours_ahead'],
        tags=['ods', 'weather', 'forecasts']
    )
}}

/*
    ODS модель для прогнозов погоды от разных провайдеров.
    Инкрементальная загрузка с merge.
*/

with source_data as (
    select
        city,
        country,
        latitude,
        longitude,
        provider,
        forecast_timestamp,
        forecast_run_hour,
        hours_ahead,
        temperature_celsius,
        humidity_percent,
        wind_speed_kph,
        forecast_created_at,
        coalesce(forecast_created_at, forecast_timestamp) as source_changed_at
    from {{ ref('stg_weather_forecast') }}
    where 1 = 1
      and hours_ahead in (1, 5, 10, 24, 48, 72, 96, 120, 144, 168)

    {% if is_incremental() %}
      and coalesce(forecast_created_at, forecast_timestamp) >= (
          select coalesce(max(source_changed_at), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
),

with_intervals as (
    select
        *,
        -- Вычисляемые поля для аналитики
        case 
            when hours_ahead <= 10 then 'short_term'    -- краткосрочный (до 10 часов)
            when hours_ahead <= 48 then 'medium_term'   -- среднесрочный (до 2 дней)
            else 'long_term'                             -- долгосрочный (более 2 дней)
        end as forecast_horizon_category,
        
        -- Целевое время прогноза (для удобства join с observations)
        forecast_run_hour + (hours_ahead || ' hours')::interval as target_datetime
    from source_data
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by city, provider, forecast_timestamp, hours_ahead
                order by source_changed_at desc, forecast_created_at desc nulls last
            ) as rn
        from with_intervals
        where hours_ahead is not null
    ) t
    where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'forecast_timestamp', 'hours_ahead']) }} as forecast_id,
    city,
    country,
    latitude,
    longitude,
    provider,
    forecast_timestamp,
    forecast_run_hour,
    hours_ahead,
    
    -- Вычисляемые поля для аналитики
    forecast_horizon_category,
    target_datetime,
    extract(dow from forecast_timestamp) as forecast_day_of_week,
    extract(hour from forecast_timestamp) as forecast_hour_of_day,
    date_trunc('day', forecast_run_hour) as forecast_run_date,

    -- Основные параметры для сравнения с реальными наблюдениями
    temperature_celsius,
    humidity_percent,
    wind_speed_kph,

    -- Метаданные
    forecast_created_at,
    source_changed_at,
    current_timestamp as processed_dttm

from deduped
