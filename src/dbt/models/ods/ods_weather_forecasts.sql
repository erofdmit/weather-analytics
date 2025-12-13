{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['city', 'provider', 'forecast_timestamp', 'hours_ahead_interval'],
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
        hours_ahead,
        temperature_celsius,
        humidity_percent,
        wind_speed_kph,
        forecast_created_at,
        coalesce(forecast_created_at, forecast_timestamp) as source_changed_at
    from {{ ref('stg_weather_forecast') }}
    where 1 = 1
      and hours_ahead >= 0
      and hours_ahead <= 48

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
        case
            when hours_ahead between 0.5 and 1.5 then 1
            when hours_ahead between 2.5 and 3.5 then 3
            when hours_ahead between 5.5 and 6.5 then 6
            when hours_ahead between 11.5 and 12.5 then 12
            when hours_ahead between 23.5 and 24.5 then 24
            when hours_ahead between 47.5 and 48.5 then 48
            else null
        end as hours_ahead_interval
    from source_data
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by city, provider, forecast_timestamp, hours_ahead_interval
                order by source_changed_at desc, forecast_created_at desc nulls last
            ) as rn
        from with_intervals
        where hours_ahead_interval is not null
    ) t
    where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'forecast_timestamp', 'hours_ahead_interval']) }} as forecast_id,
    city,
    country,
    latitude,
    longitude,
    provider,
    forecast_timestamp,
    hours_ahead,
    hours_ahead_interval,

    -- Основные параметры для сравнения с реальными наблюдениями
    temperature_celsius,
    humidity_percent,
    wind_speed_kph,

    -- Метаданные
    forecast_created_at,
    source_changed_at,
    current_timestamp as processed_dttm

from deduped
