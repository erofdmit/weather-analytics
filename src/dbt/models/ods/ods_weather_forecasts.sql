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
        wind_speed_ms,
        wind_speed_kph,
        pressure_hpa,
        precipitation_mm,
        cloud_cover_percent,
        visibility_km,
        uv_index,
        feels_like_celsius,
        wind_direction_degrees,
        weather_condition,
        precipitation_probability,
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

    -- Температурные показатели
    temperature_celsius,
    feels_like_celsius,
    round((temperature_celsius * 9.0 / 5.0 + 32)::numeric, 1) as temperature_fahrenheit,

    -- Метеорологические параметры
    humidity_percent,
    wind_speed_ms,
    wind_speed_kph,
    pressure_hpa,
    precipitation_mm,
    cloud_cover_percent,
    visibility_km,
    uv_index,
    wind_direction_degrees,

    -- Направление ветра (стороны света)
    case
        when wind_direction_degrees between 0 and 22.5 then 'N'
        when wind_direction_degrees between 22.5 and 67.5 then 'NE'
        when wind_direction_degrees between 67.5 and 112.5 then 'E'
        when wind_direction_degrees between 112.5 and 157.5 then 'SE'
        when wind_direction_degrees between 157.5 and 202.5 then 'S'
        when wind_direction_degrees between 202.5 and 247.5 then 'SW'
        when wind_direction_degrees between 247.5 and 292.5 then 'W'
        when wind_direction_degrees between 292.5 and 337.5 then 'NW'
        when wind_direction_degrees between 337.5 and 360 then 'N'
        else 'Unknown'
    end as wind_direction_cardinal,

    weather_condition,
    precipitation_probability,

    -- Метаданные
    forecast_created_at,
    source_changed_at,
    current_timestamp as processed_dttm

from deduped
