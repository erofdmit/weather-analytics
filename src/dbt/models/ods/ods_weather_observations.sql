{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['city', 'provider', 'observation_time'],
        tags=['ods', 'weather', 'observations']
    )
}}

/*
    ODS модель для наблюдений за погодой от разных провайдеров.
    Инкрементальная загрузка с delete+insert.
*/

with source_data as (
    select
        city,
        country,
        latitude,
        longitude,
        provider,
        observation_time,
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
        created_at,
        updated_at,
        coalesce(updated_at, created_at, observation_time) as source_changed_at
    from {{ ref('stg_weather_current') }}
    where 1 = 1

    {% if is_incremental() %}
      and coalesce(updated_at, created_at, observation_time) >= (
          select coalesce(max(source_changed_at), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
),

deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by city, provider, observation_time
                order by source_changed_at desc, updated_at desc nulls last, created_at desc nulls last
            ) as rn
        from source_data
    ) t
    where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'provider', 'observation_time']) }} as observation_id,
    city,
    country,
    latitude,
    longitude,
    provider,
    observation_time,

    -- Основные параметры для сравнения с прогнозами
    temperature_celsius,
    humidity_percent,
    wind_speed_kph,

    -- Метаданные
    created_at as source_created_at,
    updated_at as source_updated_at,
    source_changed_at,
    current_timestamp as processed_dttm

from deduped
