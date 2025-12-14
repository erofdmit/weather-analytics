{{
    config(
        materialized='table',
        tags=['dm', 'forecast', 'accuracy', 'overall']
    )
}}

/*
    DM витрина: Общая точность провайдеров по всем городам.
    
    Агрегированная точность прогнозов для каждого провайдера по всем городам.
    Включает MAE для температуры, влажности и скорости ветра на всех горизонтах прогноза.
    Прогноз сравнивается с реальным средним значением по всем провайдерам.
    
    Источники: ods_weather_forecasts, ods_weather_observations
    Использование: Общий рейтинг провайдеров, выбор лучшего провайдера в целом
*/

WITH forecast_data AS (
    SELECT
        provider,
        forecast_run_date,
        forecast_run_hour,
        hours_ahead,
        city,
        temperature_celsius,
        humidity_percent,
        wind_speed_kph
    FROM {{ ref('ods_weather_forecasts') }}
    WHERE hours_ahead IN (1, 5, 10, 24, 48, 72, 96, 120, 144, 168)
),

-- Реальные значения (среднее по всем провайдерам для каждого момента времени)
actual_values AS (
    SELECT
        city,
        observation_hour,
        AVG(temperature_celsius) AS actual_temperature,
        AVG(humidity_percent) AS actual_humidity,
        AVG(wind_speed_kph) AS actual_wind_speed
    FROM {{ ref('ods_weather_observations') }}
    GROUP BY city, observation_hour
),

-- Вычисляем target_datetime для каждого прогноза
forecast_with_target AS (
    SELECT
        f.*,
        f.forecast_run_hour + (f.hours_ahead || ' hours')::INTERVAL AS target_datetime
    FROM forecast_data f
),

-- Джойним прогнозы с реальными значениями
forecast_vs_actual AS (
    SELECT
        f.provider,
        f.hours_ahead,
        f.temperature_celsius AS forecast_temperature,
        f.humidity_percent AS forecast_humidity,
        f.wind_speed_kph AS forecast_wind_speed,
        a.actual_temperature,
        a.actual_humidity,
        a.actual_wind_speed,
        -- Вычисляем абсолютные ошибки
        ABS(f.temperature_celsius - a.actual_temperature) AS temperature_ae,
        ABS(f.humidity_percent - a.actual_humidity) AS humidity_ae,
        ABS(f.wind_speed_kph - a.actual_wind_speed) AS wind_speed_ae
    FROM forecast_with_target f
    INNER JOIN actual_values a
        ON f.city = a.city
        AND DATE_TRUNC('hour', f.target_datetime) = a.observation_hour
),

-- Агрегируем MAE по каждому горизонту для каждого провайдера (по всем городам)
mae_by_horizon AS (
    SELECT
        provider,
        hours_ahead,
        ROUND(AVG(temperature_ae)::NUMERIC, 2) AS temperature_mae,
        ROUND(AVG(humidity_ae)::NUMERIC, 2) AS humidity_mae,
        ROUND(AVG(wind_speed_ae)::NUMERIC, 2) AS wind_speed_mae,
        COUNT(*) AS forecast_count
    FROM forecast_vs_actual
    GROUP BY provider, hours_ahead
),

-- Пивотим данные по горизонтам
pivoted_mae AS (
    SELECT
        provider,
        
        -- MAE температуры для каждого горизонта
        MAX(CASE WHEN hours_ahead = 1 THEN temperature_mae END) AS temp_mae_1h,
        MAX(CASE WHEN hours_ahead = 5 THEN temperature_mae END) AS temp_mae_5h,
        MAX(CASE WHEN hours_ahead = 10 THEN temperature_mae END) AS temp_mae_10h,
        MAX(CASE WHEN hours_ahead = 24 THEN temperature_mae END) AS temp_mae_24h,
        MAX(CASE WHEN hours_ahead = 48 THEN temperature_mae END) AS temp_mae_48h,
        MAX(CASE WHEN hours_ahead = 72 THEN temperature_mae END) AS temp_mae_72h,
        MAX(CASE WHEN hours_ahead = 96 THEN temperature_mae END) AS temp_mae_96h,
        MAX(CASE WHEN hours_ahead = 120 THEN temperature_mae END) AS temp_mae_120h,
        MAX(CASE WHEN hours_ahead = 144 THEN temperature_mae END) AS temp_mae_144h,
        MAX(CASE WHEN hours_ahead = 168 THEN temperature_mae END) AS temp_mae_168h,
        
        -- MAE влажности для каждого горизонта
        MAX(CASE WHEN hours_ahead = 1 THEN humidity_mae END) AS humidity_mae_1h,
        MAX(CASE WHEN hours_ahead = 5 THEN humidity_mae END) AS humidity_mae_5h,
        MAX(CASE WHEN hours_ahead = 10 THEN humidity_mae END) AS humidity_mae_10h,
        MAX(CASE WHEN hours_ahead = 24 THEN humidity_mae END) AS humidity_mae_24h,
        MAX(CASE WHEN hours_ahead = 48 THEN humidity_mae END) AS humidity_mae_48h,
        MAX(CASE WHEN hours_ahead = 72 THEN humidity_mae END) AS humidity_mae_72h,
        MAX(CASE WHEN hours_ahead = 96 THEN humidity_mae END) AS humidity_mae_96h,
        MAX(CASE WHEN hours_ahead = 120 THEN humidity_mae END) AS humidity_mae_120h,
        MAX(CASE WHEN hours_ahead = 144 THEN humidity_mae END) AS humidity_mae_144h,
        MAX(CASE WHEN hours_ahead = 168 THEN humidity_mae END) AS humidity_mae_168h,
        
        -- MAE скорости ветра для каждого горизонта
        MAX(CASE WHEN hours_ahead = 1 THEN wind_speed_mae END) AS wind_mae_1h,
        MAX(CASE WHEN hours_ahead = 5 THEN wind_speed_mae END) AS wind_mae_5h,
        MAX(CASE WHEN hours_ahead = 10 THEN wind_speed_mae END) AS wind_mae_10h,
        MAX(CASE WHEN hours_ahead = 24 THEN wind_speed_mae END) AS wind_mae_24h,
        MAX(CASE WHEN hours_ahead = 48 THEN wind_speed_mae END) AS wind_mae_48h,
        MAX(CASE WHEN hours_ahead = 72 THEN wind_speed_mae END) AS wind_mae_72h,
        MAX(CASE WHEN hours_ahead = 96 THEN wind_speed_mae END) AS wind_mae_96h,
        MAX(CASE WHEN hours_ahead = 120 THEN wind_speed_mae END) AS wind_mae_120h,
        MAX(CASE WHEN hours_ahead = 144 THEN wind_speed_mae END) AS wind_mae_144h,
        MAX(CASE WHEN hours_ahead = 168 THEN wind_speed_mae END) AS wind_mae_168h,
        
        -- Общее количество прогнозов
        SUM(forecast_count) AS total_forecasts
        
    FROM mae_by_horizon
    GROUP BY provider
),

-- Вычисляем средний MAE по всем горизонтам для ранжирования
provider_avg_mae AS (
    SELECT
        provider,
        -- Средний MAE по всем горизонтам (для температуры)
        ROUND(
            (COALESCE(temp_mae_1h, 0) + COALESCE(temp_mae_5h, 0) + COALESCE(temp_mae_10h, 0) + 
             COALESCE(temp_mae_24h, 0) + COALESCE(temp_mae_48h, 0) + COALESCE(temp_mae_72h, 0) + 
             COALESCE(temp_mae_96h, 0) + COALESCE(temp_mae_120h, 0) + COALESCE(temp_mae_144h, 0) + 
             COALESCE(temp_mae_168h, 0)) / 
            NULLIF((CASE WHEN temp_mae_1h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_5h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_10h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_24h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_48h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_72h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_96h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_120h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_144h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN temp_mae_168h IS NOT NULL THEN 1 ELSE 0 END), 0)
        ::NUMERIC, 2) AS avg_temp_mae,
        
        -- Средний MAE по всем горизонтам (для влажности)
        ROUND(
            (COALESCE(humidity_mae_1h, 0) + COALESCE(humidity_mae_5h, 0) + COALESCE(humidity_mae_10h, 0) + 
             COALESCE(humidity_mae_24h, 0) + COALESCE(humidity_mae_48h, 0) + COALESCE(humidity_mae_72h, 0) + 
             COALESCE(humidity_mae_96h, 0) + COALESCE(humidity_mae_120h, 0) + COALESCE(humidity_mae_144h, 0) + 
             COALESCE(humidity_mae_168h, 0)) / 
            NULLIF((CASE WHEN humidity_mae_1h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_5h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_10h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_24h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_48h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_72h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_96h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_120h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_144h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN humidity_mae_168h IS NOT NULL THEN 1 ELSE 0 END), 0)
        ::NUMERIC, 2) AS avg_humidity_mae,
        
        -- Средний MAE по всем горизонтам (для ветра)
        ROUND(
            (COALESCE(wind_mae_1h, 0) + COALESCE(wind_mae_5h, 0) + COALESCE(wind_mae_10h, 0) + 
             COALESCE(wind_mae_24h, 0) + COALESCE(wind_mae_48h, 0) + COALESCE(wind_mae_72h, 0) + 
             COALESCE(wind_mae_96h, 0) + COALESCE(wind_mae_120h, 0) + COALESCE(wind_mae_144h, 0) + 
             COALESCE(wind_mae_168h, 0)) / 
            NULLIF((CASE WHEN wind_mae_1h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_5h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_10h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_24h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_48h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_72h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_96h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_120h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_144h IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN wind_mae_168h IS NOT NULL THEN 1 ELSE 0 END), 0)
        ::NUMERIC, 2) AS avg_wind_mae,
        
        total_forecasts,
        temp_mae_1h, temp_mae_5h, temp_mae_10h, temp_mae_24h, temp_mae_48h,
        temp_mae_72h, temp_mae_96h, temp_mae_120h, temp_mae_144h, temp_mae_168h,
        humidity_mae_1h, humidity_mae_5h, humidity_mae_10h, humidity_mae_24h, humidity_mae_48h,
        humidity_mae_72h, humidity_mae_96h, humidity_mae_120h, humidity_mae_144h, humidity_mae_168h,
        wind_mae_1h, wind_mae_5h, wind_mae_10h, wind_mae_24h, wind_mae_48h,
        wind_mae_72h, wind_mae_96h, wind_mae_120h, wind_mae_144h, wind_mae_168h
    FROM pivoted_mae
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['provider']) }} AS provider_id,
    provider,
    
    -- Средние MAE по всем горизонтам
    avg_temp_mae,
    avg_humidity_mae,
    avg_wind_mae,
    
    -- MAE температуры по горизонтам
    temp_mae_1h,
    temp_mae_5h,
    temp_mae_10h,
    temp_mae_24h,
    temp_mae_48h,
    temp_mae_72h,
    temp_mae_96h,
    temp_mae_120h,
    temp_mae_144h,
    temp_mae_168h,
    
    -- MAE влажности по горизонтам
    humidity_mae_1h,
    humidity_mae_5h,
    humidity_mae_10h,
    humidity_mae_24h,
    humidity_mae_48h,
    humidity_mae_72h,
    humidity_mae_96h,
    humidity_mae_120h,
    humidity_mae_144h,
    humidity_mae_168h,
    
    -- MAE скорости ветра по горизонтам
    wind_mae_1h,
    wind_mae_5h,
    wind_mae_10h,
    wind_mae_24h,
    wind_mae_48h,
    wind_mae_72h,
    wind_mae_96h,
    wind_mae_120h,
    wind_mae_144h,
    wind_mae_168h,
    
    -- Ранг провайдера (по средней температурной ошибке)
    ROW_NUMBER() OVER (ORDER BY avg_temp_mae ASC) AS provider_rank_by_temp,
    
    -- Ранг провайдера (по средней ошибке влажности)
    ROW_NUMBER() OVER (ORDER BY avg_humidity_mae ASC) AS provider_rank_by_humidity,
    
    -- Ранг провайдера (по средней ошибке ветра)
    ROW_NUMBER() OVER (ORDER BY avg_wind_mae ASC) AS provider_rank_by_wind,
    
    -- Общий ранг (среднее арифметическое рангов)
    ROUND(
        (ROW_NUMBER() OVER (ORDER BY avg_temp_mae ASC) +
         ROW_NUMBER() OVER (ORDER BY avg_humidity_mae ASC) +
         ROW_NUMBER() OVER (ORDER BY avg_wind_mae ASC)) / 3.0
    ::NUMERIC, 2) AS overall_rank_score,
    
    -- Количество прогнозов
    total_forecasts,
    
    -- Метаданные
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM provider_avg_mae
WHERE total_forecasts >= 10  -- Минимум 10 прогнозов для статистической значимости
ORDER BY overall_rank_score

