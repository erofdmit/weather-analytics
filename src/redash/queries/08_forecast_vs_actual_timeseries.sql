-- Query 8: Прогноз vs Факт - временной ряд
-- Показывает динамику прогнозов температуры на 24 часа вперед vs реальность

WITH daily_forecasts AS (
    SELECT DISTINCT
        DATE(forecast_run_date) AS forecast_date,
        city,
        provider,
        -- Берем первый прогноз дня на 24h вперед
        FIRST_VALUE(temp_forecast_24h) OVER (
            PARTITION BY DATE(forecast_run_date), city, provider
            ORDER BY forecast_run_hour
        ) AS predicted_temp_24h,
        -- И актуальное значение
        FIRST_VALUE(temp_actual_avg) OVER (
            PARTITION BY DATE(forecast_run_date), city, provider
            ORDER BY forecast_run_hour
        ) AS actual_temp
    FROM dm.dm_forecast_pivot
    WHERE
        city = '{{city}}'
        AND provider = '{{provider}}'
        AND temp_forecast_24h IS NOT NULL
        AND temp_actual_avg IS NOT NULL
)
SELECT
    forecast_date AS "Дата",
    ROUND(predicted_temp_24h::numeric, 1) AS "Прогноз на завтра",
    ROUND(actual_temp::numeric, 1) AS "Факт сегодня",
    ROUND((predicted_temp_24h - actual_temp)::numeric, 1) AS "Ошибка",
    ROUND(ABS(predicted_temp_24h - actual_temp)::numeric, 1) AS "Абсолютная ошибка"
FROM daily_forecasts
ORDER BY forecast_date DESC
LIMIT 30;

-- Параметры для Redash:
-- city: Dropdown List -> SELECT DISTINCT city FROM dm.dm_forecast_pivot ORDER BY city
-- provider: Dropdown List -> SELECT DISTINCT provider FROM dm.dm_forecast_pivot ORDER BY provider
