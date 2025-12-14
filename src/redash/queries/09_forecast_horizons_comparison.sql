-- Query 9: Прогноз vs Факт - Динамика по всем горизонтам
-- Показывает как точность прогноза менялась с ростом горизонта

WITH latest_forecasts AS (
    SELECT
        city,
        provider,
        forecast_run_date,
        -- Берем один день для анализа (можно параметризовать)
        temp_forecast_24h,
        temp_forecast_48h,
        temp_forecast_72h,
        temp_forecast_96h,
        temp_forecast_120h,
        temp_forecast_144h,
        temp_forecast_168h,
        temp_actual_avg
    FROM dm.dm_forecast_pivot
    WHERE
        city = '{{city}}'
        AND forecast_run_date = (
            SELECT MAX(forecast_run_date)
            FROM dm.dm_forecast_pivot
            WHERE city = '{{city}}'
        )
),
all_data AS (
    SELECT
        'Прогноз' AS "Тип",
        '24h' AS "Горизонт",
        1 AS type_order,
        1 AS horizon_order,
        ROUND(AVG(temp_forecast_24h)::numeric, 1) AS "Температура"
    FROM latest_forecasts
    WHERE temp_forecast_24h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '48h', 1, 2, ROUND(AVG(temp_forecast_48h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_48h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '72h', 1, 3, ROUND(AVG(temp_forecast_72h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_72h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '96h', 1, 4, ROUND(AVG(temp_forecast_96h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_96h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '120h', 1, 5, ROUND(AVG(temp_forecast_120h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_120h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '144h', 1, 6, ROUND(AVG(temp_forecast_144h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_144h IS NOT NULL
    UNION ALL
    SELECT 'Прогноз', '168h', 1, 7, ROUND(AVG(temp_forecast_168h)::numeric, 1)
    FROM latest_forecasts WHERE temp_forecast_168h IS NOT NULL
    UNION ALL
    SELECT
        'Факт' AS "Тип",
        'Текущий' AS "Горизонт",
        2 AS type_order,
        8 AS horizon_order,
        ROUND(AVG(temp_actual_avg)::numeric, 1) AS "Температура"
    FROM latest_forecasts
    WHERE temp_actual_avg IS NOT NULL
)
SELECT "Тип", "Горизонт", "Температура"
FROM all_data
ORDER BY type_order, horizon_order;

-- Параметр city для фильтра
-- city: Dropdown List -> SELECT DISTINCT city FROM dm.dm_forecast_pivot ORDER BY city
