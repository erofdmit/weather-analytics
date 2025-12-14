-- Запрос 2: Точность провайдеров по горизонтам прогноза
-- Показывает как меняется MAE для выбранной метрики и города
-- metric_type: 1=temperature, 2=humidity, 3=wind

WITH horizon_data AS (
    SELECT
        provider AS "Провайдер",
        '1h' AS "Горизонт",
        1 AS sort_order,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_1h, 2)
            WHEN 2 THEN ROUND(humidity_mae_1h, 2)
            WHEN 3 THEN ROUND(wind_mae_1h, 2)
        END AS "MAE"
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '5h', 2,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_5h, 2)
            WHEN 2 THEN ROUND(humidity_mae_5h, 2)
            WHEN 3 THEN ROUND(wind_mae_5h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '10h', 3,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_10h, 2)
            WHEN 2 THEN ROUND(humidity_mae_10h, 2)
            WHEN 3 THEN ROUND(wind_mae_10h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '24h', 4,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_24h, 2)
            WHEN 2 THEN ROUND(humidity_mae_24h, 2)
            WHEN 3 THEN ROUND(wind_mae_24h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '48h', 5,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_48h, 2)
            WHEN 2 THEN ROUND(humidity_mae_48h, 2)
            WHEN 3 THEN ROUND(wind_mae_48h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '72h', 6,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_72h, 2)
            WHEN 2 THEN ROUND(humidity_mae_72h, 2)
            WHEN 3 THEN ROUND(wind_mae_72h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '96h', 7,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_96h, 2)
            WHEN 2 THEN ROUND(humidity_mae_96h, 2)
            WHEN 3 THEN ROUND(wind_mae_96h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '120h', 8,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_120h, 2)
            WHEN 2 THEN ROUND(humidity_mae_120h, 2)
            WHEN 3 THEN ROUND(wind_mae_120h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '144h', 9,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_144h, 2)
            WHEN 2 THEN ROUND(humidity_mae_144h, 2)
            WHEN 3 THEN ROUND(wind_mae_144h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'

    UNION ALL

    SELECT provider, '168h', 10,
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(temp_mae_168h, 2)
            WHEN 2 THEN ROUND(humidity_mae_168h, 2)
            WHEN 3 THEN ROUND(wind_mae_168h, 2)
        END
    FROM dm.dm_provider_accuracy_by_city
    WHERE city = '{{city}}'
)
SELECT "Провайдер", "Горизонт", "MAE"
FROM horizon_data
WHERE "MAE" IS NOT NULL
ORDER BY sort_order, "Провайдер";


-- Визуализация: Line Chart
-- X Column: Горизонт
-- Y Columns: MAE
-- Group By: Провайдер
-- Покажет как растет ошибка с увеличением горизонта прогноза
