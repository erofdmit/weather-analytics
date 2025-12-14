-- Query 3: Точность провайдеров по городам
-- Использование: Фильтруемая таблица и heatmap для Dashboard 2
-- Параметр: {{city}} - выпадающий список городов (опционально)

SELECT
    city AS "Город",
    provider AS "Провайдер",
    overall_rank_score AS "Общий ранг",
    ROUND(avg_temp_mae::numeric, 2) AS "Температура MAE (°C)",
    ROUND(avg_humidity_mae::numeric, 2) AS "Влажность MAE (%)",
    ROUND(avg_wind_mae::numeric, 2) AS "Ветер MAE (км/ч)",
    provider_rank_by_temp AS "Ранг по температуре",
    provider_rank_by_humidity AS "Ранг по влажности",
    provider_rank_by_wind AS "Ранг по ветру",
    total_forecasts AS "Прогнозов"
FROM dm.dm_provider_accuracy_by_city
WHERE
    CASE
        WHEN '{{city}}' = '' OR '{{city}}' IS NULL THEN TRUE
        ELSE city = '{{city}}'
    END
ORDER BY city, overall_rank_score;

