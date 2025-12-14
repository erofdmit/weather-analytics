-- Query 1: Общий рейтинг провайдеров
-- Использование: Таблица рейтинга для Dashboard 1

SELECT
    provider AS "Провайдер",
    overall_rank_score AS "Общий ранг",
    ROUND(avg_temp_mae::numeric, 2) AS "Температура MAE (°C)",
    ROUND(avg_humidity_mae::numeric, 2) AS "Влажность MAE (%)",
    ROUND(avg_wind_mae::numeric, 2) AS "Ветер MAE (км/ч)",
    provider_rank_by_temp AS "Ранг по температуре",
    provider_rank_by_humidity AS "Ранг по влажности",
    provider_rank_by_wind AS "Ранг по ветру",
    total_forecasts AS "Всего прогнозов"
FROM dm.dm_provider_accuracy_overall
ORDER BY overall_rank_score ASC;
