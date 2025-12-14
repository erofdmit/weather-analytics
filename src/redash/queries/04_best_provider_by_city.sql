-- Query 4: Лучший провайдер для каждого города
-- Использование: Таблица с победителями по городам

WITH best_ranks AS (
    SELECT
        city,
        MIN(overall_rank_score) as min_rank
    FROM dm.dm_provider_accuracy_by_city
    GROUP BY city
)
SELECT
    p.city AS "Город",
    p.provider AS "Лучший провайдер",
    p.overall_rank_score AS "Балл",
    ROUND(p.avg_temp_mae::numeric, 2) AS "Температура MAE (°C)",
    ROUND(p.avg_humidity_mae::numeric, 2) AS "Влажность MAE (%)",
    ROUND(p.avg_wind_mae::numeric, 2) AS "Ветер MAE (км/ч)",
    p.total_forecasts AS "Количество прогнозов"
FROM dm.dm_provider_accuracy_by_city p
INNER JOIN best_ranks b
    ON p.city = b.city
    AND p.overall_rank_score = b.min_rank
ORDER BY p.city;
