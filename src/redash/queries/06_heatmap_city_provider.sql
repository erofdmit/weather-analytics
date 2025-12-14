-- Query 6: Heatmap данные - MAE по городам и провайдерам
-- Использование: Heatmap visualization

SELECT
    city AS "Город",
    provider AS "Провайдер",
    ROUND(avg_temp_mae::numeric, 2) AS "MAE"
FROM dm.dm_provider_accuracy_by_city
ORDER BY city, provider;
