-- Запрос 3: Сравнение провайдеров по всем городам
-- Показывает какой провайдер лучше в каждом городе по выбранной метрике
-- metric_type: 1=temperature, 2=humidity, 3=wind

WITH ranked_providers AS (
    SELECT
        city AS "Город",
        provider AS "Провайдер",
        CASE {{metric_type}}
            WHEN 1 THEN ROUND(avg_temp_mae, 2)
            WHEN 2 THEN ROUND(avg_humidity_mae, 2)
            WHEN 3 THEN ROUND(avg_wind_mae, 2)
        END AS "MAE",
        CASE {{metric_type}}
            WHEN 1 THEN provider_rank_by_temp
            WHEN 2 THEN provider_rank_by_humidity
            WHEN 3 THEN provider_rank_by_wind
        END AS "Ранг"
    FROM dm.dm_provider_accuracy_by_city
)
SELECT "Город", "Провайдер", "MAE", "Ранг"
FROM ranked_providers
WHERE "Ранг" = 1
ORDER BY "Город";


-- Визуализация: Table
-- Показывает лучшего провайдера (ранг = 1) для каждого города
-- Можно добавить Bar Chart для сравнения MAE между городами
