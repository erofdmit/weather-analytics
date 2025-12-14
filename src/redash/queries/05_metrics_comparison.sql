-- Query 5: Сравнение всех метрик для одного провайдера
-- Использование: Bar chart для сравнения temperature/humidity/wind MAE

SELECT
    provider AS "Провайдер",
    'Температура' AS "Метрика",
    ROUND(avg_temp_mae::numeric, 2) AS "MAE"
FROM dm.dm_provider_accuracy_overall
UNION ALL
SELECT
    provider,
    'Влажность',
    ROUND(avg_humidity_mae::numeric, 2)
FROM dm.dm_provider_accuracy_overall
UNION ALL
SELECT
    provider,
    'Ветер',
    ROUND(avg_wind_mae::numeric, 2)
FROM dm.dm_provider_accuracy_overall
ORDER BY "Провайдер", "Метрика";
