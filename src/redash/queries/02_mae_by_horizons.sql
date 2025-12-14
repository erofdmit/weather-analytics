-- Query 2: MAE температуры по горизонтам прогноза
-- Использование: Line chart для визуализации роста ошибки с увеличением горизонта

SELECT
    provider AS "Провайдер",
    horizon AS "Горизонт",
    ROUND(mae::numeric, 2) AS "MAE (°C)"
FROM (
    SELECT provider, '1h' as horizon, temp_mae_1h as mae FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '5h', temp_mae_5h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '10h', temp_mae_10h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '24h', temp_mae_24h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '48h', temp_mae_48h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '72h', temp_mae_72h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '96h', temp_mae_96h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '120h', temp_mae_120h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '144h', temp_mae_144h FROM dm.dm_provider_accuracy_overall
    UNION ALL
    SELECT provider, '168h', temp_mae_168h FROM dm.dm_provider_accuracy_overall
) t
ORDER BY
    CASE horizon
        WHEN '1h' THEN 1 WHEN '5h' THEN 2 WHEN '10h' THEN 3
        WHEN '24h' THEN 4 WHEN '48h' THEN 5 WHEN '72h' THEN 6
        WHEN '96h' THEN 7 WHEN '120h' THEN 8 WHEN '144h' THEN 9 WHEN '168h' THEN 10
    END,
    provider;
