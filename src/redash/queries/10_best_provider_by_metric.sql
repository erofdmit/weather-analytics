-- Запрос 1: Какой провайдер лучше всех прогнозирует для города?
-- Показывает топ-провайдера по каждой метрике с фильтром по городу

SELECT
    city AS "Город",
    provider AS "Провайдер",
    ROUND(avg_temp_mae, 2) AS "MAE Температура (°C)",
    ROUND(avg_humidity_mae, 2) AS "MAE Влажность (%)",
    ROUND(avg_wind_mae, 2) AS "MAE Ветер (м/с)",
    provider_rank_by_temp AS "Ранг по температуре",
    provider_rank_by_humidity AS "Ранг по влажности",
    provider_rank_by_wind AS "Ранг по ветру",
    ROUND(overall_rank_score, 2) AS "Общий рейтинг",
    total_forecasts AS "Кол-во прогнозов"
FROM dm.dm_provider_accuracy_by_city
WHERE city = '{{city}}'
ORDER BY overall_rank_score ASC;

-- Визуализация: Table
-- Лучший провайдер = минимальный overall_rank_score (1-е место)
-- Меньше MAE = точнее прогноз
