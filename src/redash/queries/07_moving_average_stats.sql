-- Query 7: Сравнение провайдеров с moving average
-- Показывает сглаженные тренды точности

WITH percentiles AS (
    SELECT
        provider,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_temp_mae) AS median_mae,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY avg_temp_mae) AS q1_mae,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY avg_temp_mae) AS q3_mae
    FROM dm.dm_provider_accuracy_by_city
    GROUP BY provider
),
city_stats AS (
    SELECT
        c.city,
        c.provider,
        c.avg_temp_mae,

        -- Скользящее среднее по 3 городам (сортированным по названию)
        AVG(c.avg_temp_mae) OVER (
            PARTITION BY c.provider
            ORDER BY c.city
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS moving_avg_3,

        -- Минимальный и максимальный MAE среди соседних городов
        MIN(c.avg_temp_mae) OVER (
            PARTITION BY c.provider
            ORDER BY c.city
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS local_min,

        MAX(c.avg_temp_mae) OVER (
            PARTITION BY c.provider
            ORDER BY c.city
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
        ) AS local_max,

        p.median_mae,
        p.q1_mae,
        p.q3_mae

    FROM dm.dm_provider_accuracy_by_city c
    JOIN percentiles p ON c.provider = p.provider
)
SELECT
    city AS "Город",
    provider AS "Провайдер",
    ROUND(avg_temp_mae::numeric, 2) AS "MAE",
    ROUND(moving_avg_3::numeric, 2) AS "Скользящее среднее (3)",
    ROUND(median_mae::numeric, 2) AS "Медиана",
    ROUND(q1_mae::numeric, 2) AS "Q1 (25%)",
    ROUND(q3_mae::numeric, 2) AS "Q3 (75%)",
    ROUND((q3_mae - q1_mae)::numeric, 2) AS "IQR (разброс)",
    ROUND(local_min::numeric, 2) AS "Локальный мин",
    ROUND(local_max::numeric, 2) AS "Локальный макс"
FROM city_stats
ORDER BY provider, city;
