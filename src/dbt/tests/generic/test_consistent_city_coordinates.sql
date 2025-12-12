-- Кастомный тест: проверка корректности координат городов
-- Города не должны менять свои координаты со временем

{% test consistent_city_coordinates(model, city_column, lat_column, lon_column, tolerance=0.1) %}

with city_coords as (
    select
        {{ city_column }} as city,
        {{ lat_column }} as latitude,
        {{ lon_column }} as longitude
    from {{ model }}
    group by {{ city_column }}, {{ lat_column }}, {{ lon_column }}
),

city_variations as (
    select
        city,
        count(*) as coord_variations,
        max(latitude) - min(latitude) as lat_range,
        max(longitude) - min(longitude) as lon_range
    from city_coords
    group by city
)

select *
from city_variations
where
    coord_variations > 1
    and (lat_range > {{ tolerance }} or lon_range > {{ tolerance }})

{% endtest %}
