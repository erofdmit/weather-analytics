-- Кастомный тест: проверка разумности температурных данных
-- Этот тест проверяет, что разница между ощущаемой и реальной температурой не слишком большая

{% test reasonable_feels_like_difference(model, column_name, max_difference=15) %}

select
    *
from {{ model }}
where
    abs({{ column_name }} - temperature_celsius) > {{ max_difference }}
    and {{ column_name }} is not null
    and temperature_celsius is not null

{% endtest %}
