{% macro celsius_to_fahrenheit(celsius_column) %}
    round((({{ celsius_column }} * 9/5) + 32)::numeric, 1)
{% endmacro %}
