{% macro wind_direction_to_cardinal(degrees_column) %}
    case
        when {{ degrees_column }} is null then 'Unknown'
        when {{ degrees_column }} between 0 and 22.5 then 'N'
        when {{ degrees_column }} between 22.5 and 67.5 then 'NE'
        when {{ degrees_column }} between 67.5 and 112.5 then 'E'
        when {{ degrees_column }} between 112.5 and 157.5 then 'SE'
        when {{ degrees_column }} between 157.5 and 202.5 then 'S'
        when {{ degrees_column }} between 202.5 and 247.5 then 'SW'
        when {{ degrees_column }} between 247.5 and 292.5 then 'W'
        when {{ degrees_column }} between 292.5 and 337.5 then 'NW'
        when {{ degrees_column }} between 337.5 and 360 then 'N'
        else 'Unknown'
    end
{% endmacro %}
