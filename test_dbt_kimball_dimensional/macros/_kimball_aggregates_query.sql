{%- macro _kimball_aggregates_query(config_args, duplicates_cte) -%}
    
    SELECT
    {{ config_args["DNI"] }}
    {% for col in config_args["type_10_columns"] -%}
        ,ARRAY_AGG(DISTINCT {{ col }}__item) AS all_{{ col }}_values
    {%- endfor %}
    FROM 
        {{ duplicates_cte }} 
    {% for col in config_args["type_10_columns"] -%}
    ,unnest( all_{{ col }}_values ) as {{ col }}__item
    {%- endfor %}
    GROUP BY 1

{%- endmacro -%}
