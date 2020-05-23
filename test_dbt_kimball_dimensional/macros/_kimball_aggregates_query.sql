{%- macro _kimball_aggregates_query(config_args, duplicates_cte) -%}
    
    SELECT
    {{ config_args["DNI"] }}
    {%- for col in config_args["type_10_columns"] -%}
        ,ARRAY_AGG(DISTINCT {{ col }}__item) AS {{ col }} 
    {%- endfor -%}
    FROM 
        {{ duplicates_cte }} 
    {%- for col in config_args["type_10_columns"] -%}
    ,unnest( {{ col }} ) as {{ col }}__item
    {%- endfor -%}
    GROUP BY 1

{%- endmacro -%}
