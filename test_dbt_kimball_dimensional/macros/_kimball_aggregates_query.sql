{%- macro _kimball_aggregates_query(config_args, duplicates_cte) -%}
    
    {%- set array_cols = config_args["type_4_columns"] + config_args["type_10_columns"] -%}
    SELECT
    {{ config_args["DNI"] }}
    {%- for col in array_cols -%}
        ,ARRAY_AGG(DISTINCT {{ col }}__item) AS {{ col }} 
    {%- endfor -%}
    FROM 
        {{ duplicates_cte }} 
    {%- for col in array_cols -%}
    ,unnest( {{ col }} ) as {{ col }}__item
    {%- endfor -%}
    GROUP BY 1

{%- endmacro -%}
