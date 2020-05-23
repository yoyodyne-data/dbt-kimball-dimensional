{%- macro _kimball_scd_with_duplicates_query(config_args, source_cte) -%}

    SELECT 
        {{ config_args["dim_key"] }}
        ,{{ config_args["dim_id"] }}
    -- type 0 
    {% for col in config_args["type_0_columns"] -%}
        ,LAST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
    {%- endfor -%}

    -- type 1
    {% for col in config_args["type_1_columns"] -%}
        ,FIRST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
    {%- endfor -%}

    -- type 4 + 10
    {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] -%}
        ,array_agg( {{col}} ) OVER (PARTITION BY {{ config_args["DNI"] }}) AS all_{{col}}_values
    {%- endfor -%}
    
    {% for col in config_args["model_query_columns"] -%}
        {%- if col not in config_args["type_0_columns"] + config_args["type_1_columns"] + config_args["type_4_columns"] -%}
        ,{{ col }} 
        {%- endif -%}
    {%- endfor -%}

    -- SCD additions
        ,CASE WHEN LAST_VALUE( {{ config_args["CDC"] }} ) over natural_key_window = {{ config_args["CDC"] }}
            THEN '{{ config_args["beginning_of_time"] }}' 
            ELSE {{ config_args["CDC"] }}
        END AS row_effective_at
        ,( LAG( {{ config_args["CDC"] }}, 1, '9999-12-31') over natural_key_window ) + interval '-1 second' AS row_expired_at
        ,CASE WHEN FIRST_VALUE( {{ config_args["CDC"] }} ) over natural_key_window  = {{ config_args["CDC"] }}
            THEN TRUE
            ELSE FALSE
        END AS row_is_current
    FROM 
        {{ source_cte }}
    WINDOW natural_key_window AS 
        (PARTITION BY {{ config_args["DNI"] }}  
         ORDER BY {{ config_args["CDC"] }} DESC 
         RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
    ORDER BY {{ config_args["DNI"] }}, {{ config_args["CDC"] }} 

{%- endmacro -%}
