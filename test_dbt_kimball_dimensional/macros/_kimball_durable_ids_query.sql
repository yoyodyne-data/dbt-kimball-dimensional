{%- macro _kimball_durable_ids_query(config_args, duplicates_cte) -%}
    
    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    WITH 
        max_existing_id AS (
            SELECT
        {% if incremental -%}
                MAX( {{ config_args["dim_id"] }} ) AS max_existing_id FROM {{ config_args["existing_relation"] }}
        {% else -%}
            0 AS max_existing_id
        {%- endif %}
        )
        ,mixed_state_dim_ids AS (
            SELECT
                {{ config_args["DNI"] }} AS {{ config_args["DNI"] }}
                ,MAX( {{ config_args["dim_id"] }} ) AS {{ config_args["dim_id"] }} 
            FROM
                {{ duplicates_cte }}
            GROUP BY 1
        )
        ,row_numbers AS (
            SELECT
                {{ config_args["DNI"] }}
                , ROW_NUMBER() OVER() AS id_increment
            FROM 
                mixed_state_dim_ids
            WHERE
                {{ config_args["dim_id"] }} IS NULL    
            GROUP BY 1
        )    
        SELECT
            mixed_state_dim_ids.{{ config_args["DNI"] }} AS {{ config_args["DNI"] }}
            ,COALESCE(mixed_state_dim_ids.{{ config_args["dim_id"] }}, 
                      max_existing_id.max_existing_id + id_increment) AS {{ config_args["dim_id"] }}
        FROM 
            mixed_state_dim_ids
        LEFT JOIN
            row_numbers
        USING ( {{ config_args["DNI"] }} )    
        INNER JOIN
            max_existing_id
        ON 1=1
{%- endmacro -%}
