{%- macro _kimball_durable_ids_query(config_args, duplicates_cte) -%}
    
    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    WITH 
        max_existing_id AS (
            SELECT
        {% if incremental -%}
                MAX( {{ config_args["dim_id"] }} ) FROM {{ config_args["existing_relation"] }}
        {% else -%}
     		0 
        {%- endif %}
            AS max_existing_id
        )
        ,row_numbers AS (
            SELECT
                {{ config_args["DNI"] }}
                , ROW_NUMBER() OVER() AS id_increment
            FROM 
                {{ duplicates_cte }}
            GROUP BY 1
        )    
        SELECT
            row_numbers.{{ config_args["DNI"] }} AS {{ config_args["DNI"] }}
            ,COALESCE(duplicates.{{ config_args["dim_id"] }}, 
                      max_existing_id + id_increment) AS {{ config_args["dim_id"] }}
        FROM 
            row_numbers 
        LEFT JOIN
            {{ duplicates_cte }} duplicates
        USING ( {{ config_args["DNI"] }} )    
        INNER JOIN
            max_existing_id
        ON 1=1
{%- endmacro -%}
