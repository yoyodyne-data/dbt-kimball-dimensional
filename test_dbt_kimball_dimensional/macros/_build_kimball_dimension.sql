{%- macro _build_kimball_dimension(config_args) -%}


    {%- set array_columns = config_args['type_10_columns'] -%}
    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    
    WITH
        __dbt_kimball_dimensional_source AS (
            {{ _kimball_source_query(config_args) }}
        )   
        ,__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates AS (
           {{ _kimball_scd_with_duplicates_query(config_args, 
                                                 '__dbt_kimball_dimensional_source') }}
        )
    {% if array_columns %}
        ,__dbt_kimball_dimensional_deduplicated_aggregates AS (
            {{ _kimball_aggregates_query(config_args, 
                                         '__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates') }}
        )
    {% endif %}
        ,__dbt_kimball_dimensional_durable_ids AS (
            {{ _kimball_durable_ids_query(config_args,    
                                          '__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates') }}
        )  
        ,__dbt_kimball_dimensional_slowly_changing_dimensions AS (
            SELECT 
                COALESCE( {{ config_args["dim_key"] }}, ROW_NUMBER() OVER() ) AS {{ config_args["dim_key"] }}
                ,durable_ids.{{ config_args["dim_id"] }}
            {%- for col in array_columns -%}
                ,deduped.all_{{ col }}_values AS all_{{ col }}_values
            {% endfor -%}
            {% for col in config_args["model_query_columns"] -%}
                ,scd.{{ col }} AS {{ col }}
            {%- endfor %}
                ,scd.row_effective_at
                ,scd.row_expired_at
                ,scd.row_is_current
            FROM   
                __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates scd
            {% if array_columns %}
            JOIN
                __dbt_kimball_dimensional_deduplicated_aggregates deduped
            USING 
                ( {{ config_args["DNI"] }} )
            {% endif %}
            JOIN
                __dbt_kimball_dimensional_durable_ids durable_ids
            USING ( {{ config_args["DNI"] }} )
        )
    
        SELECT
            * 
        FROM 
            __dbt_kimball_dimensional_slowly_changing_dimensions 
{%- endmacro -%}
