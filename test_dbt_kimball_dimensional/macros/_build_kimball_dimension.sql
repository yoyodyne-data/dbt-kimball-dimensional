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
        ,__dbt_kimball_max_key AS (
            {{ _kimball_max_key(config_args) }} 
        )
        ,__dbt_kimball_dimensional_slowly_changing_dimensions AS (
            SELECT 
                *
            FROM
            ( {{ _kimball_compiled_query_helper(config_args, "new") }} 
            UNION
             {{ _kimball_compiled_query_helper(config_args, "existing") }} ) all_records
        )
    
        SELECT
            * 
        FROM 
            __dbt_kimball_dimensional_slowly_changing_dimensions 
{%- endmacro -%}
