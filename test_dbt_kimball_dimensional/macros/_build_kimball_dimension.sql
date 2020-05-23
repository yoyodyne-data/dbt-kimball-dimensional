{%- macro _build_kimball_dimension(config_args) -%}


    {%- set has_aggregates = config_args['type_4_columns'] + config_args['type_10_columns'] -%}

    WITH
    __dbt_kimball_dimensional_source AS (
        {{ _kimball_source_query(config_args) }}
    )   
    ,__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates AS (
       {{ _kimball_scd_with_duplicates_query(config_args, '__dbt_kimball_dimensional_source') }}
    )
    {% if has_aggregates %}
    ,__dbt_kimball_dimensional_deduplicated_aggregates AS (
       SELECT
    {{ config_args["DNI"] }}
    {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] %}
     ,ARRAY_AGG(DISTINCT {{ col }}_item) AS {{ col }} 
    {% endfor %}
       FROM __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates 
       {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] %}
     ,unnest( {{ col }} ) as {{ col }}_item
       {% endfor %}
       GROUP BY 1
    )
    {% endif %}

    ,__dbt_kimball_dimensional_durable_ids AS (
       WITH 
       max_existing_id AS (
       {%- if config_args["target_exists"] and not config_args["full_refresh"] -%}
      SELECT
        MAX( {{ config_args["dim_id"] }} ) FROM {{ config_args["backup_relation"] }}
       {%- else -%}
          SELECT
          0 
       {%- endif -%}
          AS max_existing_id
       )
       ,row_numbers AS (
          SELECT
          {{ config_args["DNI"] }}
          , ROW_NUMBER() OVER() AS id_increment
          FROM __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates
          GROUP BY 1
       )
       SELECT
          row_numbers.{{ config_args["DNI"] }}
      ,COALESCE( duplicates.{{ config_args["dim_id"] }}, max_existing_id + id_increment) AS {{ config_args["dim_id"] }}
       FROM 
         row_numbers 
       LEFT JOIN
         __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates duplicates
       USING ( {{ config_args["DNI"] }} )    
       INNER JOIN
         max_existing_id
       ON 1=1
    )
    ,__dbt_kimball_dimensional_slowly_changing_dimensions AS (
    SELECT 
       COALESCE( {{ config_args["dim_key"] }}, ROW_NUMBER() OVER() ) AS {{ config_args["dim_key"] }}
       ,durable_ids.{{ config_args["dim_id"] }}
       {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] %}
    , deduped.all_{{ col }}_values AS all_{{ col }}_values
       {% endfor %}
       {% for col in config_args["target_columns"] %}
      {% if col not in config_args["type_4_columns"] %}
        ,scd.{{ col }} AS {{ col }}
      {% endif %} 
       {% endfor %}
       ,scd.row_effective_at
       ,scd.row_expired_at
       ,scd.row_is_current
    FROM   
       __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates scd
    {% if has_aggregates %}
    JOIN
        __dbt_kimball_dimensional_deduplicated_aggregates deduped
    USING ( {{ config_args["DNI"] }} )
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
