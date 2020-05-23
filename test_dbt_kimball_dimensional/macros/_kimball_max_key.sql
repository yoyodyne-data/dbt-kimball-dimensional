{%- macro _kimball_max_key(config_args) -%}

    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    SELECT
    {% if incremental -%}
        MAX( {{ config_args["dim_key"] }} ) AS max_existing_key FROM {{ config_args["existing_relation"] }}
    {% else -%}
        0 AS max_existing_key
    {%- endif %}
{%- endmacro -%}


{%- macro _kimball_compiled_query_helper(config_args, dataset) -%}
    {{ _kimball_compiled_records_query(config_args,
                                       dataset,
                                       '__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates',
                                       '__dbt_kimball_dimensional_deduplicated_aggregates',
                                       '__dbt_kimball_dimensional_durable_ids',
                                       '__dbt_kimball_max_key') }}
{%- endmacro -%}



{%- macro _kimball_compiled_records_query(config_args, 
                                              dataset,
                                              duplicates_cte, 
                                              deduped_aggregates_cte, 
                                              durable_ids_cte,
                                              max_key_cte ) -%}
    SELECT
    {% if dataset == "new" -%} 
        (max_existing_key.max_existing_key + 
            ROW_NUMBER() OVER()) AS {{ config_args["dim_key"] }}
    {% elif dataset == "existing" %}
        {{ config_args["dim_key"] }} 
    {%- endif %} 
        ,durable_ids.{{ config_args["dim_id"] }}
    {% for col in config_args["type_10_columns"] -%}
        ,deduped.all_{{ col }}_values AS all_{{ col }}_values
    {% endfor %}
    {% for col in config_args["model_query_columns"] -%}
        ,scd.{{ col }} AS {{ col }}
    {%- endfor %}
        ,scd.row_effective_at
        ,scd.row_expired_at
        ,scd.row_is_current
    FROM   
       {{ duplicates_cte }} scd
    {% if config_args["type_10_columns"]  %}
    JOIN
        {{ deduped_aggregates_cte }} deduped
    USING 
        ( {{ config_args["DNI"] }} )
    {% endif %}
    JOIN
        {{ durable_ids_cte }}  durable_ids
    USING ( {{ config_args["DNI"] }} )
    INNER JOIN
        {{ max_key_cte }} max_existing_key
    ON 1=1 

    WHERE
        {{ config_args["dim_key"] }} IS {% if dataset=="existing" %} NOT {% endif %} NULL
{%- endmacro -%}
