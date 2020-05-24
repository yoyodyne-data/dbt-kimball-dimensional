{%- macro _kimball_cdc_predicate_lookback_type_partial(config_args) -%}
    {%- if config_args['cdc_data_type'] in ('Date','DateTime',) -%}
    {{ xdb.dateadd('day',(config_args["lookback_window"] * -1) ,'(SELECT max_cdc FROM target_max) ') }}
    {%- else -%}
    ( {{ config_args["lookback_window"] }} * -1) + (SELECT max_cdc FROM target_max)
    {%- endif -%}
{%- endmacro -%}


{%- macro _kimball_cdc_predicate_calculation(config_args) -%}

    {%- if config_args["lookback_window"] | lower == "all" -%}
         {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
        NOT IN
        (SELECT     
            {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
        FROM 
            {{ config_args["existing_relation"] }})
    {%- elif config_args["lookback_window"] > 0 -%}
        {{ config_args["CDC"] }} > 
        {{ _kimball_cdc_predicate_lookback_type_partial(config_args) }}
    AND 
     {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
    NOT IN
    (SELECT     
        {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
    FROM 
        {{ config_args["existing_relation"] }}
    WHERE 
        {{ config_args["CDC"] }} > 
        {{ _kimball_cdc_predicate_lookback_type_partial(config_args) }}
    ) 
    {%- else -%}
        {{ config_args["CDC"] }} > 
        (SELECT max_cdc FROM _target_max) 
    {%- endif -%}
{%- endmacro -%}




{%- macro _kimball_source_query(config_args) -%}
   /*{# The query partial that defines our "source", ie records to be operated on.
        ARGS:
          - config_args (dict) an object containing the full materialization args set.
        RETURNS: the CTE partial `__dbt_kimball_dimensional_source`.
   #}*/
   
    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    WITH 
        _base_source AS (   
            {{ config_args["sql"] }}
        )   
    {%- if incremental -%}
        ,_target_max AS (
            SELECT 
                MAX( {{ config_args['CDC'] }} ) as max_cdc  
            FROM
                {{ config_args["existing_relation"] }}
        )
        ,_from_source AS (
            SELECT
                NULL::numeric AS {{ config_args["dim_key"] }}
                ,NULL::numeric AS {{ config_args["dim_id"] }}
                ,*
            FROM 
                _base_source
            WHERE
            {{ _kimball_cdc_predicate_calculation(config_args) }}
        )
        ,_from_target AS (
            SELECT
                *
            FROM
                {{ config_args["existing_relation"] }}
            WHERE 
                {{ config_args["DNI"] }} IN (SELECT 
                                                {{ config_args["DNI"] }} 
                                            FROM 
                                                _from_source )
        )
        ,_final_source AS (
            SELECT 
                *
            FROM 
        (SELECT 
            {{ config_args["dim_key"] }}
            ,{{ config_args["dim_id"] }}
        {% for col in config_args['model_query_columns'] %}
            ,{{ col }}
        {% endfor %}
        FROM
           _from_source
                    UNION
            SELECT 
            {{ config_args["dim_key"] }}
            ,{{ config_args["dim_id"] }}
        {% for col in config_args['model_query_columns'] -%}
            ,{{ col }}
        {% endfor %}
        FROM
           _from_target) unioned
        )
    {%- else -%}
        ,_final_source AS (
            SELECT 
                NULL::numeric AS {{ config_args["dim_key"] }}
                ,NULL::numeric AS {{ config_args["dim_id"] }}
                ,*
            FROM
                _base_source
        )
    {%- endif -%} 
          
        SELECT 
            *
        FROM 
            _final_source
{%- endmacro -%}
