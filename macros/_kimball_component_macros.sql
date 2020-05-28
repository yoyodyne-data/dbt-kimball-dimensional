{%- macro _kimball_get_columns(existing_relation,sql,type_10_columns) -%}
    /*{# gets the column names and datatypes for the target table.
        ARGS:
            - existing_relation (Relation, None) the DBT relation object if the relation already exists or none if it does not.
            - sql (string) the base sql statement from the model.
            - type_10_columns (list) the columns that will have type 10 materializations.
        RETURNS: a list of dicts with column name and data type.
    #}*/
        {% set columns = [] %}
        {% if existing_realtion is none %}
            {% set stub_sql %}
            WITH __dbt_kimball_dimensional_stub AS (
             {{ sql }} 
            )
            SELECT * FROM __dbt_kimball_dimensional_stub LIMIT 1
            {% endset %}
            {% set structure = run_query(stub_sql) %}
            {% for col in structure.columns %}
                {% set dtype = 'time' if 'DateTime' in col.data_type | string 
                               else 'date' if 'Date' in col.data_type | string 
                               else 'number' if 'Number' in col.data_type | string 
                               else 'text' %}
                {% do columns.append({"name":col.name,
                                       "data_type": dtype}) %} 
            {% endfor %}
        {% else %}
            {% set type_10_column_exclusions = [] %}
            {% for col in type_10_columns %}
                {% do type_10_column_exclusions.append('all_' ~ col | lower ~'_values') %}
            {% endfor %}
            {% set existing_columns = adapter.get_columns_in_relation(existing_relation) %}
            {% for col in existing_columns %}
                {% if col.name |lower not in ['row_effective_at',
                                     'row_expired_at',
                                     'row_is_current',
                                      this.table | lower ~ '_key',
                                      this.table | lower ~ '_id',] + type_10_column_exclusions %}
                {% set dtype = 'time' if 'timestamp' in col.dtype | string  
                               else 'date' if 'date' in col.dtype | string 
                               else 'number' if 'integer' in col.dtype | string 
                               else 'number' if 'float' in col.dtype | string 
                               else 'number' if 'numeric' in col.dtype | string 
                               else 'text' %}
                    {% do columns.append({"name":col.name,
                                          "data_type":dtype}) %}
                {% endif %}
            {% endfor %}
        {% endif %}
        {{ return(columns) }}

{%- endmacro -%}


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
    {{ kimball._kimball_compiled_records_query(config_args,
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
    {% for col in config_args["target_columns"] -%}
        ,scd.{{ col["name"] }} AS {{ col["name"] }}
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


{%- macro _kimball_aggregates_query(config_args, duplicates_cte) -%}
    
    SELECT
    {{ config_args["DNI"] }}
    {% for col in config_args["type_10_columns"] -%}
        ,ARRAY_AGG(DISTINCT {{ col }}__item) AS all_{{ col }}_values
    {%- endfor %}
    FROM 
        {{ duplicates_cte }} 
    {% for col in config_args["type_10_columns"] -%}
    ,unnest( all_{{ col }}_values ) as {{ col }}__item
    {%- endfor %}
    GROUP BY 1

{%- endmacro -%}


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


{%- macro _kimball_scd_with_duplicates_query(config_args, source_cte) -%}
    /*{# The "main" query within the dimensional CTE stack. 
        Produces the shaped structure by scd type. Aggregates in this query are not distinct.
        ARGS:
            - config_args (dict) the configuration arguments created at materialization init.
            - source_cte (string) the cte representing all source records for this increment. 
        RETURNS: the cte of shaped records ready to be deduplicated.
    #}*/
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

    -- type 10
    {% for col in config_args["type_10_columns"] -%}
        ,array_agg( {{ col }} ) OVER (PARTITION BY {{ config_args["DNI"] }}) AS all_{{ col }}_values
    {%- endfor -%}
    
    {% for col in config_args["target_columns"] -%}
        {%- if col["name"] not in config_args["type_0_columns"] + config_args["type_1_columns"] -%}
        ,{{ col["name"] }} 
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


{%- macro _kimball_cdc_predicate_lookback_type_partial(config_args) -%} 
    /*{# Enables the lookback window to adjust for data types #}*/
    {%- if config_args['cdc_data_type'] in ('time','date',) -%}
    {{ xdb.dateadd('day',(config_args["lookback_window"] * -1) ,'(SELECT max_cdc FROM _target_max) ') }}
    {%- else -%}
    ( {{ config_args["lookback_window"] }} * -1) + (SELECT max_cdc FROM _target_max)
    {%- endif -%}
{%- endmacro -%}


{%- macro _kimball_cdc_predicate_calculation(config_args) -%}
    /*{# Determines the correct predicate for the source query.#}*/

    {%- if config_args["lookback_window"] | lower == "all" -%}
         {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
        NOT IN
        (SELECT     
            {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
        FROM 
            {{ config_args["existing_relation"] }})
    {%- elif config_args["lookback_window"] > 0 -%}
        {{ config_args["CDC"] }} > 
        {{ kimball._kimball_cdc_predicate_lookback_type_partial(config_args) }}
    AND 
     {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
    NOT IN
    (SELECT     
        {{ xdb.hash([ config_args["CDC"],config_args["DNI"] ]) }} 
    FROM 
        {{ config_args["existing_relation"] }}
    WHERE 
        {{ config_args["CDC"] }} > 
        {{ kimball._kimball_cdc_predicate_lookback_type_partial(config_args) }}
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
            {{ kimball._kimball_cdc_predicate_calculation(config_args) }}
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
        {% for col in config_args["target_columns"] %}
            ,{{ col["name"] }}
        {% endfor %}
        FROM
           _from_source
                    UNION
            SELECT 
            {{ config_args["dim_key"] }}
            ,{{ config_args["dim_id"] }}
        {% for col in config_args["target_columns"] -%}
            ,{{ col["name"] }}
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

