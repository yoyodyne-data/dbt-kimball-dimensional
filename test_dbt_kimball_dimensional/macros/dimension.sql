{% materialization dimension, default %}
    /*{# builds a Kimball incremental dimension with type 0,1,2 and 10 dims.
    #}*/
    
    {%- set DNI = config.require('durable_natural_id') -%}
    {%- set CDC = config.require('change_data_capture') -%}
    {% set full_refresh = flags.FULL_REFRESH %}
    {%- set type_0_columns = config.get('type_0',default=[]) -%}
    {%- set type_1_columns = config.get('type_1',default=[]) -%}
    {%- set type_4_columns = config.get('type_4',default=[]) -%}
    {%- set type_10_columns = config.get('type_10',default=[]) -%}
    {%- set beginning_of_time = config.get('beginning_of_time',default='1970-01-01') -%}

    {%- set has_aggregates = type_4_columns + type_10_columns -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
    {%- set target_relation = this -%}
    {%- set existing_relation = adapter.get_relation(this.database, this.schema, this.name) -%}
    -- BEGIN 
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    
    -- stub table to get us the column names from the CTE
    {% set stub_sql %}
	WITH __dbt_kimball_dimensional_stub AS (
	 {{ sql }} 
	)
	SELECT * FROM __dbt_kimball_dimensional_stub LIMIT 0
    {% endset %}
    {% set structure = run_query(stub_sql) %}
    {% set target_columns = structure.column_names %}
    --fresh build

    {%- if existing_relation is none or full_refresh -%}
      {% set source_cte %} 
          WITH 
	  __dbt_kimball_dimensional_source AS (
		{{sql}}
	  )
      {% endset %}
    {%- endif -%} 

    {% set slowly_changing_dimension_body %}
	__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates AS (
   	SELECT 
	   NULL AS {{this.table}}_key
	   ,NULL AS {{this.table}}_id
	-- type 0 
	{{ log(type_0_columns, info=True) }}
        {% for col in type_0_columns %}
	   ,LAST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
	{% endfor %}

	-- type 1
        {% for col in type_1_columns %}
	   ,FIRST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
	{% endfor %}

	-- type 4 + 10
        {% for col in type_4_columns + type_10_columns %}
	   ,array_agg( {{col}} ) OVER (PARTITION BY {{ DNI }}) AS all_{{col}}_values
	{% endfor %}
	
	{% for col in target_columns %}
	   {% if col not in type_0_columns + type_1_columns + type_4_columns %}
		, {{ col }} 
	   {% endif %}
	{% endfor %}

	,CASE WHEN LAST_VALUE( {{ CDC }} ) over natural_key_window = {{ CDC }}
	THEN '1970-01-01'
	ELSE {{ CDC }}
	END AS row_effective_at
	,( LAG( {{ CDC }}, 1, '9999-12-31') over natural_key_window ) + interval '-1 second' AS row_expired_at

	,CASE WHEN FIRST_VALUE( {{ CDC }} ) over natural_key_window  = {{ CDC }}
	THEN TRUE
	ELSE FALSE
	END AS row_is_current
	FROM 
	   __dbt_kimball_dimensional_source
	WINDOW natural_key_window AS (PARTITION BY {{ DNI }}  ORDER BY {{ CDC }} DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
	order by {{ DNI }}, {{ CDC }} 
    )
    {% if has_aggregates %}
    ,__dbt_kimball_dimensional_deduplicated_aggregates AS (
       SELECT
	{{ DNI }}
	{% for col in type_4_columns + type_10_columns %}
	 ,ARRAY_AGG(DISTINCT {{ col }}_item) AS {{ col }} 
	{% endfor %}
       FROM __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates 
       {% for col in type_4_columns + type_10_columns %}
	 ,unnest( {{ col }} ) as {{ col }}_item
       {% endfor %}
       GROUP BY 1
    )
    {% endif %}

    ,__dbt_kimball_dimensional_durable_ids AS (
       SELECT
       {{ DNI }}
       ,ROW_NUMBER() OVER() AS {{this.table}}_id
       FROM __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates
       GROUP BY 1
    )
    ,__dbt_kimball_dimensional_slowly_changing_dimensions AS (
    SELECT 
       ROW_NUMBER() OVER() AS {{this.table}}_key
       ,durable_ids.{{this.table}}_id
       {% for col in type_4_columns + type_10_columns %}
	, deduped.all_{{ col }}_values AS all_{{ col }}_values
       {% endfor %}
       {% for col in target_columns %}
	  {% if col not in type_4_columns %}
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
    USING ( {{ DNI }} )
    {% endif %}
    JOIN
        __dbt_kimball_dimensional_durable_ids durable_ids
    USING ( {{ DNI }} )
    )
    SELECT 
	* 
    FROM 
       __dbt_kimball_dimensional_slowly_changing_dimensions 
  {% endset %}

  {% call statement('main') %}
	{{ create_table_as(False,
			   target_relation,
			   source_cte ~ " , " ~ slowly_changing_dimension_body) }}
  {% endcall %}
 
  {% do adapter.commit() %}


  {{ run_hooks(post_hooks) }}
  
  {{ return({'relations': [target_relation]}) }}


{% endmaterialization  %}
