{%- macro _kimball_scd_body_cte(config_args, source_cte) -%}
  /*{# Partial chain of CTEs that produces the final slowly changing dimensions dataset.
       ARGS:
	- config_args (dict) the full set of materialization config arguments.
	- source_cte (string) the combiled source CTE named __dbt_kimball_dimensional_source
       RETURNS: the combined SQL cte stack to generate the executable sql.
    #}*/
    {%- set has_aggregates = config_args['type_4_columns'] + config_args['type_10_columns'] -%}
	
	{{ source_cte }}
	
	,__dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates AS (
   	SELECT 
	   {{ this.table }}_key
	   ,{{ this.table }}_id
	-- type 0 
        {% for col in config_args["type_0_columns"] %}
	   ,LAST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
	{% endfor %}

	-- type 1
        {% for col in config_args["type_1_columns"] %}
	   ,FIRST_VALUE( {{col}} ) OVER natural_key_window AS {{col}}
	{% endfor %}

	-- type 4 + 10
        {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] %}
	   ,array_agg( {{col}} ) OVER (PARTITION BY {{ config_args["DNI"] }}) AS all_{{col}}_values
	{% endfor %}
	
	{% for col in config_args["target_columns"] %}
	   {% if col not in config_args["type_0_columns"] + config_args["type_1_columns"] + config_args["type_4_columns"] %}
		, {{ col }} 
	   {% endif %}
	{% endfor %}

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
	   __dbt_kimball_dimensional_source
	WINDOW natural_key_window AS (PARTITION BY {{ config_args["DNI"] }}  ORDER BY {{ config_args["CDC"] }} DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )
	order by {{ config_args["DNI"] }}, {{ config_args["CDC"] }} 
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
       SELECT
       {{ config_args["DNI"] }}
       ,ROW_NUMBER() OVER() AS {{this.table}}_id
       FROM __dbt_kimball_dimensional_slowly_changing_dimensions_with_duplicates
       GROUP BY 1
    )
    ,__dbt_kimball_dimensional_slowly_changing_dimensions AS (
    SELECT 
       ROW_NUMBER() OVER() AS {{this.table}}_key
       ,durable_ids.{{this.table}}_id
       {% for col in config_args["type_4_columns"] + config_args["type_10_columns"] %}
	, deduped.all_{{ col }}_values AS all_{{ col }}_values
       {% endfor %}
       {% for col in target_columns %}
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
