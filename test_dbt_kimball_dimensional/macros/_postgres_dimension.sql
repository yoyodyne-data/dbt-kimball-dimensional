{%- macro _postgres_dimension_total_replay_body(DNI,
						CDC,
						type_0_columns,
						type_1_columns,
						type_4_columns,
						type_10_columns) -%}

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
	THEN '{{ beginning_of_time }}'
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
{%- endmacro -%}
