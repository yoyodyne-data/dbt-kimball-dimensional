{%- macro _get_columns_from_query(sql) -%}
   /*{# Returns the column list from a given sql query
	ARGS:
  	  - sql (string) the sql to query.
        RETURNS: list of column names
   #}*/
    {% set stub_sql %}
	WITH __dbt_kimball_dimensional_stub AS (
	 {{ sql }} 
	)
	SELECT * FROM __dbt_kimball_dimensional_stub LIMIT 0
    {% endset %}
    {% set structure = run_query(stub_sql) %}
    {{ return((structure.column_names,structure.column_types,)) }}
{%- endmacro -%}
