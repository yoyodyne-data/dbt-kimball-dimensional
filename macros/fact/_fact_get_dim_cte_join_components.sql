{% macro _fact_get_dim_cte_join_components() %}
    /*{# Extracts and compiles the relationships between each fact and 
         required dim key from the temporary staging tables.

        Returns:
            list : a list of row tuples for the required dims. 
    #}*/
    {% set model_dni = config.get('durable_natural_id_column', default='NULL') %}
    {% set model_instance_at = config.get('instance_at_column', default='NULL') %}

    {% call statement('dim_cte_meta', fetch_result=True, auto_begin=False) %}
        SELECT 
            fact_meta.dimension AS dimension
            ,COALESCE(fact_meta.instance_at_column, {{ model_instance_at }}, dimension_meta.cdc_column) AS instance_at_column
            ,COALESCE(fact_meta.dni_column, {{ model_dni }}, dimension_meta.dni_column) AS dni_column
        FROM
            {{ xdb.fold('dbt_kimball_staging.fact_meta') }} fact_meta
        LEFT JOIN
            {{ xdb.fold('dbt_kimball_staging.dimension_meta') }} dimension_meta
        USING (dimension)
        WHERE
            fact_meta.fact = '{{ this.table }}' 
    {% endcall %}
    {{ return(load_result('dim_cte_meta')['data']) }}
{% endmacro %}