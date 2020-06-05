{% macro _build_kimball_fact(sql, required_dims ) %}
    /*{# The workhorse query builder for fact tables.
        Appends and injects each required dim key and CTE. 
        
        Args: 
            sql (sql) : the model sql statement.
            required_dims (list) a collection of dimensional tuples to be converted to CTEs.
        Returns:
            sql : a valid statement for CTAS.
    #}*/
        {{ sql }}
        {% for dim in required_dims %}
        {% set dimension, instance_at, dni = dim %}
        JOIN
            (SELECT 
                {{ xdb.fold(dimension) }}_key
                ,{{ dni }}
                ,row_effective_at
                ,row_expired_at
             FROM
                {{ ref(dimension) }} ) AS {{ kimball._dbt_kimball_dim_key_helper( dimension ) }}
        USING ( {{ dni }} )
        WHERE 
            {{ instance_at }} 
        BETWEEN         
            {{ kimball._dbt_kimball_dim_key_helper( dimension ) }}.row_effective_at
        AND
            {{ kimball._dbt_kimball_dim_key_helper( dimension ) }}.row_expired_at
        {%  endfor %}
{% endmacro %}