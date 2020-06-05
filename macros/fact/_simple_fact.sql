{% macro _simple_fact() %}

    {% set required_dims = kimball._fact_get_dim_cte_join_components() %}

    {% set sql_with_dim_joins %}
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
    {% endset %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% call statement('main') %}
        {{ create_table_as(False,
                           this,
                           sql_with_dim_joins) }}
    {% endcall %}

    {% do adapter.commit() %}
    {{ run_hooks(post_hooks) }}

{% endmacro %}

{%- macro _dbt_kimball_dim_key_helper(dim)  -%}
    _dbt_kimball_{{ xdb.fold(dim) | trim }}_key_lookup
{%- endmacro -%}