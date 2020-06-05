{% macro _simple_fact() %}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
    {% set required_dims = kimball._fact_get_dim_cte_join_components() %}
    {% set target_relation = this %}
    {% set existing_relation = load_relation(this) %}
    {% set full_refresh = flags.FULL_REFRESH %}
    {% set relations_to_drop = [] %}

    -- BEGIN
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    {% if existing_relation is none %}
        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               kimball._build_kimball_fact(sql, required_dims)) }}
        {% endcall %}

    {% elif config_args["full_refresh"] %}
        {% do relations_to_drop.append( kimball._clean_backup_relation(target_relation,
                                                                       backup_relation) ) %}
        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               kimball._build_kimball_fact(sql, required_dims)) }}
        {% endcall %}

    {% else %}
        {% set temp_relation = make_temp_relation(this) %}
        {% call statement('temp') %}
            {{ create_table_as(True,
                               temp_relation,
                               kimball._build_kimball_fact(sql, required_dims)) }}
        {% endcall %}
        {% call statement('main') %}
           {{ incremental_upsert(temp_relation,
                                 target_relation,
                                 config_args["dim_key"]) }}
        {% endcall %}

    {% endif %}

    {% do adapter.commit() %}
    {{ run_hooks(post_hooks) }}

{% endmacro %}

