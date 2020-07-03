{%- materialization fact, default -%}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    {% set config_args = {
              "sql" : sql,
              "target_relation" : this,
              "existing_relation" : load_relation(this),
              "required_dims" : kimball._fact_get_dim_cte_join_components(),
              "instance_at" : config.require("instance_at"),
              "DNI" : config.get("durable_natural_id"),
              "unique_expression" : config_get("unique_expression"),
              "lookback_window" : config.get("lookback_window"),
              "full refresh" : flags.FULL_REFRESH } %}

    {% if config_args["full_refresh"] or config_args["existing_relation"] is none %}
        {% set target_columns = kimball.get_columns_from_query(sql) %}
    {% else %}
        {% set target_columns = kimball.get_columns_from_existing(config_args["existing_relation"]) %}
    {% endif %}

    {% for col in target_columns %}
        {% if col['name'] == config_args["instance_at"] %}
            {% set _ = config_args.__setitem__("instance_at_data_type", col["data_type"]) %}
        {% endif %}
    {% endfor %}

    {% set relations_to_drop = [] %}

    {% if config_args["existing_relation"] is none %}
        {% call statement('main') %}
            {{ create_table_as(False,
                               config_args["target_relation"],
                               kimball._build_kimball_fact(config_args)) }}
        {% endcall %}

    {% elif config_args["full_refresh"] %}
        {% do relations_to_drop.append( kimball._clean_backup_relation(config_args["target_relation"],
                                                                       config_args["existing_relation"]) ) %}
        {% call statement('main') %}
            {{ create_table_as(False,
                               config_args["target_relation"],
                               kimball._build_kimball_fact(config_args)) }}
        {% endcall %}

    {% else %}
        {% set temp_relation = make_temp_relation(this) %}
        {% call statement('temp') %}
            -- needs work 
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

    {% for relation in relations_to_drop %}
        {% do drop_relation_if_exists(relation) %}
    {% endfor %}

    {{ return( {'relations': [ config_args["target_relation"] ]} ) }}

{% endmaterialization %}
