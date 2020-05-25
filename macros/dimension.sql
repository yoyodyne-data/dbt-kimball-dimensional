{% materialization dimension, default %}
    /*{# builds a Kimball incremental dimension with type 0,1,2,4 and 10 dims.
    ARGS:
      - indexes (list) The list of indexes (materialized as partitions, indexes or clustering keys based on implementation) to apply, in order. Default is 1. CDC and 2. dimensional id
      - change_data_capture_input_type (string) the data type of the CDC column. Default is timestamp.
      - lookback_window (int,string) how far to look back for late-arriving data. For datetime CDC it is expressed in days. For integer CDC it is expressed in integers. a value of 'all' will look back across the whole table (note: this is dangerously non-performant!). A lookback window of none will not look back. Default is none. Example: if records take no more than 3 days to arrive, a lookback window of 4 would be safe and still performant. If records are never more than a few hours late a lookback of 1 is safe. If records arrive years later, a lookback of 'all' is likely needed - though a full refresh may actually be cheaper. If records always arrive in chronological order a lookback of none is the least expensive. 
        #}*/
    

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
    {% set target_relation = this %}
    {% set existing_relation = load_relation(this) %}

    {% set CDC = config.require('change_data_capture') %}

    {% set model_query_columns, model_query_column_types  = kimball._get_columns_from_query(sql)  %}
    
    {%- set target_columns = kimball._kimball_get_columns(existing_relation,sql,config.get('type_10',default=[])) -%}
    {% for col in target_columns %}
        {% if col['name'] == CDC %}
            {% set cdc_data_type = col['data_type'] %}
        {% endif %}
    {% endfor %}
    
    
    {% set config_args= {
              "sql" : sql,
              "dim_key" : this.table ~ '_key',
              "dim_id" : this.table ~ '_id',
              "DNI" : config.require('durable_natural_id'),
              "CDC" : CDC,
              "cdc_data_type" : cdc_data_type,
              "full_refresh" : flags.FULL_REFRESH,
              "type_0_columns" : config.get('type_0',default=[]),
              "type_1_columns" : config.get('type_1',default=[]),
              "type_10_columns" : config.get('type_10',default=[]),
              "indexes" : config.get('indexes',default=[dim_key,dim_id]), 
              "beginning_of_time" : config.get('beginning_of_time',default='1970-01-01'),
              "lookback_window" : config.get('lookback_window',default=0),
              "target_columns" : target_columns,
              "model_query_columns" : model_query_columns, 
              "existing_relation" : existing_relation} %}

    -- BEGIN 
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set relations_to_drop = [] %}

    {% if existing_relation is none %}
        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               kimball._build_kimball_dimension(config_args)) }}
        {% endcall %}

    {% elif config_args["full_refresh"] %}

        {% set backup_relation = existing_relation.incorporate(
                path={"identifier": target_relation.identifier ~ "__dbt_kimball_backup"} ) %}
    {% if load_relation(backup_relation) is not none %}
        {% do adapter.drop_relation(backup_relation) %}
    {% endif %}
        {% do adapter.rename_relation(target_relation, backup_relation) %}
    {% do relations_to_drop.append(backup_relation) %}
       
        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               kimball._build_kimball_dimension(config_args)) }}
        {% endcall %}

    {% else %}
        {% set temp_relation = make_temp_relation(this) %}
        {% call statement('temp') %}
            {{ create_table_as(True,
                               temp_relation,
                               kimball._build_kimball_dimension(config_args)) }}
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

  {{ return({'relations': [target_relation]}) }}


{% endmaterialization  %}
