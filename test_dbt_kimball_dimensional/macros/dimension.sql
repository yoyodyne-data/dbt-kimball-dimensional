{% materialization dimension, default %}
    /*{# builds a Kimball incremental dimension with type 0,1,2,4 and 10 dims.
    ARGS:
      - indexes (list) The list of indexes (materialized as partitions, indexes or clustering keys based on implementation) to apply, in order. Default is 1. CDC and 2. dimensional id
      - change_data_capture_input_type (string) the data type of the CDC column. Default is timestamp.
      - lookback_window (int,string) how far to look back for late-arriving data. For datetime CDC it is expressed in days. For integer CDC it is expressed in integers. a value of 'all' will look back across the whole table (note: this is dangerously non-performant!). A lookback window of none will not look back. Default is none. Example: if records take no more than 3 days to arrive, a lookback window of 4 would be safe and still performant. If records are never more than a few hours late a lookback of 1 is safe. If records arrive years later, a lookback of 'all' is likely needed - though a full refresh may actually be cheaper. If records always arrive in chronological order a lookback of none is the least expensive. 
        #}*/
    

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
    {%- set target_relation = this -%}
    {%- set existing_relation = load_relation(this) -%}
    {%- set target_exists = existing_relation is not none -%}

    {%- set dim_key = this.table ~ '_key' -%}
    {%- set dim_id =  this.table ~ '_id' -%}

    {%- set DNI = config.require('durable_natural_id') -%}
    {%- set CDC = config.require('change_data_capture') -%}
    {% set full_refresh = flags.FULL_REFRESH %}
    {%- set type_0_columns = config.get('type_0',default=[]) -%}
    {%- set type_1_columns = config.get('type_1',default=[]) -%}
    {%- set type_4_columns = config.get('type_4',default=[]) -%}
    {%- set type_10_columns = config.get('type_10',default=[]) -%}
    {%- set CDC_data_type = config.get('change_data_capture_column_type',default='timestamp') -%}
    {%- set indexes = config.get('indexes',default=[dim_key,dim_id]) -%}
    {%- set beginning_of_time = config.get('beginning_of_time',default='1970-01-01') -%}
    {%- set lookback_window = config.get('lookback_window',default=none) -%}

    {% set model_query_columns = _get_columns_from_query(sql)  %}
    -- TODO: rename target_columns key to model_query_columns
    {%- set config_args= {"sql":sql,
              "DNI":DNI,
              "CDC":CDC,
              "full_refresh":full_refresh,
              "type_0_columns":type_0_columns,
              "type_1_columns":type_1_columns,
              "type_4_columns":type_4_columns,
              "type_10_columns":type_10_columns,
              "beginning_of_time":beginning_of_time,
              "lookback_window":lookback_window,
              "target_columns":model_query_columns, 
              "backup_relation":backup_relation,
              "target_exists": target_exists} -%}


    -- BEGIN 
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set slowly_changing_dimension_body = _kimball_scd_body(config_args, 
                                                              _kimball_source_query(config_args) )  %}
    
    {%- if not config_args["target_exists"] -%}

        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               slowly_changing_dimension_body) }}
        {% endcall %}

    {%- elif config_args["full_refresh"] -%}

        {%- set backup_relation = this.incorporate(
                path={"identifier": target_relation.identifier ~ "__dbt_kimball_backup"} ) -%}
        {%- set backup_relation = load_relation(backup_relation) -%}
        {%- if backup_relation is not none -%}        
            {% do adapter.drop_relation(backup_relation) %}
            {% do adapter.rename_relation(target_relation, backup_relation) %}
        {%- endif -%}
       
        {% call statement('main') %}
            {{ create_table_as(False,
                               target_relation,
                               slowly_changing_dimension_body) }}
        {% endcall %}

    {%- else -%}
        --thing
    {%- endif -%}

  {% do adapter.commit() %}
  {{ run_hooks(post_hooks) }}
  
  {{ return({'relations': [target_relation]}) }}


{% endmaterialization  %}
