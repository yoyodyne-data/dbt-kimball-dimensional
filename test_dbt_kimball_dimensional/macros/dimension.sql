{% materialization dimension, default %}
    /*{# builds a Kimball incremental dimension with type 0,1,2 and 10 dims.
    #}*/
    

    {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
    {%- set target_relation = this -%}
    {%- set existing_relation = adapter.get_relation(this.database, this.schema, this.name) -%}

    {%- set DNI = config.require('durable_natural_id') -%}
    {%- set CDC = config.require('change_data_capture') -%}
    {% set full_refresh = flags.FULL_REFRESH %}
    {%- set type_0_columns = config.get('type_0',default=[]) -%}
    {%- set type_1_columns = config.get('type_1',default=[]) -%}
    {%- set type_4_columns = config.get('type_4',default=[]) -%}
    {%- set type_10_columns = config.get('type_10',default=[]) -%}
    {%- set beginning_of_time = config.get('beginning_of_time',default='1970-01-01') -%}
    {%- set lookback_window = config.get('lookback_window',default=none) -%}
    {% set backup_identifier = existing_relation.identifier ~ "__dbt_kimball_backup" %}
    {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}   

    {% set target_columns = _get_columns_from_query(sql)  %}

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
			  "target_columns":target_columns,
			  "backup_relation":backup_relation,
			  "target_exists": existing_relation is not none} -%}


    -- BEGIN 
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- Drop existing backup and move existing target to backup
    {% do adapter.drop_relation(backup_relation) %}
    {% do adapter.rename_relation(target_relation, backup_relation) %}

    {% set target_columns = _get_columns_from_query(sql)  %}

    {% set slowly_changing_dimension_body = _kimball_scd_body(config_args, 
							          _kimball_source_query(config_args) )  %}

{% call statement('main') %}
	{{ create_table_as(False,
			   target_relation,
			   slowly_changing_dimension_body) }}
  {% endcall %}
 
  {% do adapter.commit() %}
  {% do adapter.drop_relation(backup_relation) %}
  {{ run_hooks(post_hooks) }}
  
  {{ return({'relations': [target_relation]}) }}


{% endmaterialization  %}
