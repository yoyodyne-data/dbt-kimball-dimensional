{% materialization dimension, default %}
    /*{# builds a Kimball incremental dimension with type 0,1,2 and 10 dims.
    #}*/
    
    {%- set DNK = config.require('durable_natural_key') -%}
    {%- set CDC = config.require('change_data_capture') -%}
    {% set full_refresh = flags.FULL_REFRESH %}
    {%- set type_0_columns = config.get('type_0',[]) -%}
    {%- set type_1_columns = config.get('type_1',[]) -%}
    {%- set type_4_columns = config.get('type_4',[]) -%}
    {%- set type_10_columns = config.get('type_10',[]) -%}
    {%- set beginning_of_time = config.get('beginning_of_time','1970-01-01') -%}


    {{ run_hooks(pre_hooks, inside_transaction=False) }}
 
    {%- set target = adapter.get_relation(this) -%}
    -- BEGIN 
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
    
    --fresh build
    
    {%- if target is none or full_refresh -%}
	{%- set :wq










    WITH 
    new_records AS (
    {%- if target is none -%}
    -- fresh build, get all the records
            
    {%- else -%}
	
    -- get CDC records 

    {%- endif -%}
    )
    {%- if type_0_columns -%}
        ,type_zero_cols AS (
        
        )
    {%- endif -%}
    {%- if type_1_columns -%}
        ,type_one_cols AS (
        
        )
    {%- endif -%}
    {%- if type_4_columns -%}
        ,type_four_cols AS (

        )
    {%- endif -%}
    {%- if type_10_columns -%}
        ,type_ten_cols AS (

        )
    {%- endif -%}
    
    ,replay AS (
        
    )
    
    
    
    
    {{ run_hooks(post_hooks) }}

    {{ drop_relation_if_exists(shit_temp_table) }}



{% endmaterialization %}
