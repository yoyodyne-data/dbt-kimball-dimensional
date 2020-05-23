{%- macro _kimball_source_query(config_args) -%}
   /*{# The query partial that defines our "source", ie records to be operated on.
        ARGS:
          - config_args (dict) an object containing the full materialization args set.
        RETURNS: the CTE partial `__dbt_kimball_dimensional_source`.
   #}*/
   
    {%- set incremental = ( not config_args['full_refresh'] and config_args['existing_relation'] is not none ) -%}
    WITH 
        _base_source AS (	
            {{ config_args["sql"] }}
        )	
    {%- if incremental -%}
        ,_target_max AS (
            SELECT 
                MAX( {{ config_args['CDC'] }} ) as max_cdc	
            FROM
                {{ config_args["existing_relation"] }}
        )
        ,_from_source AS (
            SELECT
                NULL::numeric AS {{ config_args["dim_key"] }}
                ,NULL::numeric AS {{ config_args["dim_id"] }}
                ,*
            FROM 
                _base_source
            WHERE
                {{ config_args["CDC"] }} > 
            {%- if lookback_window -%}
                {{ xdb.dateadd('day',(config_args["lookback_window"] * -1) ,'(SELECT max_cdc FROM target_max) ') }}
            AND 
                 {{ xdb.hash([ '_base_source.' ~ CDC, '_base_source.' ~ DNI ]) }} 
            NOT IN
                (SELECT 	
                    {{ xdb.hash([ CDC,DNI ]) }} 
                FROM 
                    {{ config_args["backup_relation"] }}
                WHERE 
                    {{ config_args["CDC"] }} > 
                    {{ xdb.dateadd('day',(config_args["lookback_window"] * -1) ,'(SELECT max_cdc FROM target_max)') }}
                ) 
            {%- else -%}
                (SELECT max_cdc FROM _target_max) 
            {%- endif -%}
        )
        ,_from_target AS (
            SELECT
                *
            FROM
                {{ config_args["existing_relation"] }}
            WHERE 
                {{ config_args["DNI"] }} IN (SELECT 
                                                {{ config_args["DNI"] }} 
                                            FROM 
                                                _from_source )
        )
        ,_final_source AS (
            SELECT 
                *
            FROM 
		(SELECT 
		    {{ config_args["dim_key"] }}
		    ,{{ config_args["dim_id"] }}
		{% for col in config_args['model_query_columns'] %}
		    ,{{ col }}
		{% endfor %}
		FROM
		   _from_source
                    UNION ALL
	        SELECT 
		    {{ config_args["dim_key"] }}
		    ,{{ config_args["dim_id"] }}
		{% for col in config_args['model_query_columns'] -%}
		    ,{{ col }}
		{% endfor %}
		FROM
		   _from_target) unioned
        )
    {%- else -%}
        ,_final_source AS (
            SELECT 
                NULL::numeric AS {{ config_args["dim_key"] }}
                ,NULL::numeric AS {{ config_args["dim_id"] }}
                ,*
            FROM
                _base_source
        )
    {%- endif -%} 
          
        SELECT 
            *
        FROM 
            _final_source
{%- endmacro -%}
