{%- macro _kimball_source_query(config_args) -%}
   /*{# The query partial that defines our "source", ie records to be operated on.
        ARGS:
          - config_args (dict) an object containing the full materialization args set.
        RETURNS: the CTE partial `__dbt_kimball_dimensional_source`.
   #}*/

    {%- set dim_id = this.table ~ '_id' -%}
    {%- set dim_key = this.table ~ '_key' -%}
   WITH 
   _base_source AS (	
     {{ config_args["sql"] }}
   )	
      {%- if config_args["target_exists"] and not config_args["full_refresh"] -%}
        ,_target_max AS (
	   SELECT 
	     MAX( {{ config_args['CDC'] }} ) as max_cdc	
	   FROM
	      {{ config_args["backup_relation"] }}
	)
	,_from_source AS (
	   SELECT
	     NULL::numeric AS {{ dim_key }}
	     ,NULL::numeric AS {{ dim_id }}
	      *
	   FROM 
	     _base_source
	   WHERE
	    {{ config_args["CDC"] }} > 
        
	
	{%- if lookback_window -%}
	   {{ xdb.dateadd('day',(config_args["lookback_window"] * -1) ,'(SELECT max_cdc FROM target_max)') }}
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
           (SELECT max_cdc FROM target_max)
	{%- endif -%}
	)
	,_from_target AS (
	   SELECT
	      *
	   FROM
	      {{ config_args["backup_relation"] }}
	   WHERE {{ config_args["DNI"] }} IN (SELECT {{ config_args["DNI"] }} FROM _final_source )
	)
	,_final_source AS (
	    SELECT * FROM 
	    (SELECT 
		*
	    FROM _from_source
	    UNION ALL
	    SELECT
		*
	    FROM _from_target)
	)
      {%- else -%}
        ,_final_source AS (
           SELECT 
	     NULL::numeric AS {{ dim_key }}
	     ,NULL::numeric AS {{ dim_id }}
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
