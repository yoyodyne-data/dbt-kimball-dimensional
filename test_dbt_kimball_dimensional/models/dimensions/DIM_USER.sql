{{ config(materialized="dimension", 
	  durable_natural_id="user_id",
	  change_data_capture="batched_at") }}

SELECT
   *
FROM 
  prod_source.user_day_{{ var('day') }}

