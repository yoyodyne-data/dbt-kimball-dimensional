{{ config(materialized="dimension", 
	  durable_natural_id="user_id",
	  beginning_of_time="2007-01-01",
	  type_10=['email','phone_number'],
	  type_1=['account_created_at'],
	  lookback_window='all',
	  change_data_capture="batched_at") }}

SELECT
   *
FROM 
  prod_source.user_day_{{ var('day') }}

