{{ config(materialized='dimension',
	  durable_natural_id="event_id",
	  beginning_of_time="2007-01-01",
	  lookback_window='all',
	  change_data_capture="collector_date") }}

SELECT
   *
FROM
   prod_source.web_event_day_{{ var('day') }} 
