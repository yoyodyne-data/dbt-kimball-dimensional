{{ config(materialized="table")}}
WITH 
source_data AS (
	SELECT *
	FROM prod_source.user_total_replay
)
,type_2 AS (
   SELECT 
 	user_id AS natural_key
	,first_name
	,last_name
	,email
	,phone_number
	,preferred_status AS is_preferred_user
	,birthdate
	,account_created_at
	
	,case when LAST_VALUE(batched_at) over w = batched_at
	THEN '1970-01-01'
	ELSE batched_at
	END as row_effective_at
	,( LAG(batched_at, 1, '9999-12-31') over w ) + interval '-1 second' as row_expired_at

	,case when FIRST_VALUE(batched_at) over w  = batched_at
	then true
	else false
	end as row_is_current
	from source_data
	window w as (partition by user_id order by batched_at desc range between unbounded preceding and unbounded following )
	order by user_id, batched_at
)
SELECT * FROM type_2
