{{ config(materialized="table")}}
WITH 
source_data AS (
	SELECT *
	FROM prod_source.user_total_replay
)
,slowly_changing_dimensions_with_duplicates AS (
   SELECT 

	-- type 0
	LAST_VALUE(account_created_at) OVER w AS account_created_at
	
	-- type 1
	,FIRST_VALUE(email) OVER w AS current_email_address

	-- type 4
	, array_agg(email) OVER (partition by user_id ORDER BY batched_at) AS all_email_addresses
	
	-- type 10 is just type 4 + type 2

	-- type 2
 	,user_id AS natural_key
	,first_name
	,last_name
	,email
	,phone_number
	,preferred_status AS is_preferred_user
	,birthdate
	

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
),
deduplicated_aggregates AS (
    SELECT
	natural_key
	,array_agg(DISTINCT item) AS all_email_addresses
   FROM slowly_changing_dimensions_with_duplicates
	,unnest(all_email_addresses) as item
   GROUP BY 1
)
,slowly_changing_dimensions AS (
SELECT 
   scd.natural_key
   ,scd.first_name
   ,scd.last_name
   ,email
   ,phone_number
   ,is_preferred_user
   ,birthdate
   ,row_effective_at
   ,row_expired_at
   ,row_is_current
   ,dedupe.all_email_addresses AS all_email_addresses
   ,scd.account_created_at
   ,scd.current_email_address
FROM 
slowly_changing_dimensions_with_duplicates scd
LEFT JOIN
deduplicated_aggregates dedupe
USING (natural_key)
)

SELECT * FROM slowly_changing_dimensions
