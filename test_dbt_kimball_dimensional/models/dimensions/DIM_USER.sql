{{ config(materialized="dimension", 
	  durable_natural_id="user_id",
	  beginning_of_time="2007-01-01",
	  type_10=['email_address','phone_number'],
	  type_1=['created_date_key'],
	  lookback_window='all',
	  change_data_capture="updated_at") }}

SELECT
    updated_at
    ,first_name
    ,last_name
    ,email AS email_address
    ,phone AS phone_number
    ,user_id AS user_id
    ,{{ kimball.dim_date_key('birthday') }} AS birthday_date_key
    ,CASE 
        WHEN preferred_customer THEN 'Is Preferred Customer'
        WHEN preferred_customer IS FALSE THEN 'Is Not Preferred Customer'
    END AS is_preferred_customer
    ,first_logged_in_at
    ,{{ kimball.dim_date_key('created_at') }} AS created_date_key
    ,CASE 
        WHEN current_email_status = 'SUBSCRIBED' THEN 'Subscribed'
        WHEN current_email_status = 'UNSUBSCRIBED' THEN 'Not Subscribed'
     END AS is_subscribed_to_email
FROM 
  prod_source.user_day_{{ var('day') }}

