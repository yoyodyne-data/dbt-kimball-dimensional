    {{ config(materialized='table') }}

/*  Not all dims need slowly changing dimensions. 
    In this case it makes more sense to materialize ``dim_salesperson`` 
    as a flat table. */ 
WITH 
distinct_salespeople AS (
SELECT
    salesperson AS name
    ,MIN(placed_at) AS first_sale_at 
FROM 
  prod_source.order_day_{{ var('day') }}
GROUP BY 1
)
SELECT 
    ROW_NUMBER() OVER() AS dim_salesperson_key
    ,distinct_salespeople.*
FROM
    distinct_salespeople
ORDER BY first_sale_at, name
