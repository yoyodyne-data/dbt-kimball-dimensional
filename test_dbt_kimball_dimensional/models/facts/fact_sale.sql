{{ config(materialized='fact') }}


SELECT
    {{ kimball.dim_date_key('placed_at') }} AS placed_at_key
    ,placed_at
    ,sale_price
    ,tax_price
    ,dim_salesperson_key
    ,{{ kimball.dim_key('dim_user', instance_at_column='placed_at') }}
FROM
    prod_source.order_day_1
JOIN
    {{ ref('dim_salesperson') }} dim_salesperson
ON 
    dim_salesperson.name = salesperson

