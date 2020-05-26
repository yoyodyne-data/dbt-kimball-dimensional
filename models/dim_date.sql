WITH
    date_range AS (
        {{ dbt_utils.date_spine("day",
                                "'1900-01-01'::DATE",
                                "'2100-12-31'::DATE") }} 
    )
SELECT 
    TO_CHAR(date_day, 'YYYYMMDD') AS dim_date_key
    ,date_day::DATE AS date
    
FROM 
    date_range
