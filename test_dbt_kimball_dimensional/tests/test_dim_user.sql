WITH
actual AS ( 
SELECT
    *
FROM
    {{ ref('DIM_USER') }}
)
,expected AS (
SELECT
    *
FROM
    {{ ref('DIM_USER_DAY_1') }}
)
,actual AS (
    SELECT
        *
    FROM
        actual
    MINUS
    SELECT
        *
    FROM
        expected
)
,expected AS (
    SELECT
        *
    FROM
        expected
    MINUS
    SELECT
        *
    FROM
        actual
)
SELECT

    SELECT
        *
    FROM expected
UNION ALL
    SELECT
        *
    FROM actual

