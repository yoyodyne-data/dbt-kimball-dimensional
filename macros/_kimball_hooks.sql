
{%- macro _kimball_create_meta_schema() -%}
    /*{# creates (if not exists) the dim meta table. #}*/
    CREATE SCHEMA IF NOT EXISTS {{ xdb.fold('dbt_kimball_staging') }} 
{%- endmacro -%}

{%- macro _kimball_create_meta_dimension_table() -%}
    /*{# creates (if not exists) the dim meta table. #}*/
    CREATE TABLE IF NOT EXISTS {{ xdb.fold('dbt_kimball_staging.dimension_meta') }} 
    (dimension VARCHAR, cdc_column VARCHAR, dni_column VARCHAR)
{%- endmacro -%}

{%- macro _kimball_create_meta_fact_table() -%}
    /*{# creates (if not exists) the fact meta table. #}*/
    CREATE TABLE IF NOT EXISTS {{ xdb.fold('dbt_kimball_staging.fact_meta') }} 
    (fact VARCHAR, dimension VARCHAR, instance_at TIMESTAMP, dni_column VARCHAR)
{%- endmacro -%}

{%- macro _kimball_drop_meta_schema() -%}
    /*{# drops (if exists) the meta schema. #}*/
    DROP SCHEMA IF EXISTS {{ xdb.fold('dbt_kimball_staging') }} CASCADE
{%- endmacro -%}

{%- macro _kimball_insert_dimension_meta(dim, cdc, dni) -%}
    /*{# adds meta for the given dimension to the staging table.
        ARGS:
            - dim (string) the name of the dimension
            - cdc (string) the name of the CDC column for this dimension
            - dni (string) the name of the DNI column for this dimension
    #}*/
        INSERT INTO {{ xdb.fold('dbt_kimball_staging.dimension_meta') }} (dimension, cdc_column, dni_column)
        VALUES ('{{ dim }}', '{{ cdc }}', '{{ dni }}')
{%- endmacro -%}



